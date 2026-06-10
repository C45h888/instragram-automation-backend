// postgres-telemetry-kernel/substrates/cognition-scanner/index.js
// Cognition Scanner Substrate: deterministic trigger substrate for the publishing FSM.
//
// Owns: Realtime subscription lifecycle, per-account FIFO pending queue,
//       dispatching PUBLISHING_DATA_AVAILABLE to governance.
//
// Does NOT own: evaluation, emission, publishing policy, buffer logic.
//
// Trigger protocol (no timers, no race conditions):
//   1. Realtime UPDATE arrives → push to per-account FIFO queue
//   2. If publishing-fsm is IDLE → flush queue immediately as PUBLISHING_DATA_AVAILABLE
//   3. If publishing-fsm is not IDLE → events queue until EMISSION_OBSERVATION fires
//   4. On EMISSION_OBSERVATION → flush any pending events for that account
//
// Workers:
//   subscription-worker  — owns per-account Realtime channel lifecycle
//   event-worker         — owns FIFO queue and FSM dispatch decisions

const subscriptionWorker = require('./workers/subscription-worker');
const eventWorker = require('./workers/event-worker');

// ── Module state ─────────────────────────────────────────────────────────────

let _governance = null;
let _publishingFsm = null;
let _started = false;

// ── Observability ───────────────────────────────────────────────────────────

function _emitTransition(previousState, nextState, extraRaw = {}) {
  try {
    const observability = require('../../control-plane/observability/emitters/transition-emitter');
    observability.transition({
      domain: 'cognition-scanner',
      entity: 'scanner',
      entityId: 'cognition-scanner',
      previousState,
      nextState,
      authority: 'cognition-scanner',
      raw: extraRaw,
    });
  } catch (_) {
    // Observability failures never disrupt the scanner
  }
}

// ── Event routing ───────────────────────────────────────────────────────────

function _onRealtimeEvent(accountId, table, record) {
  eventWorker.pushEvent(accountId, table, record, _publishingFsm, _governance);
}

function _onEmissionObservation(event) {
  eventWorker.onEmissionObservation(event, _governance);
}

// ── Public API ───────────────────────────────────────────────────────────────

/**
 * Start the cognition scanner. Opens Realtime channels for each account.
 * Registers an EMISSION_OBSERVATION subscriber to flush pending queues
 * after the publishing FSM completes each evaluation cycle.
 *
 * @param {object} governance — constitutional kernel
 * @param {Array<{id: string}>} accounts — active accounts
 * @param {object} publishingFsm — publishing FSM module (for getState())
 */
async function start(governance, accounts, publishingFsm) {
  if (_started) return;
  _governance = governance;
  _publishingFsm = publishingFsm;

  _governance.subscribeAction('EMISSION_OBSERVATION', _onEmissionObservation);

  for (const account of accounts || []) {
    subscriptionWorker.subscribe(account.id, _onRealtimeEvent);
  }

  _started = true;
  _emitTransition('STOPPED', 'ACTIVE', { accountCount: accounts ? accounts.length : 0 });
  console.log(`[cognition-scanner] Started — ${accounts ? accounts.length : 0} account(s)`);
}

/**
 * Diff current accounts against subscribed channels.
 * Adds channels for new accounts, removes for removed ones.
 *
 * @param {Array<{id: string}>} currentAccounts
 */
async function refresh(currentAccounts) {
  const currentIds = new Set(currentAccounts.map(a => a.id));
  const subscribedIds = subscriptionWorker.getSubscribedAccountIds();

  // Subscribe new accounts
  for (const id of currentIds) {
    if (!subscribedIds.has(id)) {
      subscriptionWorker.subscribe(id, _onRealtimeEvent);
    }
  }

  // Unsubscribe removed accounts
  for (const id of subscribedIds) {
    if (!currentIds.has(id)) {
      subscriptionWorker.unsubscribe(id);
      eventWorker.clearAccount(id);
    }
  }
}

/**
 * Stop all channels, clear pending queues, deregister subscriber.
 */
async function stop() {
  subscriptionWorker.unsubscribeAll();
  eventWorker.clearAll();
  _started = false;
  _governance = null;
  _publishingFsm = null;
  _emitTransition('ACTIVE', 'STOPPED');
  console.log('[cognition-scanner] Stopped');
}

/**
 * Returns live runtime state.
 */
function status() {
  return {
    state: _started ? 'active' : 'stopped',
    accountCount: subscriptionWorker.activeCount(),
    pendingCount: eventWorker.status().pendingCount,
  };
}

module.exports = {
  start,
  refresh,
  stop,
  status,
};