// substrates/db/cognition-scanner.js
// Cognition Scanner: deterministic trigger substrate for the publishing FSM.
//
// Owns: Realtime subscription on cognition tables (scheduled_posts UPDATE status=approved,
//        post_queue UPDATE status=pending), per-account FIFO pending queue,
//        dispatching COGNITION_COMPLETE events to governance.
//
// Does NOT own: evaluation, emission, publishing policy, buffer logic.
//
// Trigger protocol (no timers, no race conditions):
//   1. Realtime UPDATE arrives → push to per-account FIFO queue
//   2. If publishing-fsm is IDLE → flush queue immediately as PUBLISHING_DATA_AVAILABLE
//   3. If publishing-fsm is not IDLE → events queue until EMISSION_OBSERVATION fires
//   4. On EMISSION_OBSERVATION → flush any pending events for that account
//
// Architectural rules:
//   - Zero timers — every decision is driven by event dispatch order
//   - FIFO queue guarantees no event loss during evaluation
//   - EMISSION_OBSERVATION subscriber provides deterministic completion signal
//   - getState() check prevents dispatching into a busy FSM

const { getSupabaseAdmin } = require('../../config/supabase');

// ── Module state ─────────────────────────────────────────────────────────────

/** @type {Map<string, import('@supabase/supabase-js').RealtimeChannel>} */
const _channels = new Map();       // channel name → RealtimeChannel

/** @type {Map<string, Array<{table: string, record: object}>>} */
const _pending = new Map();        // accountId → [{ table, record }, ...]

let _governance = null;
let _publishingFsm = null;         // injected for getState() check
let _started = false;

// ── Pending queue management ────────────────────────────────────────────────

/**
 * Flush the pending queue for an account. Dispatches PUBLISHING_DATA_AVAILABLE.
 * Deterministic — no timers, synchronous.
 */
function _flushQueue(accountId) {
  if (!_pending.has(accountId) || _pending.get(accountId).length === 0) return false;

  const events = _pending.get(accountId);
  _pending.delete(accountId);

  _governance.dispatch({
    type: 'PUBLISHING_DATA_AVAILABLE',
    accountId,
  });
  return true;
}

/**
 * Handle a single Realtime event. Pushes to the account's FIFO queue.
 * Dispatches immediately only if the publishing FSM is IDLE.
 * Otherwise events accumulate and flush on EMISSION_OBSERVATION.
 */
function _onRealtimeEvent(accountId, table, record) {
  // Push to FIFO queue
  if (!_pending.has(accountId)) {
    _pending.set(accountId, []);
  }
  _pending.get(accountId).push({ table, record });

  // Only flush if FSM is IDLE — otherwise wait for EMISSION_OBSERVATION
  // getState() returns the current FSM local state synchronously
  if (_publishingFsm && _publishingFsm.getState() === 'IDLE') {
    _flushQueue(accountId);
  }
}

/**
 * Flush pending queue when the FSM completes its evaluation cycle.
 * Subscribed to EMISSION_OBSERVATION in start().
 * This is the deterministic completion signal — no timers.
 */
function _onEmissionObservation(event) {
  const accountId = event.accountId;
  if (accountId) {
    _flushQueue(accountId);
  }
}

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

// ── Channel management ─────────────────────────────────────────────────────

function _subscribeAccount(accountId) {
  const admin = getSupabaseAdmin();
  if (!admin) return null;

  const channelName = `cognition:${accountId}`;
  _unsubscribeAccount(accountId);

  const channel = admin.channel(channelName, {
    config: {
      broadcast: { self: false },
      postgres: { filter: `business_account_id=eq.${accountId}` },
    },
  });

  channel
    .on('postgres_changes', {
      event: 'UPDATE',
      schema: 'public',
      table: 'scheduled_posts',
      filter: `status=eq.approved`,
    }, (payload) => {
      _onRealtimeEvent(accountId, 'scheduled_posts', payload.new);
    })
    .on('postgres_changes', {
      event: 'UPDATE',
      schema: 'public',
      table: 'post_queue',
      filter: `status=eq.pending`,
    }, (payload) => {
      _onRealtimeEvent(accountId, 'post_queue', payload.new);
    })
    .subscribe((status) => {
      if (status === 'SUBSCRIBED') {
        console.log(`[cognition-scanner] Subscribed for account ${accountId}`);
      } else if (status === 'CHANNEL_ERROR') {
        console.error(`[cognition-scanner] Channel error for account ${accountId}`);
      } else if (status === 'TIMED_OUT') {
        console.warn(`[cognition-scanner] Subscription timed out for account ${accountId} — will retry`);
      }
    });

  _channels.set(channelName, channel);
  return channel;
}

function _unsubscribeAccount(accountId) {
  const admin = getSupabaseAdmin();
  if (!admin) return;

  const channelName = `cognition:${accountId}`;
  const existing = _channels.get(channelName);
  if (existing) {
    admin.removeChannel(existing);
    _channels.delete(channelName);
  }
}

// ── Public API ──────────────────────────────────────────────────────────────

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

  // Subscribe to emission completions — deterministic flush signal
  _governance.subscribeAction('EMISSION_OBSERVATION', _onEmissionObservation);

  for (const account of accounts || []) {
    _subscribeAccount(account.id);
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
  const subscribedIds = new Set(
    [..._channels.keys()].map(k => k.replace('cognition:', ''))
  );

  for (const id of currentIds) {
    if (!subscribedIds.has(id)) {
      _subscribeAccount(id);
    }
  }
  for (const [name] of _channels) {
    const id = name.replace('cognition:', '');
    if (!currentIds.has(id)) {
      _unsubscribeAccount(id);
      _pending.delete(id);
    }
  }
}

/**
 * Stop all channels, clear pending queues, deregister subscriber.
 */
async function stop() {
  const admin = getSupabaseAdmin();
  if (admin) {
    for (const [, channel] of _channels) {
      admin.removeChannel(channel);
    }
  }
  _channels.clear();
  _pending.clear();
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
  let pendingCount = 0;
  for (const q of _pending.values()) {
    pendingCount += q.length;
  }
  return {
    state: _started ? 'active' : 'stopped',
    accountCount: _channels.size,
    pendingCount,
  };
}

module.exports = {
  start,
  refresh,
  stop,
  status,
};
