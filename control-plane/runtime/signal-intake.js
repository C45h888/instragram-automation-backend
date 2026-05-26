// control-plane/runtime/signal-intake.js
// Signal Intake: bounded signal bus subscription and realtime lifecycle.
//
// Owns: subscribing to signal bus, starting/stopping Supabase Realtime.
// Does NOT own: evaluation, buffering, worker lifecycle, emission.
//
// Contract:
//   signalIntake.start(accounts, onEvent) → subscribe + start realtime
//   signalIntake.stop()                   → unsubscribe + stop realtime

const signalBus = require('../signal-bus');
const realtime = require('../../substrates/realtime');
const { getActiveAccounts } = require('../../substrates/persistence');

const TOPIC = 'db:insert';

let _onEvent = null;
let _started = false;
let _accountCount = 0;

/**
 * Returns live runtime state. Deterministic, no side effects.
 * @returns {{ state: 'active'|'stopped', accounts: number, topic: string }}
 */
function status() {
  return {
    state: _started ? 'active' : 'stopped',
    accounts: _accountCount,
    topic: TOPIC,
  };
}

/**
 * Start signal intake: subscribe to signal bus and open realtime channels.
 * Idempotent — calling on an already-active intake is a safe no-op.
 *
 * @param {Array<{id: string}>|null} accounts — null resolves to getActiveAccounts()
 * @param {Function} onEvent — called with { accountId, table, record } on db:insert
 * @throws {Error} if onEvent is not a function
 */
async function start(accounts, onEvent) {
  if (typeof onEvent !== 'function') {
    throw new Error(`[signal-intake] onEvent must be a function, got ${typeof onEvent}`);
  }
  if (_started) return;
  if (!accounts) {
    accounts = await getActiveAccounts();
  }
  _accountCount = accounts.length;
  _onEvent = onEvent;

  // Observability: realtime subscription state transition
  _emitTransition('STOPPED', 'SUBSCRIBED');

  signalBus.subscribe(TOPIC, _onEvent);
  await realtime.startRealtime(accounts);
  _started = true;
  console.log(`[signal-intake] Started — listening on '${TOPIC}' for ${_accountCount} account(s)`);
}

/**
 * Stop signal intake: unsubscribe from signal bus and close realtime channels.
 * Idempotent — calling on an already-stopped intake is a safe no-op.
 * Awaitable — resolves when realtime channels are fully closed.
 */
async function stop() {
  if (!_started) return;
  if (_onEvent) {
    signalBus.unsubscribe(TOPIC, _onEvent);
    _onEvent = null;
  }

  // Observability: realtime unsubscribed state transition
  _emitTransition('SUBSCRIBED', 'STOPPED');

  await realtime.stopRealtime();
  _started = false;
  _accountCount = 0;
  console.log('[signal-intake] Stopped');
}

function _emitTransition(previousState, nextState) {
  try {
    const observability = require('../observability/emitters/transition-emitter');
    observability.transition({
      domain: 'realtime',
      entity: 'realtime',
      entityId: 'signal-intake',
      previousState,
      nextState,
      authority: 'signal-intake-runtime',
      raw: { accountCount: _accountCount },
    });
  } catch (err) {
    console.warn('[signal-intake] Observability transition error:', err.message);
  }
}

module.exports = { start, stop, status };
