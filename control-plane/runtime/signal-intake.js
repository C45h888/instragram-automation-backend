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

/**
 * Start signal intake: subscribe to signal bus and open realtime channels.
 * @param {Array<{id: string}>} accounts — active accounts from persistence
 * @param {Function} onEvent — called with { accountId, table, record } on db:insert
 */
async function start(accounts, onEvent) {
  if (_started) return;
  if (!accounts) {
    accounts = await getActiveAccounts();
  }
  _onEvent = onEvent;
  signalBus.subscribe(TOPIC, _onEvent);
  await realtime.startRealtime(accounts);
  _started = true;
  console.log(`[signal-intake] Started — listening on '${TOPIC}' for ${accounts.length} account(s)`);
}

/**
 * Stop signal intake: unsubscribe from signal bus and close realtime channels.
 */
async function stop() {
  if (!_started) return;
  if (_onEvent) {
    signalBus.unsubscribe(TOPIC, _onEvent);
    _onEvent = null;
  }
  await realtime.stopRealtime();
  _started = false;
  console.log('[signal-intake] Stopped');
}

module.exports = { start, stop };
