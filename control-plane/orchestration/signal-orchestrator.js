// control-plane/orchestration/signal-orchestrator.js
// Signal Orchestrator: constitutional coordination membrane.
//
// Owns: routing signal intake events into the buffer,
//        forwarding buffer flush events upward to governance.
// Does NOT own: signal interpretation, evaluation policy,
//               buffer semantics, debounce logic.
//
// Constitutional purity: this orchestrator mechanically wires
// signal intake → buffer → governance without interpreting
// what the signals mean. It never evaluates content.

const signalIntake = require('../runtime/signal-intake');
const buffer = require('../runtime/buffer');

/**
 * Wire this orchestrator to the governance kernel.
 * Registers buffer flush forwarding.
 *
 * @param {object} governance — governance kernel module
 */
function wire(governance) {
  // ── Buffer flush → BUFFER_FLUSH_READY upward ───────────────────────────
  buffer.onFlush(async (accountId, events) => {
    governance.dispatch({
      type: 'BUFFER_FLUSH_READY',
      accountId,
      events,
      eventCount: events.length,
    });
  });
}

/**
 * Start signal intake. Called during boot sequence.
 * Pure mechanical wiring — no signal interpretation.
 *
 * @param {object} governance — governance kernel module
 */
async function start(governance) {
  await signalIntake.start(null, (event) => {
    buffer.ingest(event);
    governance.dispatch({
      type: 'BUFFER_EVENT_INGESTED',
      accountId: event.accountId,
    });
  });
}

/**
 * Stop signal intake. Called during shutdown.
 */
async function stop() {
  await signalIntake.stop();
}

module.exports = { wire, start, stop };
