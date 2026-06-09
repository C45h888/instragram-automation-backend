// scheduling-kernel/workers/metrics-report-worker.js
// Metrics Report Worker: bounded execution unit for health signal collection.
//
// Owns: collecting raw health signals from the kernel-local metrics substrate.
// Does NOT own: governance policy (threshold evaluation), scheduling decisions,
//               degradation interpretation — those belong to the FSM and
//               telemetry interpreters.
//
// Contract:
//   worker.execute(params) → { signals: { total, completed, failed, failureRate, windowMs } }
//
// Invoked by scheduling-fsm via ctx.invokeWorker('metrics-report', {}).
// The FSM owns the decision to report; this worker owns the mechanics.
// Delegates to scheduling-kernel/substrates/metrics — the kernel-local
// substrate that reads from the shared global metrics state.

const metricsSubstrate = require('../substrates/metrics');

function execute(params = {}) {
  const signals = metricsSubstrate.getHealthSignals();
  return { signals };
}

module.exports = { execute };
