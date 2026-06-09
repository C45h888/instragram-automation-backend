// scheduling-kernel/workers/safety-check-worker.js
// Safety Check Worker: bounded execution unit for operational safety checks.
//
// Owns: executing a single safety check cycle.
// Does NOT own: governance policy, scheduling decisions, health interpretation
//               — those belong to the FSM.
//
// Contract:
//   worker.execute(params) → { ok }
//
// Invoked by scheduling-fsm via ctx.invokeWorker('safety-check', {}).
// The FSM owns the decision to check; this worker owns the mechanics.

const safety = require('../substrates/cadence/operational-safety');

async function execute(params = {}) {
  await safety.runChecks();
  return { ok: true };
}

module.exports = { execute };
