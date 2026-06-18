// graph-capability-kernel/orchestrator.js
// Capability Check Orchestrator (deprecated — evaluation-worker only).
//
// Previously owned: CAPABILITY_CHECK subscription (routed to capability-check
// substrate). Now the FSM's CAPABILITY_CHECK handler calls ctx.invokeWorker()
// directly. The orchestrator remains for one concern only:
//
//   evaluationWorker.start(governance)  — subscribes to
//   CAPABILITY_EVALUATION_STARTED to trigger re-inference when vault state
//   changes. This is also wired directly in index.js for symmetry.
//
// The orchestrator is kept as a thin compatibility shim so that modules
// referencing it don't break. It may be fully removed in a future cleanup.

const evaluationWorker = require('./substrates/workers/evaluation-worker');

/**
 * Start evaluation-worker subscription.
 * Kept for backward compatibility — the primary wire is in index.js.
 */
function wire(governance) {
  evaluationWorker.start(governance);
}

module.exports = { wire };
