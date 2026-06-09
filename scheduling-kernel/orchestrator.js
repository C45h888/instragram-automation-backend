// scheduling-kernel/orchestrator.js
// Cadence Orchestrator: constitutional coordination membrane.
// Kernelized from: control-plane/orchestration/cadence-orchestrator.js
//
// Owns: fanning completion events from the scheduling FSM back into CK,
//        triggering the next step in the maintenance cycle.
// Does NOT own: governance policy, domain semantics, execution intelligence.
//
// Constitutional purity: this orchestrator is a PACKET ROUTER.
// The scheduling FSM invokes workers through ctx.invokeWorker() and emits
// completion actions. This orchestrator subscribes to those actions and
// fans them back into CK.dispatch() so the FSM receives them as governance
// events and transitions to the next state.
//
// State machine chain:
//   CADENCE_TICK → SCANNING (FSM invokes lifecycle-refresh worker)
//   → emits LIFECYCLE_REFRESHED → orchestrator fans back → FSM: REFRESHING
//   → FSM invokes safety-check worker
//   → emits SAFETY_CHECK_COMPLETE → orchestrator fans back → FSM: CHECKING
//   → FSM invokes metrics-report worker
//   → emits WORKER_METRICS_REPORTED → orchestrator fans back → FSM: IDLE
//   → FSM emits UPDATE_DOMAIN_LIST, cycle complete
//
// Workers are registered with CK in orchastrator.js. The FSM invokes them
// through the CTX gate (ownership, contract, sanity).

/**
 * Wire this orchestrator to the governance kernel.
 * Subscribes to the 3 completion actions and fans them back into CK
 * as governance events. Pure fan-in/fan-out — no imports, no execution.
 *
 * @param {object} governance — governance kernel module
 */
function wire(governance) {
  // ── LIFECYCLE_REFRESHED → fan back to CK ──────────────────────────────
  // FSM emitted this after lifecycle-refresh worker completed.
  // Fan it back so CK routes it to scheduling FSM → REFRESHING state.
  governance.subscribeAction('LIFECYCLE_REFRESHED', (action) => {
    governance.dispatch({
      type: 'LIFECYCLE_REFRESHED',
      accountIds: action.accountIds || [],
    });
  });

  // ── SAFETY_CHECK_COMPLETE → fan back to CK ────────────────────────────
  // FSM emitted this after safety-check worker completed.
  // Fan it back so CK routes it to scheduling FSM → CHECKING state.
  governance.subscribeAction('SAFETY_CHECK_COMPLETE', (action) => {
    governance.dispatch({ type: 'SAFETY_CHECK_COMPLETE' });
  });

  // ── WORKER_METRICS_REPORTED → fan back to CK ──────────────────────────
  // FSM emitted this after metrics-report worker completed.
  // Fan it back so CK routes it to scheduling FSM → IDLE state.
  // The engagement-telemetry-interpreter also receives this as an action
  // (separate subscription) for threshold evaluation — no conflict.
  governance.subscribeAction('WORKER_METRICS_REPORTED', (action) => {
    governance.dispatch({
      type: 'WORKER_METRICS_REPORTED',
      total: action.total || 0,
      failed: action.failed || 0,
      failureRate: action.failureRate || 0,
      windowMs: action.windowMs || 0,
    });
  });
}

module.exports = { wire };
