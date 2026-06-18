// graph-capability-kernel/orchestrator.js
// Capability Check Orchestrator: constitutional routing membrane for CAPABILITY_CHECK.
//
// Owns: intercepting CAPABILITY_CHECK domain action from the FSM,
//        routing to capability-check substrate, dispatching result.
//
// Constitutional purity: this orchestrator mechanically sequences
// credential read → quota read → dispatch without understanding policy.
// It never interprets capability outcomes or intent semantics.
//
// Canonical pattern (publishing-kernel/orchestrator.js):
//   wire(governance)    — orchestrator subscribes; substrate only executes
//   execute()           — bounded execution, no subscription ownership
//   dispatch()          — result flows back through CK → FSM
//
// Flow:
//   FSM → CAPABILITY_CHECK (domain action)
//     → governance.subscribeAction('CAPABILITY_CHECK') intercepts
//       → capability-check-substrate.execute()
//         → credential-capability-worker + quota-intelligence-worker
//       → governance.dispatch({ type: 'CAPABILITY_CHECK_COMPLETE', ... })
//         → CK routes to FSM via DOMAIN_EVENT_MAP

const capabilityCheckSubstrate = require('./substrates/capability-check-substrate');

/**
 * Wire this orchestrator to the governance kernel.
 * Registers the CAPABILITY_CHECK subscriber. The orchestrator owns the
 * subscription — the substrate does not.
 */
function wire(governance) {
  governance.subscribeAction('CAPABILITY_CHECK', async (action) => {
    const { businessAccountId, correlationId, sourceDomain } = action;
    if (!businessAccountId) {
      console.warn('[graph-capability-orchestrator] CAPABILITY_CHECK rejected: missing businessAccountId');
      return;
    }

    try {
      const result = await capabilityCheckSubstrate.execute({ businessAccountId }, governance);
      governance.dispatch({
        type: 'CAPABILITY_CHECK_COMPLETE',
        sourceDomain: 'graph-capability',
        businessAccountId,
        correlationId,
        sourceDomain,
        capabilityState: result.capabilityState,
        freshnessMs: result.freshnessMs,
        consecutiveFailures: result.consecutiveFailures,
        lastObservedAt: result.lastObservedAt,
        quotaState: result.quotaState,
      });
    } catch (err) {
      governance.dispatch({
        type: 'CAPABILITY_CHECK_FAILED',
        sourceDomain: 'graph-capability',
        businessAccountId,
        correlationId,
        sourceDomain,
        error: err.message,
      });
    }
  });

  console.log('[graph-capability-orchestrator] Wired — subscribed to CAPABILITY_CHECK');
}

module.exports = { wire };
