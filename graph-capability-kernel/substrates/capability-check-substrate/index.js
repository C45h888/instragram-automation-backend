// graph-capability-kernel/substrates/capability-check-substrate/index.js
// Capability Check Substrate: bounded capability resolution via two workers.
//
// Owns: sequencing credential-capability-worker → quota-intelligence-worker,
//       result aggregation, error propagation.
// Does NOT own: FSM state, capability inference, retry policy, governance subscription.
//
// Bounded executor only — no start/stop/isStarted membrane interface.
// The orchestrator owns the governance subscription; this substrate only executes.
//
// Contract:
//   execute(params, governance) → { capabilityState, quotaState, freshnessMs, consecutiveFailures, lastObservedAt }
//
// Flow:
//   FSM → governance.dispatch({ type: 'CAPABILITY_CHECK', ... })
//     → CK routes to orchestrator subscriber
//       → capability-check-substrate.execute()
//         → credential-capability-worker.execute() → { state, lastObservedAt, freshnessMs, consecutiveFailures }
//         → quota-intelligence-worker.execute()  → { quotaState }
//         → returns aggregated result to subscriber
//           → governance.dispatch({ type: 'CAPABILITY_CHECK_COMPLETE', ... })

const credentialCapWorker = require('./workers/credential-capability-worker');
const quotaIntWorker = require('./workers/quota-intelligence-worker');

/**
 * Bounded execution — no membrane interface.
 * Executes both workers in parallel and aggregates results.
 */
async function execute(params, governance) {
  const { businessAccountId } = params;

  const [credResult, quotaResult] = await Promise.all([
    credentialCapWorker.execute({ businessAccountId }, governance),
    quotaIntWorker.execute({ businessAccountId }, governance),
  ]);

  return {
    capabilityState: credResult.state,
    freshnessMs: credResult.freshnessMs,
    consecutiveFailures: credResult.consecutiveFailures,
    lastObservedAt: credResult.lastObservedAt,
    quotaState: quotaResult.quotaState,
  };
}

module.exports = { execute };
