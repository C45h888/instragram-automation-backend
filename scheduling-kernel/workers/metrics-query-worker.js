// scheduling-kernel/workers/metrics-query-worker.js
// Metrics Query Worker: bounded execution unit for reading health signals.
//
// Owns: collecting raw health signals from the metrics substrate.
// Does NOT own: governance policy, threshold evaluation, scheduling decisions
//               — those belong to the FSM and telemetry interpreters.
//
// Contract:
//   worker.execute({ queryType, params }) → { data }
//     queryType: 'health'   → returns getHealthSignals()
//     queryType: 'domain'   → params.domain → getDomainBreakdown(domain)
//     queryType: 'account'  → params.accountId → getAccountHealth(accountId)
//
// Invoked by scheduling-fsm via ctx.invokeWorker('metrics-query', {...}).
// The FSM owns the decision to query; this worker owns the mechanics.
// Delegates to scheduling-kernel/substrates/metrics — the kernel-local substrate.

const metricsSubstrate = require('../substrates/metrics');

function execute(params = {}) {
  const { queryType, params: queryParams } = params;

  switch (queryType) {
    case 'health':
      return { data: metricsSubstrate.getHealthSignals() };
    case 'domain':
      return { data: metricsSubstrate.getDomainBreakdown(queryParams?.domain) };
    case 'account':
      return { data: metricsSubstrate.getAccountHealth(queryParams?.accountId) };
    default:
      return { data: null, error: `unknown queryType: ${queryType}` };
  }
}

module.exports = { execute };
