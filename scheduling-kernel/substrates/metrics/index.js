// scheduling-kernel/substrates/metrics/index.js
// Metrics Substrate: bounded health signal aggregation for scheduling domain.
//
// Owns: collecting raw health signals, recording execution outcomes,
//        exposing them through a kernel-local interface.
// Does NOT own: governance policy, threshold evaluation —
//               those belong to the FSM and telemetry interpreters.
//
// Architectural invariant:
//   This is the sole public API for the metrics substrate within the
//   scheduling kernel. It delegates all operations to the real
//   implementation at ./metrics-substrate.js. External callers (acquisition,
//   telemetry, interpreters) go through CK.recordMetric()/CK.queryMetrics()
//   which route through the scheduling FSM → workers → this substrate.
//
// Contract:
//   metrics.record(domain, status, latencyMs, accountId)  → void
//   metrics.getHealthSignals()      → { total, completed, failed, failureRate, windowMs }
//   metrics.getDomainBreakdown()    → { [domain]: { total, ... } }
//   metrics.getAccountHealth(id)    → { total, ... } | null
//   metrics.init()                  → Promise<void>
//   metrics.reset()                 → void

const impl = require('./metrics-substrate');

module.exports = {
  record:            impl.record,
  getHealthSignals:  impl.getHealthSignals,
  getDomainBreakdown: impl.getDomainBreakdown,
  getAccountHealth:  impl.getAccountHealth,
  reset:             impl.reset,
  init:              impl.init,
  METRICS_WINDOW_MS: impl.METRICS_WINDOW_MS,
};
