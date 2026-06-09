// scheduling-kernel/workers/metrics-flush-worker.js
// Metrics Flush Worker: bounded execution unit for batch-writing buffered records.
//
// Owns: draining the FSM's write buffer and committing all records to the
//        metrics substrate in one batch.
// Does NOT own: buffering policy, flush cadence, cache management —
//               those belong to the FSM.
//
// Contract:
//   worker.execute({ records }) → { ok, count, errors? }
//     records: [{ domain, status, latencyMs, accountId }]
//
// Invoked by scheduling-fsm via ctx.invokeWorker('metrics-flush', { records }).
// The FSM owns the buffer and cadence; this worker owns the batch write.

const metricsSubstrate = require('../substrates/metrics');

function execute(params = {}) {
  const records = params.records || [];
  let count = 0;
  const errors = [];

  for (const r of records) {
    try {
      metricsSubstrate.record(r.domain, r.status, r.latencyMs, r.accountId || null);
      count++;
    } catch (err) {
      errors.push({ domain: r.domain, error: err.message });
    }
  }

  return { ok: errors.length === 0, count, errors: errors.length > 0 ? errors : null };
}

module.exports = { execute };
