// retry-cadence-kernel/workers/timeout-recovery-worker.js
// Timeout Recovery Worker — decomposes slow operations for architectural timeouts.
//
// CONSTITUTIONAL CONTRACT:
//   Owns: analyzing execution context for timeout operations, decomposing
//         slow Supabase operations into smaller batches, re-dispatching
//         in chunks.
//
//   Does NOT own: timeout detection (persistence-failure-substrate §8),
//                 retry scheduling (backoff-enforcement-worker),
//                 operation execution (connection-recovery-worker).
//
// Called by: retry-execution-substrate when analysis.category === 'TIMEOUT'
//            AND analysis.timeout.architecturalPressure === true.
//
// Strategy: for architectural timeouts (slow queries, lock contention,
// long transactions), decompose the operation into smaller batches.
// If the analysis has row-level context, split into N batches.
// Otherwise, escalate — a single slow query with no decomposition
// path is an architectural issue requiring operator attention.

const db = require('../../postgres-telemetry-kernel/writers');

/**
 * Decompose and re-dispatch a timed-out operation.
 *
 * @param {object} params — { domain, accountId, intentId, table, analysis, backoff }
 * @param {object} governance — CK reference
 * @returns {Promise<{ success: boolean, error?: string }>}
 */
async function execute(params, governance) {
  const { domain, accountId, intentId, table, analysis } = params;

  if (!governance || (typeof governance.dispatchGlobal !== 'function' && typeof governance.dispatch !== 'function')) {
    return { success: false, error: 'governance required' };
  }

  const rows = analysis?.normalized?.rows || [];

  // If we have no row-level context, we can't decompose. Escalate.
  if (!rows || rows.length === 0) {
    return {
      success: false,
      error: `timeout_decomposition_failed: no rows to split for table ${table}`,
    };
  }

  // Decompose into batches of at most 10 rows
  const BATCH_SIZE = 10;
  const batches = [];
  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    batches.push(rows.slice(i, i + BATCH_SIZE));
  }

  // Fire-and-forget each batch as a separate DB_WRITE_REQUESTED
  for (let b = 0; b < batches.length; b++) {
    (governance.dispatchGlobal || governance.dispatch)({
      type: 'DB_WRITE_REQUESTED',
      domain, accountId,
      intentId: `${intentId || 'retry'}-batch-${b}`,
      table,
      rows: batches[b],
      idempotencyKey: `${intentId || 'retry'}-batch-${b}`,
      isRetry: true,
      retrySource: 'timeout-recovery-worker',
      batchN: b,
      totalBatches: batches.length,
    });
  }

  return {
    success: true,
    error: null,
    batchesDecomposed: batches.length,
  };
}

module.exports = { execute };
