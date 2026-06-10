// retry-cadence-kernel/workers/connection-recovery-worker.js
// Connection Recovery Worker — bounded retry of failed Supabase operations.
//
// CONSTITUTIONAL CONTRACT:
//   Owns: re-running the failed Supabase write/read through the
//         postgres-telemetry-kernel writers/readers, with the same
//         idempotency key to prevent duplicate state changes.
//
//   Does NOT own: classification (persistence-failure-substrate),
//                 recommendation selection (FSM),
//                 backoff timing (backoff-enforcement-worker),
//                 operation logic (retry-execution-substrate).
//
// Called by: retry-execution-substrate after backoff enforcement.
// Replaces: persister-telemetry-retry-worker.js (stub, deleted in Tier 6).

const db = require('../../postgres-telemetry-kernel/writers');

/**
 * Re-execute a failed Supabase operation.
 *
 * @param {object} params — { domain, accountId, intentId, table, rows, idempotencyKey, analysis, backoff }
 * @param {object} governance — CK reference
 * @returns {Promise<{ success: boolean, error?: string }>}
 */
async function execute(params, governance) {
  const { domain, accountId, intentId, table, rows, idempotencyKey, analysis, readDomain, readId, readParams } = params;

  if (!governance || (typeof governance.dispatchGlobal !== 'function' && typeof governance.dispatch !== 'function')) {
    return { success: false, error: 'governance required for connection recovery' };
  }

  // ── Read path: re-dispatch the original DB_READ_REQUESTED ─────────────
  if (readDomain && readParams) {
    (governance?.dispatchGlobal || governance?.dispatch)({
      type: 'DB_READ_REQUESTED',
      readDomain,
      accountId,
      readId: readId || `${intentId}-retry-${Date.now()}`,
      params: readParams,
      isRetry: true,
      retrySource: 'connection-recovery-worker',
      originalError: analysis?.normalized?.message || null,
    });
    return { success: true, error: null };
  }

  // ── Write path: re-dispatch the original DB_WRITE_REQUESTED ───────────
  const actualRows = rows || [];
  (governance?.dispatchGlobal || governance?.dispatch)({
    type: 'DB_WRITE_REQUESTED',
    domain, accountId, intentId,
    table,
    rows: actualRows,
    idempotencyKey,
    isRetry: true,
    retrySource: 'connection-recovery-worker',
    originalError: analysis?.normalized?.message || null,
  });

  return { success: true, error: null };
}

module.exports = { execute };
