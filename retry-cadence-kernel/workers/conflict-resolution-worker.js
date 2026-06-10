// retry-cadence-kernel/workers/conflict-resolution-worker.js
// Conflict Resolution Worker — bounded resolution of uniqueness and FK conflicts.
//
// CONSTITUTIONAL CONTRACT:
//   Owns: checking if unique constraint violations are idempotent (data exists),
//         resolving FK violations (parent deletion detection),
//         signalling escalation paths for unresolved conflicts.
//
//   Does NOT own: state comparison (state-reconciliation-worker),
//                 classification (persistence-failure-substrate),
//                 recommendation selection (FSM).
//
// Called by: reconciliation-substrate for conflict subtypes.

const { getSupabaseAdmin } = require('../../config/supabase');

/**
 * Resolve a unique constraint or FK violation.
 *
 * Strategy:
 *   unique_constraint_violation → check if row exists (idempotent write)
 *   foreign_key_violation       → the parent row was deleted mid-write
 *                                  (consistency failure, escalate)
 *   http_409                    → generic HTTP conflict, treat as unique
 *
 * @param {object} params — { domain, accountId, intentId, table, analysis }
 * @param {object} governance — CK reference
 * @returns {Promise<{ success: boolean, resolution?: string, error?: string }>}
 */
async function execute(params, governance) {
  const { domain, accountId, intentId, table, analysis } = params;
  const subtype = analysis?.subtype || 'unknown';

  const supabase = getSupabaseAdmin();
  if (!supabase) {
    return { success: false, error: 'supabase_unavailable_during_conflict_resolution' };
  }

  // Extract pk from idempotency key
  const iKey = analysis?.idempotencyKey || '';
  const keyParts = iKey.split('|');
  const pkField = keyParts.length >= 4 ? keyParts[3] : 'id';
  const pkValue = keyParts.length >= 5 ? keyParts[4] : null;

  if (!pkValue) {
    return { success: false, error: 'no_pk_for_conflict_resolution' };
  }

  // ── Unique constraint: idempotent write check ─────────────────────
  if (subtype === 'unique_constraint_violation' || subtype === 'http_409') {
    try {
      const { data, error } = await supabase
        .from(table)
        .select('*')
        .eq(pkField, pkValue)
        .limit(1);

      if (error) {
        return { success: false, error: `conflict_query_failed: ${error.message}` };
      }

      if (data && data.length > 0) {
        // Row exists — the write already landed, this is an idempotent conflict.
        return { success: true, resolution: 'already_persisted', error: null };
      }

      // Row doesn't exist despite unique constraint? Edge case — escalate.
      return { success: false, error: 'unique_constraint_without_matching_row', resolution: 'escalate' };
    } catch (err) {
      return { success: false, error: `conflict_resolution_exception: ${err.message}` };
    }
  }

  // ── FK violation: parent deletion mid-write ──────────────────────
  if (subtype === 'foreign_key_violation') {
    // This is a consistency failure. The parent row was deleted between
    // when the write was validated and when it was committed.
    // Escalate to state reconciliation for a full cycle.
    (governance?.dispatchGlobal || governance?.dispatch)({
      type: 'RECONCILIATION_TRIGGERED',
      domain: domain || 'persist-telemetry',
      accountId: accountId || '*',
      intentId,
      table,
      source: 'conflict-resolution-worker',
      reason: `fk_violation_consistency_failure: ${analysis?.normalized?.details || 'unknown_parent'}`,
    });

    return {
      success: false,
      error: 'fk_violation_escalated_to_reconciliation',
      resolution: 'escalated_to_reconciliation',
    };
  }

  // ── Transaction rollback: transient conflict, retryable ──────────
  if (subtype === 'transaction_rollback') {
    (governance?.dispatchGlobal || governance?.dispatch)({
      type: 'RETRY_OPERATION_AUTHORIZED',
      domain: domain || 'persist-telemetry',
      accountId: accountId || '*',
      intentId,
      table,
      backoff: analysis?.backoff || null,
      idempotencyKey: iKey,
      analysis: { ...analysis, retryable: true },
    });

    return { success: true, resolution: 're_queued_for_retry', error: null };
  }

  return { success: false, error: `unresolved_conflict_subtype: ${subtype}` };
}

module.exports = { execute };
