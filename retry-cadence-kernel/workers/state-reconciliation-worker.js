// retry-cadence-kernel/workers/state-reconciliation-worker.js
// State Reconciliation Worker — bounded state comparison and divergence detection.
//
// CONSTITUTIONAL CONTRACT:
//   Owns: comparing expected state against observed state, detecting
//         divergence vectors, triggering reconciliation cycles.
//
//   Does NOT own: reconciliation cycle execution (reconciliation-kernel),
//                 classification (persistence-failure-substrate),
//                 conflict resolution (conflict-resolution-worker).
//
// Called by: reconciliation-substrate.

const { getSupabaseAdmin } = require('../../config/supabase');

/**
 * Compare expected vs observed state for a failed operation.
 *
 * For consistency failures and general state reconciliation:
 * - Check if the row the write was targeting exists in the expected state.
 * - If the row exists with the expected values, the write landed but
 *   ACK was lost → reconciliation confirms it.
 * - If the row doesn't exist, the write genuinely failed → flag for retry.
 *
 * @param {object} params — { domain, accountId, intentId, table, analysis }
 * @param {object} governance — CK reference
 * @returns {Promise<{ success: boolean, resolution?: string, error?: string }>}
 */
async function execute(params, governance) {
  const { domain, accountId, intentId, table, analysis } = params;

  const supabase = getSupabaseAdmin();
  if (!supabase) {
    return { success: false, error: 'supabase_unavailable_during_reconciliation' };
  }

  // For reconciliation to work, we need the original primary key.
  // The idempotency key embeds (lineageId+table+pkField+pkValue).
  // We can extract pkValue by splitting on '|'.
  const iKey = analysis?.idempotencyKey || '';
  const keyParts = iKey.split('|');
  const pkValue = keyParts.length >= 5 ? keyParts[4] : null;

  if (!pkValue) {
    // No pk to query — trigger a full reconciliation cycle
    (governance?.dispatchGlobal || governance?.dispatch)({
      type: 'RECONCILIATION_TRIGGERED',
      domain: domain || 'persist-telemetry',
      accountId: accountId || '*',
      intentId,
      table,
      source: 'state-reconciliation-worker',
      reason: 'no_pk_available_for_spot_check',
    });
    return { success: false, error: 'no_pk_for_spot_check', resolution: 'triggered_full_cycle' };
  }

  // Spot-check: does the expected row exist?
  // We use the table's primary key field parsed from the key.
  // The key format is: lineageId|table|pkField|pkValue
  const pkField = keyParts.length >= 4 ? keyParts[3] : 'id';

  try {
    const { data, error } = await supabase
      .from(table)
      .select('*')
      .eq(pkField, pkValue)
      .limit(1);

    if (error) {
      return { success: false, error: `reconciliation_query_failed: ${error.message}` };
    }

    if (data && data.length > 0) {
      // Row exists — the write landed but ACK was lost.
      return { success: true, resolution: 'already_persisted', error: null };
    }

    // Row does not exist — the write genuinely failed.
    // Trigger a re-write through retry-execution.
    (governance?.dispatchGlobal || governance?.dispatch)({
      type: 'RETRY_OPERATION_AUTHORIZED',
      domain: domain || 'persist-telemetry',
      accountId: accountId || '*',
      intentId,
      table,
      backoff: analysis?.backoff || null,
      idempotencyKey: iKey,
      analysis,
    });

    return { success: true, resolution: 're_queued_for_retry', error: null };
  } catch (err) {
    return { success: false, error: `reconciliation_exception: ${err.message}` };
  }
}

module.exports = { execute };
