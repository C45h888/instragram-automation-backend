// postgres-telemetry-kernel/substrates/alerts/workers/read-alerts-worker.js
// Alert reader: governed SELECTs on system_alerts.
//
// Owns: SELECT FROM system_alerts filtered by query type.
// Does NOT own: governance policy, signal dispatch, alert dedup.
//
// Query types:
//   by_account            — all alerts for an account
//   unresolved            — unresolved alerts, optionally per-account
//   by_type               — alerts of a specific type, optionally per-account
//   checkExistingWarning  — dedup check: does an unresolved warning of a given type exist?
//
// Governed-read contract: execute(params, governance) → { success, data, error, latencyMs }.
// Called via: reading-substrate → persist-telemetry FSM → DB_READ_REQUESTED.
// Result flows back through: FSM → DB_READ_COMPLETE → CK → READ_RESULT_AVAILABLE → calling domain FSM.

const { getSupabaseAdmin } = require('../../../../config/supabase');

/**
 * @param {object} params
 * @param {string} [params.accountId]  — business_account_id filter (required for most queries)
 * @param {string} [params.businessAccountId] — alias for accountId
 * @param {string} [params.query]       — 'by_account' | 'unresolved' | 'by_type' | 'checkExistingWarning'
 * @param {string} [params.alertType]   — alert_type filter (required for by_type and checkExistingWarning)
 * @param {number} [params.limit]       — max results (default 50)
 * @param {object} governance — CK reference
 * @returns {Promise<{ success: boolean, data?: Array|boolean|null, error?: string, latencyMs: number }>}
 */
async function execute(params, governance) {
  const accountId = params.accountId || params.businessAccountId;
  const { query = 'by_account', alertType, limit = 50 } = params;
  const startTime = Date.now();

  // ── Dedup check: does an unresolved warning exist? ────────────────────────
  if (query === 'checkExistingWarning') {
    if (!accountId || !alertType) {
      return { success: false, data: null, error: 'accountId and alertType required for checkExistingWarning', latencyMs: Date.now() - startTime };
    }

    const supabase = getSupabaseAdmin();
    if (!supabase) {
      return { success: false, data: null, error: 'supabase_unavailable', latencyMs: Date.now() - startTime };
    }

    try {
      const { data, error } = await supabase
        .from('system_alerts')
        .select('id')
        .eq('business_account_id', accountId)
        .eq('alert_type', alertType)
        .eq('resolved', false)
        .maybeSingle();

      if (error) {
        return { success: false, data: null, error: error.message, latencyMs: Date.now() - startTime };
      }

      return { success: true, data: !!data, error: null, latencyMs: Date.now() - startTime };
    } catch (err) {
      return { success: false, data: null, error: err.message, latencyMs: Date.now() - startTime };
    }
  }

  if (!accountId && query !== 'unresolved') {
    return { success: false, data: null, error: 'accountId required for this query type', latencyMs: Date.now() - startTime };
  }

  const supabase = getSupabaseAdmin();
  if (!supabase) {
    return { success: false, data: null, error: 'supabase_unavailable', latencyMs: Date.now() - startTime };
  }

  try {
    let supabaseQuery = supabase
      .from('system_alerts')
      .select('*')
      .order('created_at', { ascending: false })
      .limit(limit);

    if (query === 'by_account') {
      supabaseQuery = supabaseQuery.eq('business_account_id', accountId);
    } else if (query === 'unresolved') {
      supabaseQuery = supabaseQuery.eq('resolved', false);
      if (accountId) supabaseQuery = supabaseQuery.eq('business_account_id', accountId);
    } else if (query === 'by_type') {
      if (!alertType) {
        return { success: false, data: null, error: 'alertType required for by_type query', latencyMs: Date.now() - startTime };
      }
      supabaseQuery = supabaseQuery.eq('alert_type', alertType);
      if (accountId) supabaseQuery = supabaseQuery.eq('business_account_id', accountId);
    }

    const { data, error } = await supabaseQuery;

    if (error) {
      return { success: false, data: null, error: error.message, latencyMs: Date.now() - startTime };
    }

    return { success: true, data: data || [], error: null, latencyMs: Date.now() - startTime };
  } catch (err) {
    return { success: false, data: null, error: err.message, latencyMs: Date.now() - startTime };
  }
}

module.exports = { execute };
