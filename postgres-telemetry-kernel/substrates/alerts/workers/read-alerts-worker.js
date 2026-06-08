// postgres-telemetry-kernel/substrates/alerts/workers/read-alerts-worker.js
// Alert reader: one bounded SELECT on system_alerts.
//
// Owns: SELECT FROM system_alerts filtered by query type.
// Does NOT own: governance policy, signal dispatch, alert dedup.
//
// Governed-read contract: execute(params, governance) → { success, data, error, latencyMs }.
// Called via: reading-substrate → persist-telemetry FSM → DB_READ_REQUESTED.
// Result flows back through: FSM → DB_READ_COMPLETE → CK → READ_RESULT_AVAILABLE → calling domain FSM.

const { getSupabaseAdmin } = require('../../../../config/supabase');

/**
 * @param {{ accountId: string, query: 'by_account' | 'unresolved' | 'by_type', alertType?: string, limit?: number }} params
 * @param {object} governance — CK reference
 * @returns {Promise<{ success: boolean, data?: Array|null, error?: string, latencyMs: number }>}
 */
async function execute(params, governance) {
  const { accountId, query = 'by_account', alertType, limit = 50 } = params;
  const startTime = Date.now();

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
