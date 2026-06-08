// postgres-telemetry-kernel/substrates/alerts/workers/read-lifecycle-events-worker.js
// Lifecycle event reader: one bounded SELECT on token_lifecycle_events.
//
// Owns: SELECT FROM token_lifecycle_events filtered by query type.
// Does NOT own: governance policy, signal dispatch.
//
// Governed-read contract: execute(params, governance) → { success, data, error, latencyMs }.
// Called via: reading-substrate → persist-telemetry FSM → DB_READ_REQUESTED.
// Result flows back through: FSM → DB_READ_COMPLETE → CK → READ_RESULT_AVAILABLE → calling domain FSM.

const { getSupabaseAdmin } = require('../../../../config/supabase');

/**
 * @param {{ accountId: string, query: 'by_account' | 'by_credential' | 'recent', credentialId?: string, eventType?: string, limit?: number }} params
 * @param {object} governance — CK reference
 * @returns {Promise<{ success: boolean, data?: Array|null, error?: string, latencyMs: number }>}
 */
async function execute(params, governance) {
  const { accountId, query = 'recent', credentialId, eventType, limit = 50 } = params;
  const startTime = Date.now();

  const supabase = getSupabaseAdmin();
  if (!supabase) {
    return { success: false, data: null, error: 'supabase_unavailable', latencyMs: Date.now() - startTime };
  }

  try {
    let supabaseQuery = supabase
      .from('token_lifecycle_events')
      .select('*')
      .order('created_at', { ascending: false })
      .limit(limit);

    if (query === 'by_account') {
      if (!accountId) {
        return { success: false, data: null, error: 'accountId required for by_account query', latencyMs: Date.now() - startTime };
      }
      supabaseQuery = supabaseQuery.eq('business_account_id', accountId);
    } else if (query === 'by_credential') {
      if (!credentialId) {
        return { success: false, data: null, error: 'credentialId required for by_credential query', latencyMs: Date.now() - startTime };
      }
      supabaseQuery = supabaseQuery.eq('credential_id', credentialId);
    }
    // 'recent' — no additional filter

    if (eventType) {
      supabaseQuery = supabaseQuery.eq('event_type', eventType);
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
