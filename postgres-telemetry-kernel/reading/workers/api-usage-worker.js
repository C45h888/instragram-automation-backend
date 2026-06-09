// postgres-telemetry-kernel/reading/workers/api-usage-worker.js
// API Usage Worker: governed read for the api_usage table.
//
// Owns: checkHourlyLimit — SELECT SUM(request_count) for user in current hour.
// Does NOT own: governance policy (FSM), routing (CK), rate-limit decisions.
//
// Operationally bounded to: db.api-usage read domain.
// Dispatched by: reading/index.js (via dispatchRead).
//
// Constitutional flow:
//   GCFSM → CK.governedRead('db.api-usage', { query: 'checkHourlyLimit', userId })
//     → CK → persist-telemetry FSM → reading-substrate → worker
//     → READ_RESULT_AVAILABLE → GCFSM resolves

const { getSupabaseAdmin } = require('../../../config/supabase');

/**
 * @param {object} params — { query: 'checkHourlyLimit', userId, limit?: number }
 * @param {object} governance — CK module
 * @returns {Promise<{ success: boolean, data?: object, error?: string, latencyMs: number }>}
 */
async function execute(params, governance) {
  const { query = 'checkHourlyLimit', userId, limit = 200 } = params;
  const startTime = Date.now();

  if (!userId) {
    return { success: false, data: null, error: 'userId required', latencyMs: Date.now() - startTime };
  }

  const supabase = getSupabaseAdmin();
  if (!supabase) {
    return { success: false, data: null, error: 'supabase_unavailable', latencyMs: Date.now() - startTime };
  }

  try {
    const now = new Date();
    const hourBucket = new Date(now);
    hourBucket.setMinutes(0, 0, 0);

    const { data, error } = await supabase
      .from('api_usage')
      .select('request_count')
      .eq('user_id', userId)
      .gte('hour_bucket', hourBucket.toISOString());

    if (error) {
      return { success: false, data: null, error: error.message, latencyMs: Date.now() - startTime };
    }

    const current = (data || []).reduce((sum, row) => sum + (row.request_count || 0), 0);

    return {
      success: true,
      data: { current, limit, remaining: Math.max(0, limit - current) },
      error: null,
      latencyMs: Date.now() - startTime,
    };
  } catch (err) {
    return { success: false, data: null, error: err.message, latencyMs: Date.now() - startTime };
  }
}

module.exports = { execute };
