// postgres-telemetry-kernel/reading/workers/user-profiles-worker.js
// User Profiles Worker: governed Supabase read for the user_profiles table.
//
// Owns: count() — SELECT count(*) FROM user_profiles.
// Does NOT own: governance policy (FSM), routing (CK).
//
// Operationally bounded to: db.user-profiles read domain.
// Dispatched by: reading/index.js (via dispatchRead).
//
// Constitutional flow:
//   Caller → CK.governedRead('db.user-profiles', params)
//     → CK(DB_READ_REQUESTED) → persist-telemetry-fsm
//     → reading-substrate.executeRead → registry → worker.execute()
//     → DB_READ_COMPLETE → READ_RESULT_AVAILABLE

const { getSupabaseAdmin } = require('../../../config/supabase');

/**
 * @param {object} params     — { query: 'count' }
 * @param {object} governance — CK module
 * @returns {Promise<{success: boolean, data?: number, error?: string, latencyMs: number}>}
 */
async function execute(params, governance) {
  const { query = 'count' } = params;
  const startTime = Date.now();

  const supabase = getSupabaseAdmin();
  if (!supabase) {
    return { success: false, data: null, error: 'supabase_unavailable', latencyMs: Date.now() - startTime };
  }

  try {
    const { count, error } = await supabase
      .from('user_profiles')
      .select('*', { count: 'exact', head: true });

    if (error) {
      return { success: false, data: null, error: error.message, latencyMs: Date.now() - startTime };
    }

    return { success: true, data: count || 0, error: null, latencyMs: Date.now() - startTime };
  } catch (err) {
    return { success: false, data: null, error: err.message, latencyMs: Date.now() - startTime };
  }
}

module.exports = { execute };
