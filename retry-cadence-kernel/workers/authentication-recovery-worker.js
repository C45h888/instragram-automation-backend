// retry-cadence-kernel/workers/authentication-recovery-worker.js
// Authentication Recovery Worker — bounded token refresh and credential validation.
//
// CONSTITUTIONAL CONTRACT:
//   Owns: refreshing Supabase JWT tokens, validating credentials,
//         restoring authentication state.
//
//   Does NOT own: token storage (credential-store substrate),
//                 graph API calls (graph-capability-kernel),
//                 auth failure detection (persistence-failure-substrate).
//
// Called by: auth-recovery-substrate.

/**
 * Refresh the Supabase admin client's authentication.
 *
 * The Supabase admin client uses a service_role key which rarely expires.
 * Auth failures in the persistence layer are more commonly JWT expiry on
 * the user-facing token or RLS drift. This worker attempts to re-establish
 * the Supabase client connection by calling getSupabaseAdmin() again and
 * verifying the connection with a lightweight query.
 *
 * @param {object} params — { domain, accountId, intentId, analysis }
 * @param {object} governance — CK reference
 * @returns {Promise<{ success: boolean, error?: string }>}
 */
async function execute(params, governance) {
  const { domain, accountId, intentId, analysis } = params;

  try {
    // Attempt to re-establish the Supabase admin client
    const { getSupabaseAdmin } = require('../../config/supabase');
    const supabase = getSupabaseAdmin();

    if (!supabase) {
      return { success: false, error: 'supabase_admin_unavailable' };
    }

    // Validate connectivity with a lightweight health check
    const { data, error } = await supabase
      .from('system_alerts')
      .select('id')
      .limit(1);

    if (error) {
      return { success: false, error: `auth_refresh_validation_failed: ${error.message}` };
    }

    return { success: true, error: null };
  } catch (err) {
    return { success: false, error: `auth_refresh_exception: ${err.message}` };
  }
}

module.exports = { execute };
