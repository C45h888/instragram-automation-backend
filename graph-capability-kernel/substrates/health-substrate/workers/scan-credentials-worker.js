// graph-capability-kernel/substrates/health-substrate/workers/scan-credentials-worker.js
// Token health scan worker — batch-SELECT only (Phase D).
//
// Owns: ONE bounded SELECT against instagram_credentials. Returns the list
//       of active page credentials for the façade to iterate over.
// Does NOT own: per-cred /debug_token, classification, recovery, alert writes,
//               rate-limit pacing, the 24h skip gate. The FSM owns the
//               skip-gate policy (lastTokenHealthCheckAt vs TOKEN_HEALTH_WINDOW_MS).
//
// Migration origin: services/sync/token-health.js → runTokenHealthCheck() inner loop (lines 66-209).
//   Legacy mixed: scan + classify + recovery + alert writes + audit in one 145-line for-loop.
//   Phase A moved the skip-gate policy to the FSM (per-cred cadence timestamps).
//   Phase D moves the per-cred /debug_token + classification into the façade;
//   this worker is now a single bounded SELECT.
//
// Constitutional wiring (unchanged):
//   The façade composes: this worker (batch-SELECT) → for each cred, asks
//   fsm._shouldCheck(baId, 'token_health') → if due, calls vault.uat.detect
//   and vault.pat.retrieve (both single-call workers) → classifies result →
//   emits CAPABILITY_EVALUATE / CAPABILITY_OBSERVATION via signal-dispatch.

const { getSupabaseAdmin } = require('../../../../config/supabase');

class ScanCredentialsWorker {
  /**
   * Batch-SELECT active page credentials. One bounded I/O call.
   *
   * @param {{ businessAccountId?: string|null }} [opts]
   *   businessAccountId — optional filter. When provided, only that cred is
   *   returned (used by the membrane for per-cred targeted scans emitted by
   *   the FSM's CAPABILITY_CADENCE_TICK or targeted CAPABILITY_BOOTSTRAP).
   * @returns {Promise<{
   *   creds: Array<{ id: string, user_id: string, business_account_id: string, debug_token_checked_at: string|null, issued_at: string|null }>,
   *   stats: { total: number },
   * }>}
   */
  async execute({ businessAccountId = null } = {}) {
    const supabase = getSupabaseAdmin();
    if (!supabase) {
      return { creds: [], stats: { total: 0 } };
    }

    let query = supabase
      .from('instagram_credentials')
      .select('id, user_id, business_account_id, debug_token_checked_at, issued_at')
      .eq('token_type', 'page')
      .eq('is_active', true);
    if (businessAccountId) {
      query = query.eq('business_account_id', businessAccountId);
    }
    const { data: credentials, error } = await query;

    if (error) throw error;
    if (!credentials?.length) {
      return { creds: [], stats: { total: 0 } };
    }

    return { creds: credentials, stats: { total: credentials.length } };
  }
}

module.exports = ScanCredentialsWorker;
