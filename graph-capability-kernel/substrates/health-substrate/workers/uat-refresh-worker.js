// graph-capability-kernel/substrates/health-substrate/workers/uat-refresh-worker.js
// UAT proactive refresh worker — batch-SELECT only (Phase D).
//
// Owns: ONE bounded SELECT against instagram_credentials for user-type
//       credentials expiring within windowDays. Returns the list of due UATs.
// Does NOT own: the per-cred vault.uat.refresh call, alert writes, rate-limit
//               pacing, the 14d window policy. The FSM owns the window policy
//               (UAT_REFRESH_WINDOW_MS); the façade iterates and calls
//               vault.uat.refresh per cred (vault.uat.refresh is the canonical
//               cross-substrate façade call — it is itself a bounded operation
//               representing "refresh this UAT").
//
// Migration origin: services/sync/token-health.js → runUATRefreshCheck() lines 246-322
//   Legacy: query → loop → vault.uat.refresh → fireAndForgetInsert alert + event row.
//   Phase D: query → return batch → façade iterates → calls vault.uat.refresh
//   per cred → façade writes alerts/events.
//
// Constitutional wiring (unchanged):
//   vault.uat.refresh internally emits TOKEN_REFRESHED via signal-dispatch on
//   success. The façade emits CAPABILITY_EVALUATE / CAPABILITY_OBSERVATION
//   per the recovery branch. On refresh failure, the façade emits the
//   degraded-signal envelope.

const { getSupabaseAdmin } = require('../../../../config/supabase');

class UatRefreshWorker {
  /**
   * Batch-SELECT user-type credentials expiring within windowDays. One bounded I/O call.
   *
   * @param {{ windowDays?: number, businessAccountId?: string|null }} [opts]
   *   businessAccountId — optional filter for per-cred targeted refresh.
   * @returns {Promise<{
   *   uats: Array<{ id: string, user_id: string, business_account_id: string, expires_at: string }>,
   *   stats: { total: number },
   * }>}
   */
  async execute({ windowDays = 14, businessAccountId = null } = {}) {
    const supabase = getSupabaseAdmin();
    if (!supabase) {
      return { uats: [], stats: { total: 0 } };
    }

    const cutoffIso = new Date(Date.now() + windowDays * 24 * 60 * 60 * 1000).toISOString();

    let query = supabase
      .from('instagram_credentials')
      .select('id, user_id, business_account_id, expires_at')
      .eq('token_type', 'user')
      .eq('is_active', true)
      .not('expires_at', 'is', null)
      .lt('expires_at', cutoffIso);
    if (businessAccountId) {
      query = query.eq('business_account_id', businessAccountId);
    }
    const { data: expiringUATs, error } = await query;

    if (error) throw error;
    if (!expiringUATs?.length) {
      return { uats: [], stats: { total: 0 } };
    }

    return { uats: expiringUATs, stats: { total: expiringUATs.length } };
  }
}

module.exports = UatRefreshWorker;
