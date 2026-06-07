// graph-capability-kernel/substrates/health-substrate/workers/uat-refresh-worker.js
// UAT proactive refresh worker — scan expiring UATs, attempt refresh per cred.
//
// Owns: ONE bounded iteration over UATs expiring within 14 days, calling
//       vault.uat.refresh() for each.
// Does NOT own: data_access_expires_at scan (data-access-expiry-worker),
//               alert writes, audit logging, rate-limit pacing between calls.
//
// Migration origin: services/sync/token-health.js → runUATRefreshCheck() lines 246-322
//   (the 14-day expiring-UAT scan + refresh loop).
//   Legacy: query → loop → vault.uat.refresh → fireAndForgetInsert alert + event row.
//   This worker: query → loop → vault.uat.refresh → return structured result.
//   Alert writes stay with the orchestrator (per "steady migration" directive — dedup
//   patterns aren't being collapsed in this pass).
//
// Note: vault.uat.refresh internally emits TOKEN_REFRESHED via signal-dispatch on
//   success. We don't re-emit here. The legacy token-health NEVER emitted
//   TOKEN_REFRESHED on this path even though it called refresh — bug in legacy, not
//   in our migration. signal-dispatch handles it correctly now.

const { getSupabaseAdmin } = require('../../../../config/supabase');
const vault = require('../../vault');

class UatRefreshWorker {
  /**
   * Find UATs expiring within 14 days and attempt refresh for each.
   *
   * @param {{ triggerBridge?: object, windowDays?: number }} [opts]
   * @returns {Promise<{
   *   results: Array<{
   *     uat: { id: string, user_id: string, business_account_id: string, expires_at: string },
   *     daysLeft: number,
   *     success: boolean,
   *     newExpiresAt: string|null,
   *     error: string|null,
   *   }>,
   *   stats: { refreshed: number, failed: number, total: number },
   * }>}
   */
  async execute({ triggerBridge, windowDays = 14 } = {}) {
    const supabase = getSupabaseAdmin();
    if (!supabase) {
      return { results: [], stats: { refreshed: 0, failed: 0, total: 0 } };
    }

    const cutoffIso = new Date(Date.now() + windowDays * 24 * 60 * 60 * 1000).toISOString();

    const { data: expiringUATs, error } = await supabase
      .from('instagram_credentials')
      .select('id, user_id, business_account_id, expires_at')
      .eq('token_type', 'user')
      .eq('is_active', true)
      .not('expires_at', 'is', null)
      .lt('expires_at', cutoffIso);

    if (error) throw error;
    if (!expiringUATs?.length) {
      return { results: [], stats: { refreshed: 0, failed: 0, total: 0 } };
    }

    const results = [];
    let refreshed = 0, failed = 0;

    for (const uat of expiringUATs) {
      const daysLeft = Math.ceil((new Date(uat.expires_at) - Date.now()) / (24 * 60 * 60 * 1000));

      try {
        const refreshResult = await vault.uat.refresh({
          userId: uat.user_id,
          businessAccountId: uat.business_account_id,
          triggerBridge,
        });

        results.push({
          uat,
          daysLeft,
          success: refreshResult.success === true,
          newExpiresAt: refreshResult.expiresAt || null,
          error: refreshResult.success ? null : (refreshResult.error || 'refresh returned success=false'),
        });

        if (refreshResult.success) refreshed++;
        else failed++;
      } catch (refreshErr) {
        results.push({
          uat,
          daysLeft,
          success: false,
          newExpiresAt: null,
          error: refreshErr.message,
        });
        failed++;
      }
    }

    return { results, stats: { refreshed, failed, total: expiringUATs.length } };
  }
}

module.exports = UatRefreshWorker;
