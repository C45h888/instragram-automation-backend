// graph-capability-kernel/substrates/health-substrate/workers/data-access-expiry-worker.js
// data_access_expires_at scan worker — detect + classify (no side effects).
//
// Owns: ONE bounded iteration over UATs whose data_access_expires_at is within 30 days,
//       with dedup-pre-check against existing unresolved system_alerts.
// Does NOT own: alert writes, event writes, rate-limit pacing.
//
// Migration origin: services/sync/token-health.js → runUATRefreshCheck() lines 324-374
//   (the 30-day data_access_expires_at scan + dedup block).
//   Legacy: query → loop → existing alert check → fireAndForgetInsert alert + event row.
//   This worker: query → loop → existing alert check → return candidates.
//   Alert writes stay with the orchestrator (per "steady migration" directive — dedup
//   pattern is the explicit deferred item from the report §7a).
//
// Why dedup-pre-check stays in the worker (not the orchestrator):
//   It's a READ against the same table the alert will be written to. The orchestrator
//   would have to do this read anyway to decide whether to insert. Doing it here
//   means the worker returns "ready to alert" candidates only, not raw scan output.
//   The orchestrator's job becomes "write alerts for these" instead of "read, decide, write".

const { getSupabaseAdmin } = require('../../../../config/supabase');

class DataAccessExpiryWorker {
  /**
   * Find UATs whose data_access_expires_at is within 30 days, dedup against
   * existing unresolved data_access_expiry_warning alerts.
   *
   * @param {{ windowDays?: number, businessAccountId?: string|null }} [opts]
   *   businessAccountId — optional filter for per-cred targeted check.
   * @returns {Promise<{
   *   candidates: Array<{
   *     uat: { id: string, user_id: string, business_account_id: string, data_access_expires_at: string },
   *     daysLeft: number,
   *   }>,
   *   stats: { alertable: number, deduped: number, total: number },
   * }>}
   */
  async execute({ windowDays = 30, businessAccountId = null } = {}) {
    const supabase = getSupabaseAdmin();
    if (!supabase) {
      return { candidates: [], stats: { alertable: 0, deduped: 0, total: 0 } };
    }

    const cutoffIso = new Date(Date.now() + windowDays * 24 * 60 * 60 * 1000).toISOString();

    let query = supabase
      .from('instagram_credentials')
      .select('id, user_id, business_account_id, data_access_expires_at')
      .eq('token_type', 'user')
      .eq('is_active', true)
      .not('data_access_expires_at', 'is', null)
      .lt('data_access_expires_at', cutoffIso);
    if (businessAccountId) {
      query = query.eq('business_account_id', businessAccountId);
    }
    const { data: expiring, error } = await query;

    if (error) throw error;
    if (!expiring?.length) {
      return { candidates: [], stats: { alertable: 0, deduped: 0, total: 0 } };
    }

    const candidates = [];
    let deduped = 0;

    for (const uat of expiring) {
      const daysLeft = Math.ceil((new Date(uat.data_access_expires_at) - Date.now()) / (24 * 60 * 60 * 1000));

      // Dedup: skip if an unresolved data_access_expiry_warning already exists
      const { data: existing } = await supabase
        .from('system_alerts')
        .select('id')
        .eq('business_account_id', uat.business_account_id)
        .eq('alert_type', 'data_access_expiry_warning')
        .eq('resolved', false)
        .maybeSingle();

      if (existing) {
        deduped++;
        continue;
      }

      candidates.push({ uat, daysLeft });
    }

    return {
      candidates,
      stats: { alertable: candidates.length, deduped, total: expiring.length },
    };
  }
}

module.exports = DataAccessExpiryWorker;
