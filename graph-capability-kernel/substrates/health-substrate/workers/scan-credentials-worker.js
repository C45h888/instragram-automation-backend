// graph-capability-kernel/substrates/health-substrate/workers/scan-credentials-worker.js
// Token health scan worker — detect + classify, no side effects.
//
// Owns: ONE bounded iteration over active page credentials + a /debug_token call per cred.
// Does NOT own: recovery, alert writes, state mutation, rate-limit pacing.
//
// Migration origin: services/sync/token-health.js → runTokenHealthCheck() inner loop (lines 66-209).
//   Legacy mixed: scan + classify + recovery + alert writes + audit in one 145-line for-loop.
//   This worker is the pure scan/classify half. Side effects live in recovery-worker + the
//   orchestrator (index.js runTokenHealthCheck).
//
// Constitutional wiring:
//   On valid classification → substrate façade emits CAPABILITY_EVALUATE (per cred, via signal-dispatch).
//   On invalid → recovery-worker is invoked by the orchestrator (NOT by this worker).

const { getSupabaseAdmin } = require('../../../../config/supabase');
const vault = require('../../vault');

class ScanCredentialsWorker {
  /**
   * Scan all active page credentials. For each, skip if checked <24h ago, else call
   * /debug_token via vault.uat.detect. Return a per-credential classification.
   *
   * @param {{ scanWindowHours?: number }} [opts]
   * @returns {Promise<{
   *   scanned: Array<{
   *     cred: object,
   *     tokenInfo: object|null,
   *     classification: 'VALID' | 'INVALID' | 'SKIPPED_FRESH' | 'SKIPPED_RETRIEVE_FAILED' | 'SKIPPED_API_ERROR',
   *     skipReason: string|null,
   *   }>,
   *   stats: { valid: number, invalid: number, skipped: number, total: number },
   * }>}
   */
  async execute({ scanWindowHours = 24 } = {}) {
    const supabase = getSupabaseAdmin();
    if (!supabase) {
      return { scanned: [], stats: { valid: 0, invalid: 0, skipped: 0, total: 0 } };
    }

    const { data: credentials, error } = await supabase
      .from('instagram_credentials')
      .select('id, user_id, business_account_id, debug_token_checked_at, issued_at')
      .eq('token_type', 'page')
      .eq('is_active', true);

    if (error) throw error;
    if (!credentials?.length) {
      return { scanned: [], stats: { valid: 0, invalid: 0, skipped: 0, total: 0 } };
    }

    const scanned = [];
    let valid = 0, invalid = 0, skipped = 0;

    for (const cred of credentials) {
      // Skip if checked within the last N hours (default 24h)
      if (cred.debug_token_checked_at) {
        const hoursSince = (Date.now() - new Date(cred.debug_token_checked_at).getTime()) / 3_600_000;
        if (hoursSince < scanWindowHours) {
          scanned.push({
            cred,
            tokenInfo: null,
            classification: 'SKIPPED_FRESH',
            skipReason: `checked ${hoursSince.toFixed(1)}h ago`,
          });
          skipped++;
          continue;
        }
      }

      // Retrieve decrypted token via vault substrate
      let token;
      try {
        token = await vault.pat.retrieve({ userId: cred.user_id, businessAccountId: cred.business_account_id });
      } catch (retrieveErr) {
        scanned.push({
          cred,
          tokenInfo: null,
          classification: 'SKIPPED_RETRIEVE_FAILED',
          skipReason: retrieveErr.message,
        });
        skipped++;
        continue;
      }

      // Call /debug_token via vault.uat.detect
      let tokenInfo;
      try {
        tokenInfo = await vault.uat.detect({ token });
      } catch (apiErr) {
        scanned.push({
          cred,
          tokenInfo: null,
          classification: 'SKIPPED_API_ERROR',
          skipReason: apiErr.message,
        });
        skipped++;
        continue;
      }

      if (!tokenInfo || !tokenInfo.isValid) {
        scanned.push({
          cred,
          tokenInfo: tokenInfo || null,
          classification: 'INVALID',
          skipReason: tokenInfo ? '/debug_token returned isValid=false' : '/debug_token returned null',
        });
        invalid++;
      } else {
        scanned.push({
          cred,
          tokenInfo,
          classification: 'VALID',
          skipReason: null,
        });
        valid++;
      }
    }

    return { scanned, stats: { valid, invalid, skipped, total: credentials.length } };
  }
}

module.exports = ScanCredentialsWorker;
