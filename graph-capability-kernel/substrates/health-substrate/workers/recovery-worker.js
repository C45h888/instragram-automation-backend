// graph-capability-kernel/substrates/health-substrate/workers/recovery-worker.js
// Token recovery worker — UAT→PAT silent recovery side effect.
//
// Owns: ONE bounded recovery attempt: vault.uat.retrieve → vault.pat.exchange →
//       vault.scope.detectDynamic → vault.pat.store → clearCredentialCache.
// Does NOT own: scan (scan-credentials-worker), alert writes, audit logging,
//               rate-limit pacing, iteration over multiple credentials.
//
// Migration origin: services/sync/token-health.js → runTokenHealthCheck() recovery
//   branch (lines 91-145). Legacy mixed recovery + alert insert + audit in one block.
//   This worker is the side-effect half. Alert writes stay with the orchestrator.
//
// Why this is split out:
//   Health worker = detect + classify (pure)
//   Recovery worker = side effect (only invoked on INVALID classification)
//   Keeps both testable in isolation, and prevents the health worker from
//   becoming fat on alert + recovery branching.

const vault = require('../../vault');
const { clearCredentialCache } = require('../../../../helpers/credential-cache');

class RecoveryWorker {
  /**
   * Attempt silent PAT recovery via stored UAT for a single invalid credential.
   *
   * @param {{
   *   cred: { id: string, user_id: string, business_account_id: string },
   *   triggerBridge?: object,
   * }} input
   * @returns {Promise<{
   *   success: boolean,
   *   error: string|null,
   *   newIgBusinessAccountId: string|null,
   *   newPageId: string|null,
   *   newPageName: string|null,
   * }>}
   */
  async execute({ cred }) {
    if (!cred || !cred.user_id || !cred.business_account_id) {
      return { success: false, error: 'cred with user_id + business_account_id is required', newIgBusinessAccountId: null, newPageId: null, newPageName: null };
    }

    try {
      const uatData = await vault.uat.retrieve({ userId: cred.user_id, businessAccountId: cred.business_account_id });
      const exchangeResult = await vault.pat.exchange({ userAccessToken: uatData.token });

      if (!exchangeResult.success || exchangeResult.requiresSelection) {
        return {
          success: false,
          error: exchangeResult.requiresSelection ? 'exchange returned requiresSelection' : (exchangeResult.error || 'exchange failed'),
          newIgBusinessAccountId: null,
          newPageId: null,
          newPageName: null,
        };
      }

      const newScope = await vault.scope.detectDynamic({
        token: exchangeResult.pageAccessToken,
        credentialId: cred.id,
      });

      await vault.pat.store({
        userId: cred.user_id,
        igBusinessAccountId: exchangeResult.igBusinessAccountId,
        pageAccessToken: exchangeResult.pageAccessToken,
        pageId: exchangeResult.pageId,
        pageName: exchangeResult.pageName,
        scope: newScope,
      });

      clearCredentialCache(cred.business_account_id);

      return {
        success: true,
        error: null,
        newIgBusinessAccountId: exchangeResult.igBusinessAccountId,
        newPageId: exchangeResult.pageId,
        newPageName: exchangeResult.pageName,
      };
    } catch (err) {
      return {
        success: false,
        error: err.message,
        newIgBusinessAccountId: null,
        newPageId: null,
        newPageName: null,
      };
    }
  }
}

module.exports = RecoveryWorker;
