// graph-capability-kernel/substrates/vault/uat-substrate/workers/refresh-worker.js
// UAT refresh worker: fb_exchange_token + /debug_token validate + store.
// Migrated from substrates/vault/uat-substrate/workers/refresh-worker.js
//
// This worker orchestrates a sequence of bounded calls (exchange → detect → store) because
// they share a single business operation (refresh a UAT). The substrate façade remains
// mutation-plane (state, signals) while this worker is executor-plane (the actual I/O sequence).

const { axios, GRAPH_API_BASE } = require('../../api-surface');
const { clearCredentialCache } = require('../../../../../helpers/credential-cache');
const StoreWorker = require('./store-worker');
const DetectWorker = require('./detect-worker');

class RefreshWorker {
  /**
   * @param {{ userId: string, businessAccountId: string }} input
   * @returns {Promise<{ success: boolean, expiresAt: string|null, scopes: string[], error?: string }>}
   */
  async execute({ userId, businessAccountId }) {
    // Step 1: retrieve current UAT
    const RetrieveWorker = require('./retrieve-worker');
    const retrieveWorker = new RetrieveWorker();
    const current = await retrieveWorker.execute({ userId, businessAccountId });

    // Step 2: fb_exchange_token
    const extendRes = await axios.get(`${GRAPH_API_BASE}/oauth/access_token`, {
      params: {
        grant_type: 'fb_exchange_token',
        client_id: process.env.INSTAGRAM_APP_ID,
        client_secret: process.env.INSTAGRAM_APP_SECRET,
        fb_exchange_token: current.token,
      },
      timeout: 10000,
    });

    const newToken = extendRes.data.access_token;
    const expiresIn = extendRes.data.expires_in;

    // Step 3: /debug_token validate
    const detectWorker = new DetectWorker();
    const tokenInfo = await detectWorker.execute({ token: newToken });
    if (!tokenInfo || !tokenInfo.isValid) throw new Error('Refreshed UAT failed /debug_token validation');

    const newExpiresAt = expiresIn ? new Date(Date.now() + expiresIn * 1000).toISOString() : null;
    const dataAccessExpiresAt = tokenInfo.dataAccessExpiresAt
      ? new Date(tokenInfo.dataAccessExpiresAt * 1000).toISOString()
      : null;

    // Step 4: store the new UAT
    const storeWorker = new StoreWorker();
    const storeResult = await storeWorker.execute({
      userId,
      businessAccountId,
      userAccessToken: newToken,
      scope: tokenInfo.scopes,
      expiresAt: newExpiresAt,
      dataAccessExpiresAt,
    });
    if (!storeResult.success) throw new Error(`Failed to store refreshed UAT: ${storeResult.error}`);

    clearCredentialCache(businessAccountId);

    return { success: true, expiresAt: newExpiresAt, scopes: tokenInfo.scopes };
  }
}

module.exports = RefreshWorker;