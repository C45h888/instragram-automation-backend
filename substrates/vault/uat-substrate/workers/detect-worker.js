// substrates/vault/uat-substrate/workers/detect-worker.js
// UAT detect worker: one bounded /debug_token call.
// Migrated from services/tokens/detection.js: detectTokenType.

const { axios, GRAPH_API_BASE } = require('../../api-surface');

class DetectWorker {
  /**
   * @param {{ token: string }} input
   * @returns {Promise<{ isValid: boolean, type: string|null, appId: string|null, scopes: string[], expiresAt: number|null, issuedAt: number|null, userId: string|null, dataAccessExpiresAt: number|null }|null>}
   */
  async execute({ token }) {
    try {
      const response = await axios.get(`${GRAPH_API_BASE}/debug_token`, {
        params: { input_token: token, access_token: token },
        timeout: 5000,
      });
      const data = response.data.data;
      return {
        isValid: data.is_valid,
        type: data.type,
        appId: data.app_id,
        scopes: data.scopes || [],
        expiresAt: data.expires_at,
        issuedAt: data.issued_at,
        userId: data.user_id,
        dataAccessExpiresAt: data.data_access_expires_at || null,
      };
    } catch (err) {
      console.warn('⚠️ Token type detection failed:', err.message);
      return null;
    }
  }
}

module.exports = DetectWorker;
