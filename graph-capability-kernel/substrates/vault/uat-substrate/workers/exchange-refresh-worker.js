// graph-capability-kernel/substrates/vault/uat-substrate/workers/exchange-refresh-worker.js
// Exchange-refresh worker: one bounded fb_exchange_token HTTP call.
// Split from refresh-worker.js — the façade now orchestrates the full refresh pipeline.
//
// Owns: ONE bounded HTTP call to Meta's oauth/access_token endpoint.
// Does NOT own: retrieve, detect, store, cache invalidation, signal dispatch.

const { axios, GRAPH_API_BASE } = require('../../../api-surface');

class ExchangeRefreshWorker {
  /**
   * @param {{ token: string }} input — the current UAT to refresh
   * @returns {Promise<{ success: boolean, accessToken?: string, expiresIn?: number, error?: string }>}
   */
  async execute({ token }) {
    try {
      const extendRes = await axios.get(`${GRAPH_API_BASE}/oauth/access_token`, {
        params: {
          grant_type: 'fb_exchange_token',
          client_id: process.env.INSTAGRAM_APP_ID,
          client_secret: process.env.INSTAGRAM_APP_SECRET,
          fb_exchange_token: token,
        },
        timeout: 10000,
      });

      return {
        success: true,
        accessToken: extendRes.data.access_token,
        expiresIn: extendRes.data.expires_in,
      };
    } catch (error) {
      if (error.response) {
        const apiError = error.response.data?.error;
        return { success: false, error: apiError?.message || 'fb_exchange_token failed' };
      }
      return { success: false, error: error.message || 'fb_exchange_token failed' };
    }
  }
}

module.exports = ExchangeRefreshWorker;
