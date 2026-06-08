// graph-capability-kernel/substrates/vault/pat-substrate/workers/exchange-worker.js
// PAT exchange worker: one bounded /me/accounts Graph call + IG business account discovery.
// Migrated from substrates/vault/pat-substrate/workers/exchange-worker.js
//
// Owns: ONE bounded HTTP call.
// Does NOT own: factory, pre-flight, orchestration, state, retry.

const { axios, GRAPH_API_BASE } = require('../../../../api-surface');

class ExchangeWorker {
  /**
   * @param {{ userAccessToken: string }} input
   * @returns {Promise<{ success: boolean, requiresSelection?: boolean, pageAccessToken?: string, pageId?: string, pageName?: string, igBusinessAccountId?: string, pages?: Array, error?: string, errorCode?: string }>}
   */
  async execute({ userAccessToken }) {
    try {
      const pagesResponse = await axios.get(`${GRAPH_API_BASE}/me/accounts`, {
        params: { fields: 'id,name,access_token,instagram_business_account', access_token: userAccessToken },
        timeout: 10000,
      });

      const pages = pagesResponse.data.data;
      if (!pages || pages.length === 0) {
        return { success: false, error: 'No Facebook pages found. Please ensure you have a Facebook Page connected to your account.' };
      }

      const pagesWithIG = pages.filter(p => p.instagram_business_account?.id);
      if (pagesWithIG.length === 0) {
        return { success: false, error: 'No Instagram Business Account connected.', errorCode: 'NO_IG_BUSINESS_ACCOUNT' };
      }

      if (pagesWithIG.length === 1) {
        const page = pagesWithIG[0];
        return {
          success: true,
          requiresSelection: false,
          pageAccessToken: page.access_token,
          pageId: page.id,
          pageName: page.name,
          igBusinessAccountId: page.instagram_business_account.id,
          tokenType: 'page',
        };
      }

      return {
        success: true,
        requiresSelection: true,
        pages: pagesWithIG.map(page => ({
          pageId: page.id,
          pageName: page.name,
          pageAccessToken: page.access_token,
          igBusinessAccountId: page.instagram_business_account.id,
        })),
      };
    } catch (error) {
      if (error.response) {
        const apiError = error.response.data?.error;
        if (apiError?.code === 190) return { success: false, error: 'Invalid or expired user access token.' };
        if (apiError?.code === 100) return { success: false, error: 'Invalid API request. Check permissions.' };
        if (apiError?.message) return { success: false, error: `Facebook API Error: ${apiError.message}` };
      }
      if (error.code === 'ECONNREFUSED' || error.code === 'ETIMEDOUT') {
        return { success: false, error: 'Unable to connect to Facebook Graph API.' };
      }
      return { success: false, error: error.message || 'Page token exchange failed' };
    }
  }
}

module.exports = ExchangeWorker;