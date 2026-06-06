// substrates/ugc-content-substrate/fetch-workers/ugc-fetcher.js
// UGC Fetcher: one bounded IG API read call per instantiation.
// Handles both tagged media and hashtag search.
//
// Owns: ONE HTTP GET to IG Graph API (tagged media or hashtag media).
// Does NOT own: retry, credentials, error classification, state, normalization.
//
// Contract: execute() → { success, records, count, paging?, _usagePct? }
// Stateless. Created by ugc-content-substrate (factory), destroyed after result.

const transport = require('../ugc-transport');

module.exports = class UgcFetcher {
  /**
   * Execute one bounded IG Graph API read call.
   * Dispatches to tagged media or hashtag media based on params.
   * @param {string} accountId
   * @param {object} params — { hashtag?: string, limit?: number }
   * @param {object} credentials — pre-resolved { igUserId, pageToken }
   * @returns {Promise<object>} raw transport response
   */
  async execute(accountId, params, credentials) {
    if (params?.hashtag) {
      const limit = params?.limit || 25;
      return transport.fetchHashtagMedia(accountId, params.hashtag, limit, credentials);
    }
    // Tagged media (default)
    const limit = params?.limit || 25;
    return transport.fetchTaggedMedia(accountId, limit, credentials);
  }
};

