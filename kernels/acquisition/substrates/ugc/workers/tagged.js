// substrates/ugc/workers/tagged.js
// UGC TaggedWorker: one bounded IG API read call per instantiation.
//
// Owns: ONE HTTP GET to IG Graph API (tagged media for business account).
// Does NOT own: retry, credentials, error classification, state, normalization.
//
// Contract: execute() → { success, records, count, paging?, _usagePct? }
// Stateless. Created by ugc substrate (factory), destroyed after result.

const transport = require('../transport');

module.exports = class TaggedWorker {
  /**
   * Execute one bounded IG Graph API read call for tagged media.
   * @param {string} accountId
   * @param {object} params — { limit?: number }
   * @param {object} credentials — pre-resolved { igUserId, pageToken }
   * @returns {Promise<object>} raw transport response
   */
  async execute(accountId, params, credentials) {
    const limit = params?.limit || 25;
    return transport.fetchTaggedMedia(accountId, limit, credentials);
  }
};
