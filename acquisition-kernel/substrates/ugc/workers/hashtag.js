// substrates/ugc/workers/hashtag.js
// UGC HashtagWorker: one bounded IG API read call per instantiation.
//
// Owns: ONE 2-step HTTP call to IG Graph API (hashtag ID lookup → recent media).
// Does NOT own: retry, credentials, error classification, state, normalization.
//
// Contract: execute() → { success, rawMedia, hashtagId, cleanHashtag, count, _usagePct? }
// Stateless. Created by ugc substrate (factory), destroyed after result.

const transport = require('../transport');

module.exports = class HashtagWorker {
  /**
   * Execute one bounded IG Graph API read call for hashtag media.
   * @param {string} accountId
   * @param {object} params — { hashtag: string, limit?: number }
   * @param {object} credentials — pre-resolved { igUserId, pageToken }
   * @returns {Promise<object>} raw transport response
   */
  async execute(accountId, params, credentials) {
    const hashtag = params?.hashtag;
    const limit = params?.limit || 25;
    return transport.fetchHashtagMedia(accountId, hashtag, limit, credentials);
  }
};
