// substrates/content/workers/posts.js
// Content PostsWorker: one bounded IG API read call per instantiation.
//
// Owns: ONE HTTP GET to IG Graph API via transport.
// Does NOT own: retry, credentials, error classification, state, persistence.
//
// Contract: execute() → { success, posts, count, igUserId, pageToken?, _usagePct? }
// Stateless. Created by content substrate (factory), destroyed after result.

const transport = require('../transport');

module.exports = class PostsWorker {
  /**
   * Execute one bounded IG Graph API read call.
   * @param {string} accountId
   * @param {object} params — { limit?, since?, until? }
   * @param {object} credentials — pre-resolved { igUserId, pageToken, userId }
   * @returns {Promise<object>} raw transport response
   */
  async execute(accountId, params, credentials) {
    const limit = params?.limit || 50;
    const timeWindow = (params?.since || params?.until)
      ? { since: params.since, until: params.until }
      : null;
    return transport.fetchPosts(accountId, limit, credentials, timeWindow);
  }
};
