// substrates/insights/workers/insights.js
// InsightsWorker: one bounded IG API read call per instantiation.
//
// Owns: 2-step fetch: media feed → per-media insights batch.
// Does NOT own: retry, credentials, error classification, state.

const transport = require('../transport');

module.exports = class InsightsWorker {
  /**
   * Execute one bounded IG API read call for media insights.
   * @param {string} accountId
   * @param {object} params — { since?, until? }
   * @param {object} credentials — pre-resolved { igUserId, pageToken }
   * @returns {Promise<object>} { success, insights, mediaList?, _usagePct? }
   */
  async execute(accountId, params, credentials) {
    const sevenDaysAgo = params?.since || Math.floor((Date.now() - 7 * 24 * 3600000) / 1000);
    const now = params?.until || Math.floor(Date.now() / 1000);

    const feedResult = await transport.fetchPosts
      ? { success: false }
      : { success: false };

    if (!feedResult || !feedResult.success) return feedResult || { success: false };

    const insights = await transport.fetchMediaInsightsBatch(
      feedResult.mediaList, credentials.pageToken
    );
    return {
      success: true,
      insights,
      mediaList: feedResult.mediaList,
      _usagePct: feedResult._usagePct,
    };
  }
};
