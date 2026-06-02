// substrates/insights/index.js
// Insights substrate: full pipeline for account insights and media insights.
//
// Owns: fetch → parse → normalize → persist for insights domain.
// Does NOT own: retry logic, error classification, orchestration, credential resolution.

const transport = require('./transport');
const persistence = require('../persistence');

/**
 * Fetch raw data from Instagram API for insights domain.
 * Two-step: media feed → insights batch.
 */
async function fetch(accountId, params, credentials) {
  const sevenDaysAgo = params.since || Math.floor((Date.now() - 7 * 24 * 3600000) / 1000);
  const now = params.until || Math.floor(Date.now() / 1000);
  const feedResult = await transport.fetchMediaInsightsBatch ?
    null : transport.fetchMediaFeed(accountId, sevenDaysAgo, now, credentials);
  if (!feedResult || !feedResult.success) return feedResult || { success: false };
  const insights = await transport.fetchMediaInsightsBatch(feedResult.mediaList, credentials.pageToken);
  return { success: true, insights, mediaList: feedResult.mediaList, _usagePct: feedResult._usagePct };
}

/**
 * Persist raw insights data to Supabase. Normalizes internally.
 */
async function persist(accountId, rawData) {
  if (!rawData.insights || rawData.insights.length === 0) return { count: 0 };
  const captions = (rawData.mediaList || []).map(m => m.caption).filter(Boolean);
  return persistence.storeMediaInsightsBatch(accountId, rawData.insights, captions);
}

module.exports = { fetch, persist };
