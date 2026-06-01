// substrates/insights/index.js
// Insights substrate: full acquisition pipeline for account + media insights.
//
// Owns: fetch → parse → normalize → persist for insights domain.
// Does NOT own: retry decisions, governance, orchestration.

const transport = require('./transport');
const parser = require('./parser');
const normalizer = require('./normalizer');
const persistence = require('../persistence');

/**
 * Execute a full acquisition cycle for the insights domain.
 *
 * @param {string} accountId
 * @param {string} domain — 'insights'
 * @param {object} params — intent payload { since, until, hasWebsite }
 * @param {object} credentials — pre-resolved { igUserId, pageToken }
 * @returns {Promise<{status: string, count: number, error: string|null, _usagePct: number|null}>}
 */
async function acquire(accountId, domain, params, credentials) {
  const sevenDaysAgo = params.since || Math.floor((Date.now() - 7 * 24 * 3600000) / 1000);
  const now = params.until || Math.floor(Date.now() / 1000);

  // Step 1: fetch account insights
  const insightsRaw = await transport.fetchAccountInsights(accountId, {
    since: sevenDaysAgo, until: now, hasWebsite: params.hasWebsite || false,
  }, credentials);

  if (!insightsRaw.success) {
    return { status: 'failed', count: 0, error: insightsRaw.error, _usagePct: insightsRaw._usagePct || null };
  }

  // Step 2: fetch media feed
  const feedRaw = await transport.fetchPosts(accountId, 50, credentials, {
    since: sevenDaysAgo, until: now,
  });

  if (!feedRaw.success) {
    return { status: 'failed', count: 0, error: feedRaw.error, _usagePct: feedRaw._usagePct || null };
  }

  const mediaList = feedRaw.posts || [];
  if (mediaList.length === 0) {
    return { status: 'completed', count: 0, error: null, _usagePct: feedRaw._usagePct };
  }

  // Step 3: fetch per-media insights
  const mediaInsights = await transport.fetchMediaInsightsBatch(mediaList, credentials.pageToken);

  // Step 4: parse + normalize
  const parsed = parser.parseMediaInsights(mediaInsights);
  if (parsed.length === 0) {
    return { status: 'completed', count: 0, error: null, _usagePct: feedRaw._usagePct };
  }

  const records = parsed.map(m => normalizer.normalizeMediaInsight(m, accountId));

  // Step 5: persist
  const captions = (feedRaw.posts || []).map(p => p.caption).filter(Boolean);
  const stored = await persistence.storeMediaInsightsBatch(accountId, records, captions);

  return { status: 'completed', count: stored.count, error: null, _usagePct: feedRaw._usagePct };
}

module.exports = { acquire };
