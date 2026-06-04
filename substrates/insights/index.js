// substrates/insights/index.js
// Insights substrate: factory-creates worker → bounded IG API read.
//
// Owns: worker factory + transport bridge. Pure delegation plane.
// Does NOT own: retry, error classification, orchestration, credential resolution.
//
// Worker: InsightsWorker — 2-step: media feed → insights batch.
// Persist: routes to persistence substrate (called by parsing workers asynchronously).

const InsightsWorker = require('./workers/insights');
const persistence = require('../persistence');

/**
 * Fetch raw data from Instagram API for insights domain.
 * Factory-creates an InsightsWorker and delegates the bounded call.
 *
 * @param {string} accountId
 * @param {object} params — { since?, until? }
 * @param {object} credentials — pre-resolved
 * @returns {Promise<object>} raw transport response
 */
async function fetch(accountId, params, credentials) {
  const worker = new InsightsWorker();
  return worker.execute(accountId, params, credentials);
}

/**
 * Persist raw insights data to Supabase. Normalizes internally.
 * Called by parsing workers asynchronously.
 */
async function persist(accountId, rawData) {
  if (!rawData.insights || rawData.insights.length === 0) return { count: 0 };
  const captions = (rawData.mediaList || []).map(m => m.caption).filter(Boolean);
  return persistence.storeMediaInsightsBatch(accountId, rawData.insights, captions);
}

module.exports = { fetch, persist };
