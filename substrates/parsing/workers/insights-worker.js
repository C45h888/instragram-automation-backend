// substrates/parsing/workers/insights-worker.js
// Insights parsing worker: parse → normalize → persist for insights domain.
//
// Owns: transforming raw insights data into Supabase rows.
// Does NOT own: fetch, transport, orchestration, governance.

const persistence = require('../../persistence');

/**
 * Execute the insights parsing pipeline.
 *
 * @param {object} rawData — raw transport response { insights, mediaList }
 * @param {string} accountId
 * @param {object} [extra] — unused
 * @returns {Promise<{count: number, error?: string}>}
 */
async function execute(rawData, accountId, extra = {}) {
  if (!rawData.insights || rawData.insights.length === 0) return { count: 0 };

  const captions = (rawData.mediaList || []).map(m => m.caption).filter(Boolean);
  const result = await persistence.storeMediaInsightsBatch(accountId, rawData.insights, captions);
  return { count: result.count || 0 };
}

module.exports = { execute };
