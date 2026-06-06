// substrates/parsing/workers/insights-worker.js
// Insights parsing worker: parse → normalize → CK(DB_WRITE_REQUESTED).
//
// Owns: sequencing the insights pipeline for media insights data.
// Does NOT own: normalization logic (insights normalizer), Supabase,
//               governance policy. Hashtag enrichment deferred to Phase 6.
//
// Phase 4: canonical path — uses domain substrate tools, no inline normalization.

const { normalizeMediaInsight } = require('../../insights-substrate/normalizer');

async function execute(rawData, accountId, intentId, extra = {}, governance) {
  if (!rawData.insights || rawData.insights.length === 0) return { count: 0 };

  const rows = rawData.insights
    .filter(item => item && item.media_id)
    .map(item => normalizeMediaInsight(item, accountId));

  if (!rows.length) return { count: 0 };

  if (governance) {
    governance.dispatch({
      type: 'DB_WRITE_REQUESTED',
      domain: 'insights',
      accountId, intentId,
      table: 'instagram_media',
      operation: 'batch_upsert_insights',
      rows,
    });
  }

  return { count: rows.length };
}

module.exports = { execute };
