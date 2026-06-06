// substrates/insights/index.js
// Insights substrate: factory-creates worker → bounded IG API read.
//
// Owns: worker factory + transport bridge + credential resolution
//        (Step 7 normalisation).
// Does NOT own: retry, error classification, orchestration.
//
// Worker: InsightsWorker — 2-step: media feed → insights batch.
// Persist: routes to persistence substrate (called by parsing workers asynchronously).

const InsightsWorker = require('./workers/insights');
const { normalizeMediaInsight } = require('./normalizer');
const { syncHashtagsFromCaptions } = require('../content/hashtag-sync');
const { getSupabaseAdmin } = require('../../../config/supabase');
const { resolveAccountCredentials } =
  require('../../../graph-capability-kernel/substrates/credential-resolver');

/**
 * Fetch raw data from Instagram API for insights domain.
 * Factory-creates an InsightsWorker and delegates the bounded call.
 *
 * Step 7: substrate resolves credentials internally.
 *
 * @param {string} accountId
 * @param {object} params — { since?, until? }
 * @returns {Promise<object>} raw transport response
 */
async function fetch(accountId, params) {
  const credentials = await resolveAccountCredentials(accountId);
  const worker = new InsightsWorker();
  return worker.execute(accountId, params, credentials);
}

/**
 * Persist insights data to Supabase.
 * Constitutional path: normalize → CK(DB_WRITE_REQUESTED) → writer → hashtag sync.
 * Uses canonical normalizer — no inline field mapping.
 */
async function persist(accountId, rawData, extra = {}) {
  const governance = extra._governance;

  if (!rawData.insights || rawData.insights.length === 0) return { count: 0 };

  const rows = rawData.insights
    .filter(item => item && item.media_id)
    .map(item => normalizeMediaInsight(item, accountId));

  if (rows.length === 0) return { count: 0 };

  governance?.dispatch({
    type: 'DB_WRITE_REQUESTED',
    domain: 'insights', accountId, intentId: null,
    table: 'instagram_media',
    operation: 'batch_upsert_insights',
    rows,
  });

  // Side-effect: extract hashtags from captions into ugc_monitored_hashtags
  const captions = rawData.insights.map(p => p.caption).filter(Boolean);
  if (captions.length > 0) {
    const supabase = getSupabaseAdmin();
    if (supabase) syncHashtagsFromCaptions(supabase, accountId, captions).catch(() => {});
  }

  return { count: rows.length };
}

module.exports = { fetch, persist };
