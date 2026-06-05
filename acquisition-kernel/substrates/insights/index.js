// substrates/insights/index.js
// Insights substrate: factory-creates worker → bounded IG API read.
//
// Owns: worker factory + transport bridge. Pure delegation plane.
// Does NOT own: retry, error classification, orchestration, credential resolution.
//
// Worker: InsightsWorker — 2-step: media feed → insights batch.
// Persist: routes to persistence substrate (called by parsing workers asynchronously).

const InsightsWorker = require('./workers/insights');
const dispatchWrite = require('../../../postgres-telemetry-kernel/writers').dispatchWrite;

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
 * Persist insights data to Supabase.
 * Routes through CK dispatch path: DB_WRITE_REQUESTED → persist-telemetry-fsm → db/writer.
 * Note: syncHashtagsFromCaptions side-effect deferred to Phase 4 enrichment membrane.
 */
async function persist(accountId, rawData) {
  if (!rawData.insights || rawData.insights.length === 0) return { count: 0 };
  const rows = rawData.insights
    .filter(item => item && item.media_id)
    .map(item => {
      const isStory = item.media_type === 'STORY';
      return {
        instagram_media_id: item.media_id,
        business_account_id: accountId,
        media_type: item.media_type || null,
        caption: item.caption || null,
        media_url: item.media_url || null,
        thumbnail_url: item.thumbnail_url || null,
        permalink: item.permalink || null,
        like_count: item.like_count || 0,
        comments_count: item.comments_count || 0,
        reach: item.insights.find(i => i.name === 'reach')?.values?.[0]?.value || 0,
        impressions: item.insights.find(i => i.name === 'impressions')?.values?.[0]?.value || 0,
        saves: isStory ? null : (item.insights.find(i => i.name === 'saved')?.values?.[0]?.value ?? 0),
        published_at: item.timestamp || null,
      };
    });
  if (rows.length === 0) return { count: 0 };
  dispatchWrite('batch_upsert_insights', {
    domain: 'insights', accountId, intentId: null, table: 'instagram_media',
    rows,
  });
  return { count: rows.length };
}

module.exports = { fetch, persist };
