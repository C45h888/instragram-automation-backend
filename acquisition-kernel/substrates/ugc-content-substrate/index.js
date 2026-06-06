// substrates/ugc-content-substrate/index.js
// Merged UGC + Content substrate: factory-creates workers for both domains.
//
// Owns: worker factories for content and UGC domains + transport bridges
//        + credential resolution (Step 7 normalisation).
// Does NOT own: retry, error classification, orchestration.
//
// Workers: ContentFetcher (posts), UgcFetcher (tagged + hashtag media).
// Persist: routes to persistence substrate (called by parsing workers asynchronously).

const ContentFetcher = require('./fetch-workers/content-fetcher');
const UgcFetcher = require('./fetch-workers/ugc-fetcher');
const { normalizeBusinessPost } = require('./content-normalizer');
const { mapRawPostToUgcContent } = require('./ugc-normalizer');
const { syncHashtagsFromCaptions } = require('./hashtag-sync');
const { getSupabaseAdmin } = require('../../../config/supabase');
const { resolveAccountCredentials } =
  require('../../../graph-capability-kernel/substrates/credential-resolver');

/**
 * Fetch raw data from Instagram API for content or UGC domain.
 * Disambiguates: params.hashtag → UGC (hashtag search),
 *                params.since/until → content (posts with time window),
 *                else → UGC (tagged media by default).
 *
 * @param {string} accountId
 * @param {object} params — { hashtag?, limit?, since?, until?, maxPosts? }
 * @returns {Promise<object>} raw transport response
 */
async function fetch(accountId, params) {
  const credentials = await resolveAccountCredentials(accountId);

  // UGC path — hashtag search or tagged media
  if (params?.hashtag || (!params?.since && !params?.until)) {
    const worker = new UgcFetcher();
    return worker.execute(accountId, params, credentials);
  }

  // Content path — posts with optional time window
  const worker = new ContentFetcher();
  return worker.execute(accountId, params, credentials);
}

/**
 * Persist business post data to Supabase (content domain).
 */
async function persistContent(accountId, rawData, extra = {}) {
  const governance = extra._governance;
  if (!rawData.posts || rawData.posts.length === 0) return { count: 0 };
  const rows = rawData.posts
    .filter(p => p && p.id)
    .map(p => normalizeBusinessPost(p, accountId));
  if (rows.length === 0) return { count: 0 };
  governance?.dispatch({
    type: 'DB_WRITE_REQUESTED', domain: 'media', accountId, intentId: null,
    table: 'instagram_media', operation: 'batch_upsert_posts', rows,
  });
  const captions = rawData.posts.map(p => p.caption).filter(Boolean);
  if (captions.length > 0) {
    const supabase = getSupabaseAdmin();
    if (supabase) syncHashtagsFromCaptions(supabase, accountId, captions).catch(() => {});
  }
  return { count: rows.length };
}

/**
 * Persist UGC data to Supabase (UGC domain).
 */
async function persistUgc(accountId, rawData, extra = {}) {
  const governance = extra._governance;
  if (!rawData.records || rawData.records.length === 0) return { count: 0 };
  const source = rawData.cleanHashtag ? 'hashtag' : 'tagged';
  const rows = rawData.records
    .filter(p => p.id)
    .map(p => mapRawPostToUgcContent(p, accountId, source, rawData.cleanHashtag || null));
  if (rows.length === 0) return { count: 0 };
  governance?.dispatch({
    type: 'DB_WRITE_REQUESTED', domain: 'ugc', accountId, intentId: null,
    table: 'ugc_content', operation: 'batch_upsert_ugc', rows,
  });
  return { count: rows.length };
}

module.exports = { fetch, persistContent, persistUgc };

