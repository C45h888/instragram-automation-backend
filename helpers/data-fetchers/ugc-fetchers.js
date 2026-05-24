// helpers/data-fetchers/ugc-fetchers.js
// Domain: ugc — hashtag search, tagged media.
// Transport layer lives in substrates/transport/instagram.js.
// This file is a thin wrapper: transport → normalize → persistence call.
// No req/res dependencies — callable from routes and proactive-sync cron.
//
// All api_usage rows written with domain='ugc' for targeted debugging:
//   SELECT * FROM api_usage WHERE domain = 'ugc' AND success = false ORDER BY created_at DESC
//
// Thin fetch/write split:
//   fetchHashtagMedia()        → delegates to transport (no DB write)
//   fetchAndStoreHashtagMedia() — shim: transport + storeUgcContentBatch
//   fetchTaggedMedia()         → delegates to transport (no DB write)
//   fetchAndStoreTaggedMedia()  — shim: transport + storeUgcContentBatch

const {
  categorizeIgError,
  mapRawPostToUgcContent,
  logWithDomain,
  storeUgcContentBatch,
} = require('./base');
const transport = require('../../substrates/transport/instagram');

// ============================================
// HASHTAG MEDIA — THIN FETCH (delegates to transport)
// ============================================

/**
 * Searches hashtag media from the Instagram Graph API.
 * Delegates to transport.fetchHashtagMedia, then shapes records via mapRawPostToUgcContent.
 * NO DB write — callers use storeUgcContentBatch() for persistence.
 *
 * @param {string} businessAccountId - UUID
 * @param {string} hashtag - Hashtag string (with or without #)
 * @param {number} [limit=25] - Max media (capped at 50)
 * @param {object} [credentials=null] - Pre-resolved credentials
 * @returns {Promise<{success: boolean, records: Array, count: number, hashtagId?: string, error?: string}>}
 */
async function fetchHashtagMedia(businessAccountId, hashtag, limit = 25, credentials = null) {
  const startTime = Date.now();
  const result = await transport.fetchHashtagMedia(businessAccountId, hashtag, limit, credentials);

  if (!result.success) {
    const errorMessage = result.error || 'Unknown error';
    const { retryable, error_category, retry_after_seconds } = result;
    await logWithDomain('ugc', {
      endpoint: '/search-hashtag', method: 'POST',
      business_account_id: businessAccountId,
      success: false, error: errorMessage, latency: Date.now() - startTime,
      status_code: null,
      details: { action: 'proxy_failure', error_category, retryable, retry_after_seconds: retry_after_seconds || null },
    });
    return result;
  }

  // Shape records via mapRawPostToUgcContent (DB-ready rows)
  const cleanHashtag = result.cleanHashtag || String(hashtag).replace(/^#/, '');
  const records = (result.rawMedia || result.records || [])
    .filter(m => m.id)
    .map(m => mapRawPostToUgcContent({ ...m, owner_id: m.owner_id || m.owner?.id || null }, businessAccountId, 'hashtag', cleanHashtag));

  await logWithDomain('ugc', {
    endpoint: '/search-hashtag', method: 'POST',
    business_account_id: businessAccountId,
    success: true, latency: Date.now() - startTime,
  });

  return { ...result, records, count: records.length };
}

// ============================================
// HASHTAG MEDIA — SHIM (route backward-compat)
// ============================================

/**
 * Searches hashtag media and persists to ugc_content.
 * Shim wrapping fetchHashtagMedia + storeUgcContentBatch.
 * Routes call this unchanged; domain loops call fetchHashtagMedia directly for batch parallelism.
 *
 * @param {string} businessAccountId
 * @param {string} hashtag
 * @param {number} [limit=25]
 * @returns {Promise<{success: boolean, media: Array, count: number, hashtagId?: string, error?: string}>}
 */
async function fetchAndStoreHashtagMedia(businessAccountId, hashtag, limit = 25) {
  const result = await fetchHashtagMedia(businessAccountId, hashtag, limit);
  if (result.success && result.records.length > 0) {
    await storeUgcContentBatch(result.records);
  }
  // backward-compat: callers expect .media, not .records
  return { ...result, media: result.records };
}

// ============================================
// TAGGED MEDIA — THIN FETCH (delegates to transport)
// ============================================

/**
 * Fetches posts where the business account is tagged by other users.
 * Delegates to transport.fetchTaggedMedia, then shapes records via mapRawPostToUgcContent.
 * NO DB write — callers use storeUgcContentBatch() for persistence.
 *
 * @param {string} businessAccountId - UUID
 * @param {number} [limit=25] - Max tagged posts (capped at 50)
 * @param {object} [credentials=null] - Pre-resolved credentials
 * @returns {Promise<{success: boolean, records: Array, count: number, paging: Object, _usagePct: number|null, error?: string}>}
 */
async function fetchTaggedMedia(businessAccountId, limit = 25, credentials = null) {
  const startTime = Date.now();
  const result = await transport.fetchTaggedMedia(businessAccountId, limit, credentials);

  if (!result.success) {
    const errorMessage = result.error || 'Unknown error';
    const { retryable, error_category, retry_after_seconds } = result;
    await logWithDomain('ugc', {
      endpoint: '/tags', method: 'GET',
      business_account_id: businessAccountId,
      success: false, error: errorMessage, latency: Date.now() - startTime,
      status_code: null,
      details: { action: 'proxy_failure', error_category, retryable, retry_after_seconds: retry_after_seconds || null },
    });
    return result;
  }

  // Shape records via mapRawPostToUgcContent (DB-ready rows)
  const records = (result.records || [])
    .filter(p => p.id)
    .map(p => mapRawPostToUgcContent(
      { ...p, owner_id: p.owner_id || p.owner?.id || null },
      businessAccountId,
      'tagged',
      null
    ));

  await logWithDomain('ugc', {
    endpoint: '/tags', method: 'GET',
    business_account_id: businessAccountId,
    success: true, latency: Date.now() - startTime,
  });

  return { ...result, records, count: records.length };
}

// ============================================
// TAGGED MEDIA — SHIM (route + sync backward-compat)
// ============================================

/**
 * Fetches tagged posts and persists to ugc_content.
 * Shim wrapping fetchTaggedMedia + storeUgcContentBatch.
 * All callers (sync/ugc.js, routes /tags, /sync-ugc) keep using this unchanged.
 *
 * @param {string} businessAccountId
 * @param {number} [limit=25]
 * @returns {Promise<{success: boolean, taggedPosts: Array, count: number, error?: string}>}
 */
async function fetchAndStoreTaggedMedia(businessAccountId, limit = 25) {
  const result = await fetchTaggedMedia(businessAccountId, limit);
  if (result.success && result.records.length > 0) {
    await storeUgcContentBatch(result.records);
  }
  // backward-compat: callers expect .taggedPosts; sync uses .count and .success only
  return { ...result, taggedPosts: result.records };
}

module.exports = {
  fetchHashtagMedia,
  fetchTaggedMedia,
  fetchAndStoreHashtagMedia,
  fetchAndStoreTaggedMedia,
};
