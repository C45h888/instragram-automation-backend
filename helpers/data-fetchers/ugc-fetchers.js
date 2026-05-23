// backend.api/helpers/data-fetchers/ugc-fetchers.js
// Domain: ugc — hashtag search, tagged media.
// Fetches from Instagram Graph API and upserts to Supabase ugc_content.
// No req/res dependencies — callable from routes and proactive-sync cron.
//
// All api_usage rows written with domain='ugc' for targeted debugging:
//   SELECT * FROM api_usage WHERE domain = 'ugc' AND success = false ORDER BY created_at DESC
//
// Thin fetch/write split:
//   fetchHashtagMedia()       — IG API calls only, returns shaped records (no DB write)
//   fetchAndStoreHashtagMedia() — shim: fetchHashtagMedia + storeUgcContentBatch (route compat)
//   fetchAndStoreTaggedMedia()  — unchanged (called once per account, no inner loop)

const {
  axios,
  getSupabaseAdmin,
  resolveAccountCredentials,
  categorizeIgError,
  mapRawPostToUgcContent,
  GRAPH_API_BASE,
  logWithDomain,
  storeUgcContentBatch,
  parseUsageHeader,
} = require('./base');

// ============================================
// HASHTAG MEDIA — THIN FETCH
// ============================================

/**
 * Searches hashtag media from the Instagram Graph API.
 * NO DB write — shapes records via mapRawPostToUgcContent so they're ready for storeUgcContentBatch.
 *
 * @param {string} businessAccountId - UUID
 * @param {string} hashtag - Hashtag string (with or without #)
 * @param {number} [limit=25] - Max media (capped at 50)
 * @returns {Promise<{success: boolean, records: Array, count: number, hashtagId?: string, error?: string}>}
 */
async function fetchHashtagMedia(businessAccountId, hashtag, limit = 25, credentials = null) {
  const startTime = Date.now();
  const searchLimit = Math.min(parseInt(limit) || 25, 50);
  const cleanHashtag = String(hashtag).replace(/^#/, '');

  try {
    const { igUserId, pageToken } = credentials || await resolveAccountCredentials(businessAccountId);

    // Step 1: Search for hashtag ID
    const hashtagSearchRes = await axios.get(`${GRAPH_API_BASE}/ig_hashtag_search`, {
      params: {
        user_id: igUserId,
        q: cleanHashtag,
        access_token: pageToken
      }
    });

    const hashtagId = hashtagSearchRes.data?.data?.[0]?.id;
    if (!hashtagId) {
      return { success: false, records: [], count: 0, error: `Hashtag not found: #${cleanHashtag}` };
    }

    // Step 2: Get recent media for hashtag
    const mediaRes = await axios.get(`${GRAPH_API_BASE}/${hashtagId}/recent_media`, {
      params: {
        user_id: igUserId,
        fields: 'id,media_type,media_url,thumbnail_url,permalink,caption,timestamp,username,like_count,comments_count,owner{id}',
        limit: searchLimit,
        access_token: pageToken
      }
    });

    const latency = Date.now() - startTime;

    await logWithDomain('ugc', {
      endpoint: '/search-hashtag',
      method: 'POST',
      business_account_id: businessAccountId,
      user_id: igUserId,
      success: true,
      latency
    });

    // Flatten owner{id} → owner_id then shape via mapRawPostToUgcContent (DB-ready rows)
    const rawMedia = (mediaRes.data.data || []).map(item => ({
      ...item,
      owner_id: item.owner?.id || null,
    }));

    const records = rawMedia
      .filter(m => m.id)
      .map(m => mapRawPostToUgcContent(m, businessAccountId, 'hashtag', cleanHashtag));

    if (rawMedia.length >= searchLimit) {
      logWithDomain('ugc', {
        endpoint: '/search-hashtag/paging', method: 'SYSTEM', success: true,
        business_account_id: businessAccountId,
        details: { action: 'paging_next_detected', items_this_page: rawMedia.length, next_cursor_present: true },
      }).catch(() => {});
    }

    return {
      success: true, records, count: records.length, hashtagId,
      _usagePct: parseUsageHeader(mediaRes.headers?.['x-business-use-case-usage']),
    };

  } catch (error) {
    const latency = Date.now() - startTime;
    const errorMessage = error.response?.data?.error?.message || error.message;
    const { retryable, error_category, retry_after_seconds } = categorizeIgError(error);

    await logWithDomain('ugc', {
      endpoint: '/search-hashtag',
      method: 'POST',
      business_account_id: businessAccountId,
      success: false,
      error: errorMessage,
      latency,
      status_code: error.response?.status || null,
      details: { action: 'proxy_failure', error_category, retryable, retry_after_seconds: retry_after_seconds || null, latency_ms: latency },
    });

    return {
      success: false, records: [], count: 0, error: errorMessage,
      code: error.response?.data?.error?.code,
      retryable, error_category, retry_after_seconds
    };
  }
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
// TAGGED MEDIA — THIN FETCH
// ============================================

/**
 * Fetches posts where the business account is tagged by other users.
 * NO DB write — callers use storeUgcContentBatch() for persistence.
 *
 * Field fix: owner{id} added — previously missing, causing author_instagram_id to
 * always be null in ugc_content, breaking UGC creator DM permission flows.
 *
 * @param {string} businessAccountId - UUID
 * @param {number} [limit=25] - Max tagged posts (capped at 50)
 * @param {object} [credentials=null] - Pre-resolved credentials
 * @returns {Promise<{success: boolean, records: Array, count: number, paging: Object, _usagePct: number|null, error?: string}>}
 */
async function fetchTaggedMedia(businessAccountId, limit = 25, credentials = null) {
  const startTime = Date.now();
  const fetchLimit = Math.min(parseInt(limit) || 25, 50);

  try {
    const { igUserId, pageToken } = credentials || await resolveAccountCredentials(businessAccountId);

    const tagsRes = await axios.get(`${GRAPH_API_BASE}/${igUserId}/tags`, {
      params: {
        // owner{id} added: was missing, causing author_instagram_id=null for all tagged UGC
        fields: 'id,media_type,media_url,thumbnail_url,caption,permalink,timestamp,username,like_count,comments_count,owner{id}',
        limit: fetchLimit,
        access_token: pageToken,
      },
    });

    const latency = Date.now() - startTime;

    await logWithDomain('ugc', {
      endpoint: '/tags',
      method: 'GET',
      business_account_id: businessAccountId,
      user_id: igUserId,
      success: true,
      latency,
    });

    const rawPosts = tagsRes.data.data || [];
    const paging = tagsRes.data.paging || {};

    // Flatten owner{id} → owner_id then shape via mapRawPostToUgcContent (DB-ready rows)
    const records = rawPosts
      .filter(p => p.id)
      .map(p => mapRawPostToUgcContent(
        { ...p, owner_id: p.owner?.id || null },
        businessAccountId,
        'tagged',
        null
      ));

    return {
      success: true,
      records,
      count: records.length,
      paging,
      _usagePct: parseUsageHeader(tagsRes.headers?.['x-business-use-case-usage']),
    };

  } catch (error) {
    const latency = Date.now() - startTime;
    const errorMessage = error.response?.data?.error?.message || error.message;
    const { retryable, error_category, retry_after_seconds } = categorizeIgError(error);

    await logWithDomain('ugc', {
      endpoint: '/tags',
      method: 'GET',
      business_account_id: businessAccountId,
      success: false,
      error: errorMessage,
      latency,
      status_code: error.response?.status || null,
      details: { action: 'proxy_failure', error_category, retryable, retry_after_seconds: retry_after_seconds || null, latency_ms: latency },
    });

    return {
      success: false, records: [], count: 0, paging: {}, error: errorMessage,
      code: error.response?.data?.error?.code,
      retryable, error_category, retry_after_seconds,
    };
  }
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
