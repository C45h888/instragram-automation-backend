// substrates/ugc/transport.js
// UGC substrate transport: hashtag search and tagged media.
//
// Owns: calling Instagram Graph API for user-generated content discovery.
// Does NOT own: DB writes, normalization, retry logic, orchestration.
//
// Decomposed from substrates/transport/instagram.js (former god module).

const {
  axios,
  GRAPH_API_BASE,
  resolveCreds,
  clampLimit,
  buildErrorResponse,
  extractUsage,
  logTelemetry,
} = require('../../../substrates/transport/_shared');

// ═══════════════════════════════════════════════════════════════════════════════
// HASHTAG MEDIA
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Search for media by hashtag (2-step: hashtag ID lookup → recent media).
 *
 * @param {string} accountId
 * @param {string} hashtag - with or without # prefix
 * @param {number} [limit=25]
 * @param {object} [credentials=null] - pre-resolved { igUserId, pageToken }
 * @returns {Promise<object>} { success, rawMedia, hashtagId, cleanHashtag, count, _usagePct, ...errorMeta }
 */
async function fetchHashtagMedia(accountId, hashtag, limit = 25, credentials = null) {
  const startTime = Date.now();
  const searchLimit = clampLimit(limit, 25, 50);
  const cleanHashtag = String(hashtag).replace(/^#/, '');

  try {
    const { igUserId, pageToken } = await resolveCreds(accountId, credentials);

    // Step 1: resolve hashtag to ID
    const searchRes = await axios.get(`${GRAPH_API_BASE}/ig_hashtag_search`, {
      params: { user_id: igUserId, q: cleanHashtag, access_token: pageToken },
    });

    const hashtagId = searchRes.data?.data?.[0]?.id;
    if (!hashtagId) {
      return { success: false, rawMedia: [], count: 0, error: `Hashtag not found: #${cleanHashtag}` };
    }

    // Step 2: fetch recent media for that hashtag
    const mediaRes = await axios.get(`${GRAPH_API_BASE}/${hashtagId}/recent_media`, {
      params: {
        user_id: igUserId,
        fields: 'id,media_type,media_url,thumbnail_url,permalink,caption,timestamp,username,like_count,comments_count,owner{id}',
        limit: searchLimit,
        access_token: pageToken,
      },
    });

    await logTelemetry('ugc', '/hashtag-media', accountId, igUserId, true, Date.now() - startTime);

    const rawMedia = (mediaRes.data.data || []).map(item => ({
      ...item,
      owner_id: item.owner?.id || null,
    }));

    return {
      success: true,
      rawMedia,
      hashtagId,
      cleanHashtag,
      count: rawMedia.length,
      _usagePct: extractUsage(mediaRes.headers),
    };
  } catch (error) {
    await logTelemetry('ugc', '/hashtag-media', accountId, null, false, Date.now() - startTime, {
      status_code: error.response?.status || null,
      error: error.response?.data?.error?.message || error.message,
      meta: buildErrorResponse(error),
    });
    return { success: false, rawMedia: [], count: 0, ...buildErrorResponse(error) };
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// TAGGED MEDIA
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Fetch posts where the business account is tagged.
 *
 * @param {string} accountId
 * @param {number} [limit=25]
 * @param {object} [credentials=null] - pre-resolved { igUserId, pageToken }
 * @returns {Promise<object>} { success, records, count, paging, _usagePct, ...errorMeta }
 */
async function fetchTaggedMedia(accountId, limit = 25, credentials = null) {
  const startTime = Date.now();
  const fetchLimit = clampLimit(limit, 25, 50);

  try {
    const { igUserId, pageToken } = await resolveCreds(accountId, credentials);

    const res = await axios.get(`${GRAPH_API_BASE}/${igUserId}/tags`, {
      params: {
        fields: 'id,media_type,media_url,thumbnail_url,caption,permalink,timestamp,username,like_count,comments_count,owner{id}',
        limit: fetchLimit,
        access_token: pageToken,
      },
    });

    await logTelemetry('ugc', '/tagged-media', accountId, igUserId, true, Date.now() - startTime);

    const records = (res.data.data || []).map(p => ({
      ...p,
      owner_id: p.owner?.id || null,
    }));

    return {
      success: true, records, count: records.length,
      paging: res.data.paging || {},
      _usagePct: extractUsage(res.headers),
    };
  } catch (error) {
    await logTelemetry('ugc', '/tagged-media', accountId, null, false, Date.now() - startTime, {
      status_code: error.response?.status || null,
      error: error.response?.data?.error?.message || error.message,
      meta: buildErrorResponse(error),
    });
    return { success: false, records: [], count: 0, paging: {}, ...buildErrorResponse(error) };
  }
}

module.exports = { fetchHashtagMedia, fetchTaggedMedia };
