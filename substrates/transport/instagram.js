// substrates/transport/instagram.js
// Bounded substrate: raw Instagram Graph API transport.
//
// Owns: making HTTP calls to the Instagram Graph API, returning raw/semi-structured
//        responses. Each function is a pure fetch — NO persistence, NO normalization.
// Does NOT own: database writes, schema normalization, retry decisions, orchestration.
//
// Every function accepts optional pre-resolved credentials to avoid duplicate
// DB hits when called in parallel batches.

const axios = require('axios');
const { resolveAccountCredentials, categorizeIgError, GRAPH_API_BASE } = require('../../helpers/agent-helpers');
const { logWithDomain } = require('../telemetry');
const { parseUsageHeader } = require('../quota');

// ═══════════════════════════════════════════════════════════════════════════════
// ACCOUNT INSIGHTS
// ═══════════════════════════════════════════════════════════════════════════════

const V1_METRICS = ['reach'];
const V2_METRICS_BASE = ['accounts_engaged', 'profile_views'];
const V2_METRICS_WEBSITE = ['website_clicks'];

/**
 * Fetches account-level insights from the Instagram Graph API. NO DB write.
 * Inlined from the former getAccountInsights in instagram-tokens.
 *
 * @param {string} businessAccountId - UUID
 * @param {Object} [options] - {since, until, period}
 * @param {object} [credentials=null] - Pre-resolved credentials
 * @returns {Promise<object>} { success, v1Data, v2Data, period, hasWebsite, _usagePct, error? }
 */
async function fetchAccountInsights(businessAccountId, options = {}, credentials = null) {
  const { period = '7d', until: untilParam } = options;
  const periodMatch = period.match(/^(\d+)d$/);
  if (!periodMatch) throw new Error(`Invalid period format: ${period}. Use format: '7d', '30d', '90d'`);

  const periodDays = parseInt(periodMatch[1]);
  if (periodDays < 1 || periodDays > 90) throw new Error(`Period must be between 1 and 90 days. Got: ${periodDays}`);

  const until = untilParam || Math.floor(Date.now() / 1000);
  const since = until - (periodDays * 24 * 60 * 60);

  // Check if account has website (v2 total_value metric only for accounts with websites)
  const { igUserId, pageToken } = credentials || await resolveAccountCredentials(businessAccountId);

  const v2Metrics = [...V2_METRICS_BASE];
  let hasWebsite = false;

  if (businessAccountId && !credentials) {
    // credentials not pre-resolved — check website flag via supabase
    const { getSupabaseAdmin } = require('../../config/supabase');
    const supabase = getSupabaseAdmin();
    if (supabase) {
      const { data: accountRow } = await supabase
        .from('instagram_business_accounts')
        .select('website')
        .eq('id', businessAccountId)
        .single();
      hasWebsite = !!accountRow?.website;
      if (hasWebsite) v2Metrics.push(...V2_METRICS_WEBSITE);
    }
  } else if (credentials?._hasWebsite) {
    hasWebsite = credentials._hasWebsite;
    if (hasWebsite) v2Metrics.push(...V2_METRICS_WEBSITE);
  }

  const [v1Response, v2Response] = await Promise.all([
    axios.get(`${GRAPH_API_BASE}/${igUserId}/insights`, {
      params: { metric: V1_METRICS.join(','), period: 'day', since, until, access_token: pageToken },
      timeout: 15000,
    }),
    axios.get(`${GRAPH_API_BASE}/${igUserId}/insights`, {
      params: { metric: v2Metrics.join(','), period: 'day', metric_type: 'total_value', since, until, access_token: pageToken },
      timeout: 15000,
    }),
  ]);

  if (v1Response.data.error) throw new Error(`Instagram API Error (v1): ${v1Response.data.error.message}`);
  if (v2Response.data.error) throw new Error(`Instagram API Error (v2): ${v2Response.data.error.message}`);

  return {
    success: true,
    v1Data: v1Response.data.data || [],
    v2Data: v2Response.data.data || [],
    period: { since, until, days: periodDays, start_date: new Date(since * 1000).toISOString(), end_date: new Date(until * 1000).toISOString() },
    hasWebsite,
    igUserId,
    _usagePct: parseUsageHeader(v1Response.headers?.['x-business-use-case-usage']),
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// COMMENTS
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Fetches comments for a single media post.
 * Meta caps comments at 50 per query.
 *
 * @param {string} businessAccountId - UUID
 * @param {string} mediaId - Instagram media ID (numeric string)
 * @param {number} [limit=50]
 * @param {object} [credentials=null] - Pre-resolved { pageToken, igUserId }
 * @returns {Promise<object>} { success, records, count, paging, _usagePct, error?, code?, retryable?, error_category?, retry_after_seconds? }
 */
async function fetchComments(businessAccountId, mediaId, limit = 50, credentials = null) {
  const startTime = Date.now();
  const fetchLimit = Math.min(parseInt(limit) || 50, 50);

  try {
    const { pageToken, userId } = credentials || await resolveAccountCredentials(businessAccountId);

    const res = await axios.get(`${GRAPH_API_BASE}/${mediaId}/comments`, {
      params: {
        fields: 'id,text,timestamp,username,like_count',
        limit: fetchLimit,
        access_token: pageToken,
      },
      timeout: 10000,
    });

    await logWithDomain('messaging', {
      endpoint: '/post-comments', method: 'GET',
      business_account_id: businessAccountId, user_id: userId,
      success: true, latency: Date.now() - startTime,
    });

    const records = res.data.data || [];
    return {
      success: true, records, count: records.length, paging: res.data.paging || {},
      _usagePct: parseUsageHeader(res.headers?.['x-business-use-case-usage']),
    };
  } catch (error) {
    const errorMessage = error.response?.data?.error?.message || error.message;
    const { retryable, error_category, retry_after_seconds } = categorizeIgError(error);

    await logWithDomain('messaging', {
      endpoint: '/post-comments', method: 'GET',
      business_account_id: businessAccountId,
      success: false, error: errorMessage, latency: Date.now() - startTime,
      status_code: error.response?.status || null,
      details: { error_category, retryable, retry_after_seconds: retry_after_seconds || null },
    });

    return {
      success: false, records: [], count: 0, paging: {}, error: errorMessage,
      code: error.response?.data?.error?.code,
      retryable, error_category, retry_after_seconds,
    };
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// CONVERSATIONS
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Fetches DM conversations. NO DB write.
 * Uses messages.limit(2) so storeConversationBatches can identify the customer's
 * last message even when the business replied most recently.
 *
 * @param {string} businessAccountId
 * @param {number} [limit=20]
 * @param {object} [credentials=null]
 * @returns {Promise<object>} { success, rawConversations, igUserId, pageId, count, _usagePct, error? }
 */
async function fetchConversations(businessAccountId, limit = 20, credentials = null) {
  const startTime = Date.now();
  const fetchLimit = Math.min(parseInt(limit) || 20, 50);

  try {
    const { igUserId, pageToken, userId, pageId } = credentials || await resolveAccountCredentials(businessAccountId);

    const conversationNode = pageId || igUserId;
    const res = await axios.get(`${GRAPH_API_BASE}/${conversationNode}/conversations`, {
      params: {
        fields: 'id,participants{id,username},updated_time,message_count,messages.limit(2){created_time,from{id}}',
        platform: 'INSTAGRAM',
        limit: fetchLimit,
        access_token: pageToken,
      },
      timeout: 10000,
    });

    await logWithDomain('messaging', {
      endpoint: '/conversations', method: 'GET',
      business_account_id: businessAccountId, user_id: userId,
      success: true, latency: Date.now() - startTime,
    });

    return {
      success: true,
      rawConversations: res.data.data || [],
      igUserId, pageId,
      count: (res.data.data || []).length,
      _usagePct: parseUsageHeader(res.headers?.['x-business-use-case-usage']),
    };
  } catch (error) {
    const errorMessage = error.response?.data?.error?.message || error.message;
    const { retryable, error_category, retry_after_seconds } = categorizeIgError(error);

    await logWithDomain('messaging', {
      endpoint: '/conversations', method: 'GET',
      business_account_id: businessAccountId,
      success: false, error: errorMessage, latency: Date.now() - startTime,
      status_code: error.response?.status || null,
      details: { error_category, retryable, retry_after_seconds: retry_after_seconds || null },
    });

    return {
      success: false, rawConversations: [], igUserId: null, pageId: null,
      count: 0, error: errorMessage,
      code: error.response?.data?.error?.code,
      retryable, error_category, retry_after_seconds,
    };
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// MESSAGES
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Fetches messages for a single DM conversation. NO DB write.
 *
 * @param {string} businessAccountId
 * @param {string} conversationId - Instagram thread ID
 * @param {number} [limit=20]
 * @param {object} [credentials=null]
 * @returns {Promise<object>} { success, rawMessages, igUserId, pageId, pageToken, count, _usagePct, error? }
 */
async function fetchMessages(businessAccountId, conversationId, limit = 20, credentials = null) {
  const startTime = Date.now();
  const fetchLimit = Math.min(parseInt(limit) || 20, 100);

  try {
    const { igUserId, pageToken, userId, pageId } = credentials || await resolveAccountCredentials(businessAccountId);

    const res = await axios.get(`${GRAPH_API_BASE}/${conversationId}/messages`, {
      params: {
        fields: 'id,message,from{id,username},to{id,username},created_time,' +
                'attachments{id,image_data{url,preview_url,render_as_sticker,animated_gif_url},file_url,name},' +
                'story,shares,is_unsupported',
        limit: fetchLimit,
        access_token: pageToken,
      },
      timeout: 10000,
    });

    await logWithDomain('messaging', {
      endpoint: '/conversation-messages', method: 'GET',
      business_account_id: businessAccountId, user_id: userId,
      success: true, latency: Date.now() - startTime,
    });

    return {
      success: true,
      rawMessages: res.data.data || [],
      igUserId, pageId, pageToken,
      count: (res.data.data || []).length,
      _usagePct: parseUsageHeader(res.headers?.['x-business-use-case-usage']),
    };
  } catch (error) {
    const errorMessage = error.response?.data?.error?.message || error.message;
    const { retryable, error_category, retry_after_seconds } = categorizeIgError(error);

    await logWithDomain('messaging', {
      endpoint: '/conversation-messages', method: 'GET',
      business_account_id: businessAccountId,
      success: false, error: errorMessage, latency: Date.now() - startTime,
      status_code: error.response?.status || null,
      details: { error_category, retryable, retry_after_seconds: retry_after_seconds || null },
    });

    return {
      success: false, rawMessages: [], igUserId: null, pageId: null,
      count: 0, error: errorMessage,
      code: error.response?.data?.error?.code,
      retryable, error_category, retry_after_seconds,
    };
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// MEDIA FEED
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Fetches the business account's media feed list. NO DB write.
 *
 * @param {string} businessAccountId
 * @param {string|number} [since] - ISO date string or unix timestamp
 * @param {string|number} [until]
 * @param {object} [credentials=null]
 * @returns {Promise<object>} { success, mediaList, igUserId, pageToken, count, _usagePct, error? }
 */
async function fetchMediaFeed(businessAccountId, since, until, credentials = null) {
  try {
    const { igUserId, pageToken } = credentials || await resolveAccountCredentials(businessAccountId);

    const params = {
      fields: 'id,media_type,timestamp,caption,media_url,thumbnail_url,permalink,like_count,comments_count',
      limit: 50,
      access_token: pageToken,
    };
    if (since) params.since = typeof since === 'number' ? since : Math.floor(new Date(since).getTime() / 1000);
    if (until) params.until = typeof until === 'number' ? until : Math.floor(new Date(until).getTime() / 1000);

    const res = await axios.get(`${GRAPH_API_BASE}/${igUserId}/media`, { params });

    return {
      success: true,
      mediaList: res.data.data || [],
      igUserId, pageToken,
      count: (res.data.data || []).length,
      _usagePct: parseUsageHeader(res.headers?.['x-business-use-case-usage']),
    };
  } catch (error) {
    const errorMessage = error.response?.data?.error?.message || error.message;
    const { retryable, error_category, retry_after_seconds } = categorizeIgError(error);
    return {
      success: false, mediaList: [], igUserId: null, pageToken: null, count: 0,
      error: errorMessage,
      code: error.response?.data?.error?.code,
      retryable, error_category, retry_after_seconds,
    };
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// MEDIA INSIGHTS BATCH
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Fetches per-media insights (reach, impressions, saved) for a list of media objects.
 * Batches 5 posts in parallel with 500ms delay between batches.
 * NO DB write.
 *
 * @param {Array} mediaList - Media objects with .id and .media_type
 * @param {string} pageToken
 * @returns {Promise<Array>} mediaInsights — one entry per media item
 */
async function fetchMediaInsightsBatch(mediaList, pageToken) {
  const BATCH_SIZE = 5;
  const BATCH_DELAY_MS = 500;

  const fetchOne = async (media) => {
    try {
      const isStory = media.media_type === 'STORY';
      const metricParam = isStory ? 'reach,impressions' : 'reach,impressions,saved';

      const res = await axios.get(`${GRAPH_API_BASE}/${media.id}/insights`, {
        params: { metric: metricParam, access_token: pageToken },
      });

      return {
        media_id: media.id,
        media_type: media.media_type,
        timestamp: media.timestamp,
        caption: media.caption || null,
        media_url: media.media_url || null,
        thumbnail_url: media.thumbnail_url || null,
        permalink: media.permalink || null,
        like_count: media.like_count || 0,
        comments_count: media.comments_count || 0,
        insights: res.data.data || [],
      };
    } catch (err) {
      console.warn(`[transport] Failed to fetch insights for media ${media.id}:`, err.message);
      return {
        media_id: media.id,
        media_type: media.media_type,
        timestamp: media.timestamp,
        caption: media.caption || null,
        media_url: media.media_url || null,
        thumbnail_url: media.thumbnail_url || null,
        permalink: media.permalink || null,
        like_count: media.like_count || 0,
        comments_count: media.comments_count || 0,
        insights: [],
        error: err.message,
      };
    }
  };

  const results = [];
  for (let i = 0; i < mediaList.length; i += BATCH_SIZE) {
    const batch = mediaList.slice(i, i + BATCH_SIZE);
    const batchResults = await Promise.all(batch.map(fetchOne));
    results.push(...batchResults);
    if (i + BATCH_SIZE < mediaList.length) {
      await new Promise(resolve => setTimeout(resolve, BATCH_DELAY_MS));
    }
  }
  return results;
}

// ═══════════════════════════════════════════════════════════════════════════════
// BUSINESS POSTS
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Fetches the business account's own media feed. NO DB write.
 * Extracted from the former fetchAndStoreBusinessPosts (which combined fetch + store).
 *
 * @param {string} businessAccountId
 * @param {number} [limit=50]
 * @returns {Promise<object>} { success, posts, count, _usagePct, error? }
 */
async function fetchBusinessPosts(businessAccountId, limit = 50) {
  const startTime = Date.now();
  const fetchLimit = Math.min(parseInt(limit) || 50, 100);

  try {
    const { igUserId, pageToken } = await resolveAccountCredentials(businessAccountId);

    const res = await axios.get(`${GRAPH_API_BASE}/${igUserId}/media`, {
      params: {
        fields: 'id,media_type,caption,media_url,thumbnail_url,permalink,timestamp,like_count,comments_count',
        limit: fetchLimit,
        access_token: pageToken,
      },
      timeout: 15000,
    });

    const posts = res.data.data || [];

    await logWithDomain('media', {
      endpoint: '/sync/posts', method: 'GET',
      business_account_id: businessAccountId, user_id: igUserId,
      success: true, latency: Date.now() - startTime,
    });

    return {
      success: true, posts, count: posts.length,
      _usagePct: parseUsageHeader(res.headers?.['x-business-use-case-usage']),
    };
  } catch (error) {
    const errorMessage = error.response?.data?.error?.message || error.message;
    const { retryable, error_category, retry_after_seconds } = categorizeIgError(error);

    await logWithDomain('media', {
      endpoint: '/sync/posts', method: 'GET',
      business_account_id: businessAccountId,
      success: false, error: errorMessage, latency: Date.now() - startTime,
      status_code: error.response?.status || null,
      details: { error_category, retryable, retry_after_seconds: retry_after_seconds || null },
    });

    return {
      success: false, posts: [], count: 0, error: errorMessage,
      code: error.response?.data?.error?.code,
      retryable, error_category, retry_after_seconds,
    };
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// HASHTAG MEDIA
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Searches hashtag media (2-step: hashtag ID lookup → recent media). NO DB write.
 * Shapes records via mapRawPostToUgcContent so they're ready for storeUgcContentBatch.
 *
 * @param {string} businessAccountId
 * @param {string} hashtag - Hashtag string (with or without #)
 * @param {number} [limit=25]
 * @param {object} [credentials=null]
 * @returns {Promise<object>} { success, records, count, hashtagId?, _usagePct?, error? }
 */
async function fetchHashtagMedia(businessAccountId, hashtag, limit = 25, credentials = null) {
  const startTime = Date.now();
  const searchLimit = Math.min(parseInt(limit) || 25, 50);
  const cleanHashtag = String(hashtag).replace(/^#/, '');

  try {
    const { igUserId, pageToken } = credentials || await resolveAccountCredentials(businessAccountId);

    // Step 1: Search for hashtag ID
    const searchRes = await axios.get(`${GRAPH_API_BASE}/ig_hashtag_search`, {
      params: { user_id: igUserId, q: cleanHashtag, access_token: pageToken },
    });

    const hashtagId = searchRes.data?.data?.[0]?.id;
    if (!hashtagId) {
      return { success: false, records: [], count: 0, error: `Hashtag not found: #${cleanHashtag}` };
    }

    // Step 2: Get recent media
    const mediaRes = await axios.get(`${GRAPH_API_BASE}/${hashtagId}/recent_media`, {
      params: {
        user_id: igUserId,
        fields: 'id,media_type,media_url,thumbnail_url,permalink,caption,timestamp,username,like_count,comments_count,owner{id}',
        limit: searchLimit,
        access_token: pageToken,
      },
    });

    await logWithDomain('ugc', {
      endpoint: '/search-hashtag', method: 'POST',
      business_account_id: businessAccountId, user_id: igUserId,
      success: true, latency: Date.now() - startTime,
    });

    const rawMedia = (mediaRes.data.data || []).map(item => ({
      ...item,
      owner_id: item.owner?.id || null,
    }));

    return {
      success: true,
      rawMedia,
      records: rawMedia, // normalized by caller via mapRawPostToUgcContent
      hashtagId,
      cleanHashtag,
      count: rawMedia.length,
      _usagePct: parseUsageHeader(mediaRes.headers?.['x-business-use-case-usage']),
    };
  } catch (error) {
    const errorMessage = error.response?.data?.error?.message || error.message;
    const { retryable, error_category, retry_after_seconds } = categorizeIgError(error);

    await logWithDomain('ugc', {
      endpoint: '/search-hashtag', method: 'POST',
      business_account_id: businessAccountId,
      success: false, error: errorMessage, latency: Date.now() - startTime,
      status_code: error.response?.status || null,
      details: { error_category, retryable, retry_after_seconds: retry_after_seconds || null },
    });

    return {
      success: false, records: [], rawMedia: [], count: 0, error: errorMessage,
      code: error.response?.data?.error?.code,
      retryable, error_category, retry_after_seconds,
    };
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// TAGGED MEDIA
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Fetches posts where the business account is tagged. NO DB write.
 *
 * @param {string} businessAccountId
 * @param {number} [limit=25]
 * @param {object} [credentials=null]
 * @returns {Promise<object>} { success, records, count, paging, _usagePct?, error? }
 */
async function fetchTaggedMedia(businessAccountId, limit = 25, credentials = null) {
  const startTime = Date.now();
  const fetchLimit = Math.min(parseInt(limit) || 25, 50);

  try {
    const { igUserId, pageToken } = credentials || await resolveAccountCredentials(businessAccountId);

    const res = await axios.get(`${GRAPH_API_BASE}/${igUserId}/tags`, {
      params: {
        fields: 'id,media_type,media_url,thumbnail_url,caption,permalink,timestamp,username,like_count,comments_count,owner{id}',
        limit: fetchLimit,
        access_token: pageToken,
      },
    });

    await logWithDomain('ugc', {
      endpoint: '/tags', method: 'GET',
      business_account_id: businessAccountId, user_id: igUserId,
      success: true, latency: Date.now() - startTime,
    });

    const rawPosts = (res.data.data || []).map(p => ({
      ...p,
      owner_id: p.owner?.id || null,
    }));

    return {
      success: true,
      records: rawPosts,
      count: rawPosts.length,
      paging: res.data.paging || {},
      _usagePct: parseUsageHeader(res.headers?.['x-business-use-case-usage']),
    };
  } catch (error) {
    const errorMessage = error.response?.data?.error?.message || error.message;
    const { retryable, error_category, retry_after_seconds } = categorizeIgError(error);

    await logWithDomain('ugc', {
      endpoint: '/tags', method: 'GET',
      business_account_id: businessAccountId,
      success: false, error: errorMessage, latency: Date.now() - startTime,
      status_code: error.response?.status || null,
      details: { error_category, retryable, retry_after_seconds: retry_after_seconds || null },
    });

    return {
      success: false, records: [], count: 0, paging: {}, error: errorMessage,
      code: error.response?.data?.error?.code,
      retryable, error_category, retry_after_seconds,
    };
  }
}

module.exports = {
  fetchAccountInsights,
  fetchComments,
  fetchConversations,
  fetchMessages,
  fetchMediaFeed,
  fetchMediaInsightsBatch,
  fetchBusinessPosts,
  fetchHashtagMedia,
  fetchTaggedMedia,
};
