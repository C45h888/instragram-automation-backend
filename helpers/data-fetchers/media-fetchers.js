// backend.api/helpers/data-fetchers/media-fetchers.js
// Domain: media — business post feed, per-post insights.
// Fetches from Instagram Graph API and upserts to Supabase instagram_media.
// No req/res dependencies — callable from routes and proactive-sync cron.
//
// All api_usage rows written with domain='media' for targeted debugging:
//   SELECT * FROM api_usage WHERE domain = 'media' AND success = false ORDER BY created_at DESC

const {
  axios,
  getSupabaseAdmin,
  resolveAccountCredentials,
  categorizeIgError,
  syncHashtagsFromCaptions,
  GRAPH_API_BASE,
  logWithDomain,
  parseUsageHeader,
} = require('./base');

// ============================================
// MEDIA INSIGHTS — THIN FETCH: FEED
// ============================================

/**
 * Fetches the business account's media feed list.
 * NO DB write — returns the raw media list + credentials for the next pipeline step.
 *
 * @param {string} businessAccountId - UUID
 * @param {string|number} [since] - ISO date string or unix timestamp
 * @param {string|number} [until] - ISO date string or unix timestamp
 * @param {object} [credentials=null] - Pre-resolved credentials
 * @returns {Promise<{success: boolean, mediaList: Array, igUserId: string, pageToken: string, count: number, _usagePct: number|null, error?: string}>}
 */
async function fetchMediaFeed(businessAccountId, since, until, credentials = null) {
  try {
    const { igUserId, pageToken } = credentials || await resolveAccountCredentials(businessAccountId);

    const mediaParams = {
      fields: 'id,media_type,timestamp,caption,media_url,thumbnail_url,permalink,like_count,comments_count',
      limit: 50,
      access_token: pageToken,
    };
    if (since) mediaParams.since = typeof since === 'number' ? since : Math.floor(new Date(since).getTime() / 1000);
    if (until) mediaParams.until = typeof until === 'number' ? until : Math.floor(new Date(until).getTime() / 1000);

    const mediaRes = await axios.get(`${GRAPH_API_BASE}/${igUserId}/media`, { params: mediaParams });
    const mediaList = mediaRes.data.data || [];

    return {
      success: true,
      mediaList,
      igUserId,
      pageToken,
      count: mediaList.length,
      _usagePct: parseUsageHeader(mediaRes.headers?.['x-business-use-case-usage']),
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

// ============================================
// MEDIA INSIGHTS — THIN FETCH: PER-MEDIA INSIGHTS
// ============================================

/**
 * Fetches per-media insights (reach, impressions, saved) for a list of media objects.
 * NO DB write. Batches 5 posts in parallel with 500ms delay between batches
 * to avoid rate-limit bursts.
 *
 * Meta constraint: `saved` is NOT a valid metric for STORY media type.
 * Story valid metrics: reach, impressions (and others like exits, taps_forward, etc.)
 *
 * @param {Array} mediaList - Media objects (must have .id and .media_type)
 * @param {string} pageToken - Page access token
 * @returns {Promise<Array>} mediaInsights — one entry per media item
 */
async function fetchMediaInsightsBatch(mediaList, pageToken) {
  const INSIGHTS_BATCH_SIZE = 5;
  const INSIGHTS_BATCH_DELAY_MS = 500;

  const fetchInsightsForMedia = async (media) => {
    try {
      // Meta: `saved` is not a valid metric for STORY — only for IMAGE, VIDEO, REELS, CAROUSEL_ALBUM
      const isStory = media.media_type === 'STORY';
      const metricParam = isStory ? 'reach,impressions' : 'reach,impressions,saved';

      const insightsRes = await axios.get(`${GRAPH_API_BASE}/${media.id}/insights`, {
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
        insights: insightsRes.data.data || [],
      };
    } catch (err) {
      console.warn(`[media] Failed to fetch insights for media ${media.id}:`, err.message);
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

  const mediaInsights = [];
  for (let i = 0; i < mediaList.length; i += INSIGHTS_BATCH_SIZE) {
    const batch = mediaList.slice(i, i + INSIGHTS_BATCH_SIZE);
    const batchResults = await Promise.all(batch.map(fetchInsightsForMedia));
    mediaInsights.push(...batchResults);
    // Pause between batches to avoid rate-limit bursts (skip delay after last batch)
    if (i + INSIGHTS_BATCH_SIZE < mediaList.length) {
      await new Promise(resolve => setTimeout(resolve, INSIGHTS_BATCH_DELAY_MS));
    }
  }

  return mediaInsights;
}

// ============================================
// MEDIA INSIGHTS — BATCH WRITER
// ============================================

/**
 * Writes media insight records to instagram_media and syncs hashtags.
 * Story saves fix: writes NULL (not 0) for STORY rows — NULL means "metric not applicable"
 * whereas 0 would imply the metric was tracked and found to be zero, which is incorrect.
 *
 * @param {string} businessAccountId - UUID
 * @param {Array} mediaInsights - Output of fetchMediaInsightsBatch()
 * @param {Array<string>} captions - Post captions for hashtag auto-sync
 * @returns {Promise<{count: number}>}
 */
async function storeMediaInsightsBatch(businessAccountId, mediaInsights, captions) {
  if (!mediaInsights.length) return { count: 0 };
  const supabase = getSupabaseAdmin();
  if (!supabase) return { count: 0 };

  const mediaRecords = mediaInsights.map(m => {
    const isStory = m.media_type === 'STORY';
    return {
      instagram_media_id: m.media_id,
      business_account_id: businessAccountId,
      media_type: m.media_type || null,
      caption: m.caption || null,
      media_url: m.media_url || null,
      thumbnail_url: m.thumbnail_url || null,
      permalink: m.permalink || null,
      like_count: m.like_count || 0,
      comments_count: m.comments_count || 0,
      reach: m.insights.find(i => i.name === 'reach')?.values?.[0]?.value || 0,
      impressions: m.insights.find(i => i.name === 'impressions')?.values?.[0]?.value || 0,
      // STORY: saves=NULL (metric not applicable). Non-story: saves=0 if not found.
      saves: isStory ? null : (m.insights.find(i => i.name === 'saved')?.values?.[0]?.value ?? 0),
      published_at: m.timestamp || null,
    };
  });

  const { error: mediaErr } = await supabase
    .from('instagram_media')
    .upsert(mediaRecords, { onConflict: 'instagram_media_id', ignoreDuplicates: false });

  if (mediaErr) {
    await logWithDomain('media', {
      endpoint: '/media-insights/upsert', method: 'SYSTEM', success: false,
      business_account_id: businessAccountId,
      error: mediaErr.message,
      details: { action: 'db_upsert_failed', table: 'instagram_media', count_attempted: mediaRecords.length },
    });
    throw mediaErr;
  }

  if (captions.length > 0) {
    await syncHashtagsFromCaptions(supabase, businessAccountId, captions);
  }

  return { count: mediaRecords.length };
}

// ============================================
// MEDIA INSIGHTS — SHIM (route + sync backward-compat)
// ============================================

/**
 * Fetches media insights and persists to instagram_media.
 * Shim wiring fetchMediaFeed → fetchMediaInsightsBatch → storeMediaInsightsBatch.
 * Both callers (sync/insights.js and routes/agents/analytics.js) keep using this unchanged.
 *
 * @param {string} businessAccountId - UUID
 * @param {string|number} [since] - ISO date string or unix timestamp
 * @param {string|number} [until] - ISO date string or unix timestamp
 * @returns {Promise<{success: boolean, mediaInsights: Array, count: number, error?: string}>}
 */
async function fetchAndStoreMediaInsights(businessAccountId, since, until) {
  const startTime = Date.now();

  try {
    const credentials = await resolveAccountCredentials(businessAccountId);
    const feedResult = await fetchMediaFeed(businessAccountId, since, until, credentials);

    if (!feedResult.success) {
      return {
        success: false, mediaInsights: [], count: 0,
        error: feedResult.error,
        code: feedResult.code,
        retryable: feedResult.retryable,
        error_category: feedResult.error_category,
        retry_after_seconds: feedResult.retry_after_seconds,
      };
    }

    const mediaInsights = await fetchMediaInsightsBatch(feedResult.mediaList, credentials.pageToken);
    const captions = feedResult.mediaList.map(m => m.caption).filter(Boolean);
    await storeMediaInsightsBatch(businessAccountId, mediaInsights, captions);

    const latency = Date.now() - startTime;
    await logWithDomain('media', {
      endpoint: '/media-insights',
      method: 'GET',
      business_account_id: businessAccountId,
      user_id: credentials.igUserId,
      success: true,
      latency,
    });

    return {
      success: true,
      mediaInsights,
      count: mediaInsights.length,
      _usagePct: feedResult._usagePct,
    };

  } catch (error) {
    const latency = Date.now() - startTime;
    const errorMessage = error.response?.data?.error?.message || error.message;
    const { retryable, error_category, retry_after_seconds } = categorizeIgError(error);

    await logWithDomain('media', {
      endpoint: '/media-insights',
      method: 'GET',
      business_account_id: businessAccountId,
      success: false,
      error: errorMessage,
      latency,
      status_code: error.response?.status || null,
      details: { action: 'proxy_failure', error_category, retryable, retry_after_seconds: retry_after_seconds || null, latency_ms: latency },
    });

    return {
      success: false, mediaInsights: [], count: 0, error: errorMessage,
      code: error.response?.data?.error?.code,
      retryable, error_category, retry_after_seconds,
    };
  }
}

// ============================================
// BUSINESS POSTS
// ============================================

/**
 * Fetches the business account's own media feed and upserts full post data to instagram_media.
 * This is the proactive sync that populates the table read by GET /media/:accountId.
 *
 * @param {string} businessAccountId - UUID
 * @param {number} [limit=50] - Max posts to fetch
 * @returns {Promise<{success: boolean, count: number, error?: string}>}
 */
async function fetchAndStoreBusinessPosts(businessAccountId, limit = 50) {
  const startTime = Date.now();
  const fetchLimit = Math.min(parseInt(limit) || 50, 100);

  try {
    const { igUserId, pageToken } = await resolveAccountCredentials(businessAccountId);

    const mediaRes = await axios.get(`${GRAPH_API_BASE}/${igUserId}/media`, {
      params: {
        fields: 'id,media_type,caption,media_url,thumbnail_url,permalink,timestamp,like_count,comments_count',
        limit: fetchLimit,
        access_token: pageToken
      },
      timeout: 15000
    });

    const posts = mediaRes.data.data || [];
    const latency = Date.now() - startTime;

    await logWithDomain('media', {
      endpoint: '/sync/posts',
      method: 'GET',
      business_account_id: businessAccountId,
      user_id: igUserId,
      success: true,
      latency
    });

    if (posts.length > 0) {
      try {
        const supabase = getSupabaseAdmin();
        if (supabase) {
          const mediaRecords = posts.map(p => ({
            instagram_media_id: p.id,
            business_account_id: businessAccountId,
            media_type: p.media_type || null,
            caption: p.caption || null,
            media_url: p.media_url || null,
            thumbnail_url: p.thumbnail_url || null,
            permalink: p.permalink || null,
            like_count: p.like_count || 0,
            comments_count: p.comments_count || 0,
            published_at: p.timestamp || null,
          }));
          const { error: upsertErr } = await supabase
            .from('instagram_media')
            .upsert(mediaRecords, { onConflict: 'instagram_media_id', ignoreDuplicates: false });
          if (upsertErr) {
            await logWithDomain('media', {
              endpoint: '/sync/posts/upsert', method: 'SYSTEM', success: false,
              business_account_id: businessAccountId,
              error: upsertErr.message,
              details: { action: 'db_upsert_failed', table: 'instagram_media', count_attempted: mediaRecords.length },
            });
            throw upsertErr;
          }

          // Auto-populate monitored hashtags from captions
          const captions = posts.map(p => p.caption).filter(Boolean);
          await syncHashtagsFromCaptions(supabase, businessAccountId, captions);
        }
      } catch (wtErr) {
        console.warn('[media] Business posts write-through error:', wtErr.message);
        throw wtErr;
      }
    }

    return {
      success: true, count: posts.length,
      _usagePct: parseUsageHeader(mediaRes.headers?.['x-business-use-case-usage']),
    };

  } catch (error) {
    const latency = Date.now() - startTime;
    const errorMessage = error.response?.data?.error?.message || error.message;
    const { retryable, error_category, retry_after_seconds } = categorizeIgError(error);

    await logWithDomain('media', {
      endpoint: '/sync/posts',
      method: 'GET',
      business_account_id: businessAccountId,
      success: false,
      error: errorMessage,
      latency,
      status_code: error.response?.status || null,
      details: { action: 'proxy_failure', error_category, retryable, retry_after_seconds: retry_after_seconds || null, latency_ms: latency },
    });

    return {
      success: false, count: 0, error: errorMessage,
      code: error.response?.data?.error?.code,
      retryable, error_category, retry_after_seconds
    };
  }
}

module.exports = {
  fetchMediaFeed,
  fetchMediaInsightsBatch,
  storeMediaInsightsBatch,
  fetchAndStoreMediaInsights,
  fetchAndStoreBusinessPosts,
};
