// backend.api/routes/agents/ugc.js
// UGC Discovery endpoints: /search-hashtag, /tags, /repost-ugc, /sync-ugc

const express = require('express');
const router = express.Router();
const axios = require('axios');
const { getSupabaseAdmin, logAudit, logApiRequest } = require('../../config/supabase');
const {
  resolveAccountCredentials,
  categorizeIgError,
  GRAPH_API_BASE,
  buildIdempotencyKey,
  insertQueueRow,
  updateQueueRow,
  pollMediaContainerStatus,
} = require('../../helpers/agent-helpers');
const {
  fetchAndStoreHashtagMedia,
  fetchAndStoreTaggedMedia,
} = require('../../helpers/data-fetchers/ugc-fetchers');

// ============================================
// ENDPOINT 1: POST /search-hashtag (UGC Discovery)
// ============================================

/**
 * Searches for recent media posts by hashtag.
 * Used by: UGC discovery scheduler (scheduler/ugc_discovery.py)
 * Delegates to fetchAndStoreHashtagMedia() for Graph API + Supabase logic.
 */
router.post('/search-hashtag', async (req, res) => {
  const { business_account_id, hashtag, limit } = req.body;

  // HTTP validation
  if (!business_account_id || !hashtag) {
    return res.status(400).json({
      error: 'Missing required fields: business_account_id, hashtag'
    });
  }

  const result = await fetchAndStoreHashtagMedia(business_account_id, hashtag, limit);

  if (!result.success) {
    // Distinguish "not found" from server error
    const isNotFound = result.error && result.error.includes('Hashtag not found');
    return res.status(isNotFound ? 404 : 500).json({
      error: result.error,
      code: result.code,
      retryable: result.retryable,
      error_category: result.error_category,
      retry_after_seconds: result.retry_after_seconds
    });
  }

  res.json({ recent_media: result.media, data: result.media });
});

// ============================================
// ENDPOINT 2: GET /tags (UGC Discovery)
// ============================================

/**
 * Gets posts where the business account is tagged.
 * Used by: UGC discovery scheduler (scheduler/ugc_discovery.py)
 * Delegates to fetchAndStoreTaggedMedia() for Graph API + Supabase logic.
 */
router.get('/tags', async (req, res) => {
  const { business_account_id, limit } = req.query;

  if (!business_account_id) {
    return res.status(400).json({
      error: 'Missing required query parameter: business_account_id'
    });
  }

  const result = await fetchAndStoreTaggedMedia(business_account_id, limit);

  if (!result.success) {
    return res.status(500).json({
      error: result.error,
      code: result.code,
      retryable: result.retryable,
      error_category: result.error_category,
      retry_after_seconds: result.retry_after_seconds
    });
  }

  res.json({ tagged_posts: result.taggedPosts, data: result.taggedPosts });
});

// ============================================
// ENDPOINT 12: POST /repost-ugc (UGC Discovery)
// ============================================

/**
 * Reposts UGC content to the business Instagram account after verifying permission.
 * Used by: UGC discovery scheduler after creator grants permission.
 */
router.post('/repost-ugc', async (req, res) => {
  const startTime = Date.now();
  const { business_account_id, permission_id } = req.body;
  const fallbackEnabled = process.env.POST_FALLBACK_ENABLED === 'true';

  try {
    if (!business_account_id || !permission_id) {
      return res.status(400).json({
        error: 'Missing required fields: business_account_id, permission_id'
      });
    }

    const supabase = getSupabaseAdmin();

    // Step 1: Fetch permission record — must exist and be 'granted'
    // These checks are preconditions, not retryable failures. No queue row inserted if they fail.
    const { data: permission, error: permError } = await supabase
      .from('ugc_permissions')
      .select('id, ugc_content_id, status, business_account_id')
      .eq('id', permission_id)
      .eq('business_account_id', business_account_id)
      .single();

    if (permError || !permission) {
      return res.status(404).json({
        error: 'Permission record not found',
        code: 'PERMISSION_NOT_FOUND'
      });
    }

    if (permission.status !== 'granted') {
      return res.status(403).json({
        error: 'Cannot repost: permission not granted by content creator',
        code: 'PERMISSION_DENIED',
        details: { current_status: permission.status }
      });
    }

    // Step 2: Fetch UGC media data from unified ugc_content
    const { data: ugcContent, error: ugcError } = await supabase
      .from('ugc_content')
      .select('id, media_url, media_type, message, author_username')
      .eq('id', permission.ugc_content_id)
      .single();

    if (ugcError || !ugcContent) {
      return res.status(404).json({
        error: 'UGC content record not found',
        code: 'CONTENT_NOT_FOUND'
      });
    }

    const mediaUrl = ugcContent.media_url;
    if (!mediaUrl) {
      return res.status(400).json({ error: 'UGC content has no media URL', code: 'NO_MEDIA_URL' });
    }

    // --- Queue: pre-log intent (preconditions passed, safe to queue) ---
    let queueId = null;
    if (fallbackEnabled) {
      queueId = await insertQueueRow(supabase, {
        business_account_id,
        action_type: 'repost_ugc',
        payload: { permission_id },
        idempotency_key: buildIdempotencyKey(`repost_ugc:${permission_id}`)
      });
    }

    // Step 3: Resolve credentials and publish (2-step: container → publish)
    const { igUserId, pageToken, userId } = await resolveAccountCredentials(business_account_id);

    const caption = ugcContent.message
      ? `📸 @${ugcContent.author_username}: ${ugcContent.message}\n\n#repost`
      : `📸 @${ugcContent.author_username}\n\n#repost`;

    const ugcMediaType = (ugcContent.media_type || 'IMAGE').toUpperCase();
    const createParams = { caption, access_token: pageToken };
    if (ugcMediaType === 'VIDEO' || ugcMediaType === 'REELS') {
      createParams.video_url = mediaUrl;
      createParams.media_type = ugcMediaType;
    } else {
      createParams.image_url = mediaUrl;
    }

    const createRes = await axios.post(`${GRAPH_API_BASE}/${igUserId}/media`, null, {
      params: createParams,
      timeout: 15000
    });

    const creationId = createRes.data.id;
    if (!creationId) throw new Error('Failed to create media container');

    // --- Queue: persist creation_id so cron retries skip Step 1 ---
    if (queueId) {
      await updateQueueRow(supabase, queueId, {
        payload: { permission_id, creation_id: creationId }
      });
    }

    // For VIDEO/REELS: poll until container status is FINISHED before publishing.
    if (ugcMediaType === 'VIDEO' || ugcMediaType === 'REELS') {
      await pollMediaContainerStatus(creationId, pageToken);
    }

    const publishRes = await axios.post(`${GRAPH_API_BASE}/${igUserId}/media_publish`, null, {
      params: { creation_id: creationId, access_token: pageToken },
      timeout: 15000
    });

    const mediaId = publishRes.data.id;
    const latency = Date.now() - startTime;

    // --- Queue: mark sent ---
    if (queueId) {
      await updateQueueRow(supabase, queueId, {
        status: 'sent',
        instagram_id: mediaId
      });
    }

    // Mark permission as reposted and link published media
    await supabase
      .from('ugc_permissions')
      .update({
        instagram_media_id: mediaId,
        status: 'reposted',
        reposted_at: new Date().toISOString()
      })
      .eq('id', permission_id);

    await logApiRequest({
      endpoint: '/repost-ugc',
      method: 'POST',
      business_account_id,
      user_id: userId,
      success: true,
      latency
    });

    await logAudit({
      event_type: 'ugc_reposted',
      action: 'repost',
      resource_type: 'ugc_permissions',
      resource_id: mediaId,
      details: { permission_id, ugc_content_id: permission.ugc_content_id, author: ugcContent.author_username },
      success: true
    });

    res.json({ success: true, id: mediaId, original_author: ugcContent.author_username, job_id: queueId });

  } catch (error) {
    const latency = Date.now() - startTime;
    const errorMessage = error.response?.data?.error?.message || error.message;
    const { retryable, error_category, retry_after_seconds } = categorizeIgError(error);

    // --- Queue: mark failed or dlq ---
    if (queueId) {
      const nextRetryAt = retryable
        ? new Date(Date.now() + Math.min(Math.pow(2, 1) * 60000, 3600000)).toISOString()
        : null;
      await updateQueueRow(getSupabaseAdmin(), queueId, {
        status: retryable ? 'failed' : 'dlq',
        retry_count: 1,
        error: errorMessage,
        error_category,
        next_retry_at: nextRetryAt
      });
    }

    await logApiRequest({
      endpoint: '/repost-ugc',
      method: 'POST',
      business_account_id,
      success: false,
      error: errorMessage,
      latency
    });

    console.error('❌ UGC repost failed:', errorMessage);
    res.status(error.response?.status || 500).json({
      error: errorMessage,
      code: error.response?.data?.error?.code,
      retryable,
      error_category,
      retry_after_seconds
    });
  }
});

// ============================================
// ENDPOINT 13: POST /sync-ugc (UGC / Analytics)
// ============================================

/**
 * Triggers a fresh sync of tagged/UGC posts from Instagram Graph API into Supabase.
 * Used by: UGC discovery scheduler after processing tags.
 */
router.post('/sync-ugc', async (req, res) => {
  const { business_account_id } = req.body;

  if (!business_account_id) {
    return res.status(400).json({ error: 'Missing required field: business_account_id' });
  }

  // Delegates to ugc-fetchers — handles Graph API call, ugc_content upsert, and domain logging
  const result = await fetchAndStoreTaggedMedia(business_account_id, 50);

  if (!result.success) {
    return res.status(500).json({
      error: result.error,
      code: result.code,
      retryable: result.retryable,
      error_category: result.error_category,
      retry_after_seconds: result.retry_after_seconds
    });
  }

  res.json({ success: true, synced_count: result.count });
});

module.exports = router;
