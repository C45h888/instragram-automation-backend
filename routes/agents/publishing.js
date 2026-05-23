// backend.api/routes/agents/publishing.js
// Content Scheduler endpoints: /publish-post

const express = require('express');
const router = express.Router();
const axios = require('axios');
const { getSupabaseAdmin, logApiRequest, logAudit } = require('../../config/supabase');
const {
  resolveAccountCredentials,
  categorizeIgError,
  GRAPH_API_BASE,
  buildIdempotencyKey,
  insertQueueRow,
  updateQueueRow,
  pollMediaContainerStatus,
} = require('../../helpers/agent-helpers');

// ============================================
// ENDPOINT 4: POST /publish-post (Content Scheduler)
// ============================================

/**
 * Publishes an Instagram post (2-step: create media container, then publish)
 * Used by: Content scheduler (scheduler/content_scheduler.py)
 */
router.post('/publish-post', async (req, res) => {
  const startTime = Date.now();
  const { business_account_id, image_url, caption, media_type, scheduled_post_id } = req.body;
  const fallbackEnabled = process.env.POST_FALLBACK_ENABLED === 'true';

  try {
    if (!business_account_id || !image_url || !caption) {
      return res.status(400).json({
        error: 'Missing required fields: business_account_id, image_url, caption'
      });
    }

    const type = (media_type || 'IMAGE').toUpperCase();

    // --- Queue: pre-log intent before 2-step publish (creation_id not yet known) ---
    let queueId = null;
    if (fallbackEnabled) {
      const supabase = getSupabaseAdmin();
      if (supabase) {
        // Use scheduled_post_id for stable dedup; fall back to image_url hash
        const idemSeed = scheduled_post_id
          ? `publish_post:${scheduled_post_id}`
          : `publish_post:${buildIdempotencyKey(image_url).slice(0, 16)}`;
        queueId = await insertQueueRow(supabase, {
          business_account_id,
          action_type: 'publish_post',
          payload: {
            image_url,
            caption,
            media_type: type,
            scheduled_post_id: scheduled_post_id || null
          },
          idempotency_key: buildIdempotencyKey(idemSeed)
        });
      }
    }

    const { igUserId, pageToken, userId } = await resolveAccountCredentials(business_account_id);

    // Step 1: Create media container
    const createPayload = { caption, access_token: pageToken };

    if (type === 'VIDEO' || type === 'REELS') {
      createPayload.video_url = image_url;
      createPayload.media_type = type;
    } else {
      createPayload.image_url = image_url;
    }

    const createRes = await axios.post(`${GRAPH_API_BASE}/${igUserId}/media`, null, {
      params: createPayload,
      timeout: 15000
    });
    const creationId = createRes.data.id;

    if (!creationId) throw new Error('Failed to create media container');

    // --- Queue: persist creation_id so cron retries skip Step 1 ---
    if (queueId) {
      await updateQueueRow(getSupabaseAdmin(), queueId, {
        payload: { image_url, caption, media_type: type, scheduled_post_id: scheduled_post_id || null, creation_id: creationId }
      });
    }

    // For VIDEO/REELS: poll until container status is FINISHED before publishing.
    // Meta requires this — calling media_publish before FINISHED returns (#9007).
    if (type === 'VIDEO' || type === 'REELS') {
      await pollMediaContainerStatus(creationId, pageToken);
    }

    // Step 2: Publish media container
    const publishRes = await axios.post(`${GRAPH_API_BASE}/${igUserId}/media_publish`, null, {
      params: { creation_id: creationId, access_token: pageToken },
      timeout: 15000
    });

    const mediaId = publishRes.data.id;
    const latency = Date.now() - startTime;

    // --- Queue: mark sent ---
    if (queueId) {
      await updateQueueRow(getSupabaseAdmin(), queueId, {
        status: 'sent',
        instagram_id: mediaId
      });
    }

    // Update scheduled_posts table if scheduled_post_id provided
    if (scheduled_post_id) {
      const supabase = getSupabaseAdmin();
      if (supabase) {
        await supabase
          .from('scheduled_posts')
          .update({
            status: 'published',
            instagram_media_id: mediaId,
            published_at: new Date().toISOString()
          })
          .eq('id', scheduled_post_id);
      }
    }

    // Supabase write-through: create instagram_media stub so agent can read post context immediately
    try {
      const supabase = getSupabaseAdmin();
      if (supabase && mediaId) {
        await supabase
          .from('instagram_media')
          .upsert({
            instagram_media_id: mediaId,
            business_account_id,
            media_type: type,
            caption,
            published_at: new Date().toISOString(),
            like_count: 0,
            comments_count: 0,
            reach: 0,
          }, { onConflict: 'instagram_media_id', ignoreDuplicates: false });
      }
    } catch (wtErr) {
      console.warn('⚠️ instagram_media publish write-through error:', wtErr.message);
    }

    await logApiRequest({
      endpoint: '/publish-post',
      method: 'POST',
      business_account_id,
      user_id: userId,
      success: true,
      latency
    });

    await logAudit({
      event_type: 'post_published',
      action: 'publish',
      resource_type: 'instagram_post',
      resource_id: mediaId,
      details: { caption, image_url, media_type: type, scheduled_post_id },
      success: true
    });

    res.json({ id: mediaId, job_id: queueId });

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
      endpoint: '/publish-post',
      method: 'POST',
      business_account_id,
      success: false,
      error: errorMessage,
      latency
    });

    console.error('❌ Post publish failed:', errorMessage);
    res.status(error.response?.status || 500).json({
      error: errorMessage,
      code: error.response?.data?.error?.code,
      retryable,
      error_category,
      retry_after_seconds
    });
  }
});

module.exports = router;
