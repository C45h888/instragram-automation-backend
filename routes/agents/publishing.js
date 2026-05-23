// backend.api/routes/agents/publishing.js
// Content Scheduler endpoints: /publish-post
//
// Architecture: routes emit intents to Redis only (no direct IG API calls).
// The publish-worker consumes intents, executes via IG API, and updates post_queue.
//
// Flow: route → Redis (supervisor:acquisitions:publish:media:{account_id})
//                → publish-worker BRPOP → IG API → post_queue/scheduled_posts

const express = require('express');
const router = express.Router();
const crypto = require('crypto');
const { getSupabaseAdmin, logAudit, logApiRequest } = require('../../config/supabase');
const { getRedisClient } = require('../../config/redis');
const { buildIdempotencyKey } = require('../../helpers/agent-helpers');

// ============================================
// POST /publish-post (Content Scheduler)
// ============================================

/**
 * Queues a post for Instagram publishing via the governed publish-worker.
 * Emits a publish intent to Redis and marks scheduled_posts as 'publishing'.
 *
 * Body: { business_account_id, image_url, caption, media_type, scheduled_post_id? }
 */
router.post('/publish-post', async (req, res) => {
  const startTime = Date.now();
  const { business_account_id, image_url, caption, media_type, scheduled_post_id } = req.body;

  try {
    if (!business_account_id || !image_url || !caption) {
      return res.status(400).json({
        error: 'Missing required fields: business_account_id, image_url, caption'
      });
    }

    const redis = getRedisClient();
    if (!redis || redis.status !== 'ready') {
      return res.status(503).json({ error: 'Redis unavailable' });
    }

    const type = (media_type || 'IMAGE').toUpperCase();

    // ── 1. Insert into post_queue for retry tracking ─────────────────────
    const supabase = getSupabaseAdmin();
    let queueId = null;

    if (supabase) {
      const idemSeed = scheduled_post_id
        ? `publish_post:${scheduled_post_id}`
        : `publish_post:${buildIdempotencyKey(image_url).slice(0, 16)}`;

      const { data: queueRow, error: queueErr } = await supabase
        .from('post_queue')
        .insert({
          business_account_id,
          action_type: 'publish_post',
          payload: { image_url, caption, media_type: type, scheduled_post_id: scheduled_post_id || null },
          idempotency_key: buildIdempotencyKey(idemSeed),
          status: 'pending',
        })
        .select('id')
        .single();

      if (queueErr) {
        console.warn('[publish:route] post_queue insert failed:', queueErr.message);
      } else {
        queueId = queueRow.id;
      }
    }

    // ── 2. Mark scheduled_posts as publishing (prevents db-worker re-scan) ─
    if (scheduled_post_id && supabase) {
      await supabase
        .from('scheduled_posts')
        .update({ status: 'publishing' })
        .eq('id', scheduled_post_id)
        .eq('status', 'approved'); // only if not already picked up
    }

    // ── 3. Emit intent to Redis ─────────────────────────────────────────
    const intent_id = crypto.randomUUID();
    const intent = {
      intent_id,
      account_id: business_account_id,
      fetch_type: 'publish_media',
      action_type: 'publish_post',
      payload: {
        image_url,
        caption,
        media_type: type,
        scheduled_post_id: scheduled_post_id || null,
      },
      priority: 'normal',
      issued_at: new Date().toISOString(),
      queue_row_id: queueId,
      scheduled_post_id: scheduled_post_id || null,
    };

    const queueKey = `supervisor:acquisitions:publish:media:${business_account_id}`;
    await redis.lpush(queueKey, JSON.stringify(intent));

    const latency = Date.now() - startTime;

    await logApiRequest({
      endpoint: '/publish-post',
      method: 'POST',
      business_account_id,
      success: true,
      latency,
      details: { intent_id, queued: true },
    });

    await logAudit({
      event_type: 'post_queued',
      action: 'publish',
      resource_type: 'instagram_post',
      resource_id: null,
      details: { image_url, caption, media_type: type, scheduled_post_id, intent_id },
      success: true
    });

    res.json({ queued: true, intent_id, job_id: queueId });

  } catch (error) {
    const latency = Date.now() - startTime;
    const errorMessage = error.message;

    await logApiRequest({
      endpoint: '/publish-post',
      method: 'POST',
      business_account_id,
      success: false,
      error: errorMessage,
      latency
    });

    console.error('❌ /publish-post queue error:', errorMessage);
    res.status(500).json({ error: errorMessage });
  }
});

module.exports = router;
