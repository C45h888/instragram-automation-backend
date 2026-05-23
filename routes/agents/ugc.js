// backend.api/routes/agents/ugc.js
// UGC Discovery endpoints: /search-hashtag, /tags, /repost-ugc, /sync-ugc
//
// Architecture: /repost-ugc emits intent to Redis only (no direct IG API calls).
// The publish-worker (ugc domain) consumes intents, executes via IG API,
// and updates post_queue + ugc_permissions.
//
// Flow: route → Redis (supervisor:acquisitions:publish:ugc:{account_id})
//                → publish-worker BRPOP → IG API → post_queue/ugc_permissions
//
// /search-hashtag and /tags are read-only acquisition (unchanged).

const express = require('express');
const router = express.Router();
const crypto = require('crypto');
const { getSupabaseAdmin, logAudit, logApiRequest } = require('../../config/supabase');
const { getRedisClient } = require('../../config/redis');
const { buildIdempotencyKey } = require('../../helpers/agent-helpers');
const {
  fetchAndStoreHashtagMedia,
  fetchAndStoreTaggedMedia,
} = require('../../helpers/data-fetchers/ugc-fetchers');

// ============================================
// POST /search-hashtag (UGC Discovery - read-only)
// ============================================

router.post('/search-hashtag', async (req, res) => {
  const { business_account_id, hashtag, limit } = req.body;
  if (!business_account_id || !hashtag) {
    return res.status(400).json({ error: 'Missing required fields: business_account_id, hashtag' });
  }
  const result = await fetchAndStoreHashtagMedia(business_account_id, hashtag, limit);
  if (!result.success) {
    const isNotFound = result.error && result.error.includes('Hashtag not found');
    return res.status(isNotFound ? 404 : 500).json({ error: result.error, code: result.code });
  }
  res.json({ recent_media: result.media, data: result.media });
});

// ============================================
// GET /tags (UGC Discovery - read-only)
// ============================================

router.get('/tags', async (req, res) => {
  const { business_account_id, limit } = req.query;
  if (!business_account_id) {
    return res.status(400).json({ error: 'Missing required query parameter: business_account_id' });
  }
  const result = await fetchAndStoreTaggedMedia(business_account_id, limit);
  if (!result.success) {
    return res.status(500).json({ error: result.error, code: result.code });
  }
  res.json({ tagged_posts: result.taggedPosts, data: result.taggedPosts });
});

// ============================================
// POST /sync-ugc (UGC Discovery - read-only)
// ============================================

router.post('/sync-ugc', async (req, res) => {
  const { business_account_id } = req.body;
  if (!business_account_id) {
    return res.status(400).json({ error: 'Missing required field: business_account_id' });
  }
  const result = await fetchAndStoreTaggedMedia(business_account_id, 50);
  if (!result.success) {
    return res.status(500).json({ error: result.error, code: result.code });
  }
  res.json({ success: true, synced_count: result.count });
});

// ============================================
// POST /repost-ugc (UGC Publishing - intent emission)
// ============================================

/**
 * Queues a UGC repost via publish-worker (ugc domain).
 * Verifies permission is 'granted' before queueing.
 */
router.post('/repost-ugc', async (req, res) => {
  const startTime = Date.now();
  const { business_account_id, permission_id } = req.body;

  try {
    if (!business_account_id || !permission_id) {
      return res.status(400).json({ error: 'Missing required fields: business_account_id, permission_id' });
    }

    const redis = getRedisClient();
    if (!redis || redis.status !== 'ready') {
      return res.status(503).json({ error: 'Redis unavailable' });
    }

    const supabase = getSupabaseAdmin();

    // ── Precondition check: permission must exist and be 'granted' ───────
    // These checks are preconditions, not retryable failures. Route rejects before queueing.
    const { data: permission, error: permError } = await supabase
      .from('ugc_permissions')
      .select('id, ugc_content_id, status, business_account_id')
      .eq('id', permission_id)
      .eq('business_account_id', business_account_id)
      .single();

    if (permError || !permission) {
      return res.status(404).json({ error: 'Permission record not found', code: 'PERMISSION_NOT_FOUND' });
    }

    if (permission.status !== 'granted') {
      return res.status(403).json({
        error: 'Cannot repost: permission not granted by content creator',
        code: 'PERMISSION_DENIED',
        details: { current_status: permission.status }
      });
    }

    // ── Insert post_queue row for retry tracking ───────────────────────
    let queueId = null;
    if (supabase) {
      const { data: queueRow, error: queueErr } = await supabase
        .from('post_queue')
        .insert({
          business_account_id,
          action_type: 'repost_ugc',
          payload: { permission_id },
          idempotency_key: buildIdempotencyKey(`repost_ugc:${permission_id}`),
          status: 'pending',
        })
        .select('id')
        .single();

      if (queueErr) {
        console.warn('[ugc:route] post_queue insert failed:', queueErr.message);
      } else {
        queueId = queueRow.id;
      }
    }

    // ── Emit intent to Redis ─────────────────────────────────────────
    const intent_id = crypto.randomUUID();
    const intent = {
      intent_id,
      account_id: business_account_id,
      fetch_type: 'publish_ugc',
      action_type: 'repost_ugc',
      payload: { permission_id },
      priority: 'normal',
      issued_at: new Date().toISOString(),
      queue_row_id: queueId,
    };

    const queueKey = `supervisor:acquisitions:publish:ugc:${business_account_id}`;
    await redis.lpush(queueKey, JSON.stringify(intent));

    const latency = Date.now() - startTime;

    await logApiRequest({
      endpoint: '/repost-ugc', method: 'POST',
      business_account_id, success: true, latency,
      details: { intent_id, queued: true, permission_id },
    });

    await logAudit({
      event_type: 'ugc_repost_queued',
      action: 'repost',
      resource_type: 'ugc_permissions',
      resource_id: permission_id,
      details: { permission_id, ugc_content_id: permission.ugc_content_id, intent_id },
      success: true
    });

    res.json({ queued: true, intent_id, job_id: queueId });

  } catch (error) {
    const latency = Date.now() - startTime;
    const errorMessage = error.message;

    await logApiRequest({
      endpoint: '/repost-ugc', method: 'POST',
      business_account_id, success: false, error: errorMessage, latency,
    });

    console.error('❌ /repost-ugc queue error:', errorMessage);
    res.status(500).json({ error: errorMessage });
  }
});

module.exports = router;
