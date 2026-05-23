// backend.api/routes/agents/engagement.js
// Engagement Monitor endpoints: /reply-comment, /reply-dm, /send-dm
//
// Architecture: routes emit intents to Redis only (no direct IG API calls).
// The publish-worker (messaging domain) consumes intents, executes via IG API,
// and updates post_queue for retry tracking.
//
// Flow: route → Redis (supervisor:acquisitions:publish:messaging:{account_id})
//                → publish-worker BRPOP → IG API → post_queue

const express = require('express');
const router = express.Router();
const crypto = require('crypto');
const { getSupabaseAdmin, logApiRequest, logAudit } = require('../../config/supabase');
const { getRedisClient } = require('../../config/redis');
const { buildIdempotencyKey } = require('../../helpers/agent-helpers');

// ── Shared intent emitter ─────────────────────────────────────────────────────

async function emitMessagingIntent(businessAccountId, actionType, payload) {
  const redis = getRedisClient();
  const supabase = getSupabaseAdmin();

  // Insert retry queue row
  let queueId = null;
  if (supabase) {
    const idemSeed = `${actionType}:${payload.comment_id || payload.conversation_id || payload.recipient_id}`;
    const { data: queueRow, error: queueErr } = await supabase
      .from('post_queue')
      .insert({
        business_account_id: businessAccountId,
        action_type: actionType,
        payload,
        idempotency_key: buildIdempotencyKey(idemSeed),
        status: 'pending',
      })
      .select('id')
      .single();

    if (queueErr) {
      console.warn(`[engagement:route] post_queue insert failed (${actionType}):`, queueErr.message);
    } else {
      queueId = queueRow.id;
    }
  }

  // Emit to Redis
  const intent_id = crypto.randomUUID();
  const intent = {
    intent_id,
    account_id: businessAccountId,
    fetch_type: 'publish_messaging',
    action_type: actionType,
    payload,
    priority: 'normal',
    issued_at: new Date().toISOString(),
    queue_row_id: queueId,
  };

  const queueKey = `supervisor:acquisitions:publish:messaging:${businessAccountId}`;
  await redis.lpush(queueKey, JSON.stringify(intent));

  return { intent_id, queue_id: queueId };
}

// ============================================
// POST /reply-comment
// ============================================

/**
 * Queues a comment reply via publish-worker (messaging domain).
 */
router.post('/reply-comment', async (req, res) => {
  const startTime = Date.now();
  const { business_account_id, comment_id, reply_text, post_id } = req.body;

  try {
    if (!business_account_id || !comment_id || !reply_text) {
      return res.status(400).json({ error: 'Missing required fields: business_account_id, comment_id, reply_text' });
    }

    if (!/^\d+$/.test(String(comment_id))) {
      return res.status(400).json({ error: 'Invalid comment_id format' });
    }

    if (reply_text.length > 2200) {
      return res.status(400).json({ error: 'reply_text exceeds 2200 character limit' });
    }

    const redis = getRedisClient();
    if (!redis || redis.status !== 'ready') {
      return res.status(503).json({ error: 'Redis unavailable' });
    }

    const { intent_id, queue_id } = await emitMessagingIntent(business_account_id, 'reply_comment', {
      comment_id, reply_text: reply_text.trim(), post_id: post_id || null,
    });

    await logApiRequest({
      endpoint: '/reply-comment', method: 'POST',
      business_account_id, success: true, latency: Date.now() - startTime,
      details: { intent_id, queued: true },
    });

    res.json({ queued: true, intent_id, job_id: queue_id });

  } catch (error) {
    const errorMessage = error.message;
    await logApiRequest({
      endpoint: '/reply-comment', method: 'POST',
      business_account_id, success: false, error: errorMessage,
      latency: Date.now() - startTime,
    });
    console.error('❌ /reply-comment queue error:', errorMessage);
    res.status(500).json({ error: errorMessage });
  }
});

// ============================================
// POST /reply-dm
// ============================================

/**
 * Queues a DM reply via publish-worker (messaging domain).
 */
router.post('/reply-dm', async (req, res) => {
  const startTime = Date.now();
  const { business_account_id, conversation_id, recipient_id, message_text } = req.body;

  try {
    if (!business_account_id || !conversation_id || !message_text) {
      return res.status(400).json({ error: 'Missing required fields: business_account_id, conversation_id, message_text' });
    }

    if (!/^[\w-]+$/.test(String(conversation_id))) {
      return res.status(400).json({ error: 'Invalid conversation_id format' });
    }

    if (message_text.length > 1000) {
      return res.status(400).json({ error: 'message_text exceeds 1000 character limit' });
    }

    const redis = getRedisClient();
    if (!redis || redis.status !== 'ready') {
      return res.status(503).json({ error: 'Redis unavailable' });
    }

    const { intent_id, queue_id } = await emitMessagingIntent(business_account_id, 'reply_dm', {
      conversation_id, recipient_id: recipient_id || null, message_text: message_text.trim(),
    });

    await logApiRequest({
      endpoint: '/reply-dm', method: 'POST',
      business_account_id, success: true, latency: Date.now() - startTime,
      details: { intent_id, queued: true },
    });

    res.json({ queued: true, intent_id, job_id: queue_id });

  } catch (error) {
    const errorMessage = error.message;
    await logApiRequest({
      endpoint: '/reply-dm', method: 'POST',
      business_account_id, success: false, error: errorMessage,
      latency: Date.now() - startTime,
    });
    console.error('❌ /reply-dm queue error:', errorMessage);
    res.status(500).json({ error: errorMessage });
  }
});

// ============================================
// POST /send-dm
// ============================================

/**
 * Queues an outbound DM (new thread) via publish-worker (messaging domain).
 */
router.post('/send-dm', async (req, res) => {
  const startTime = Date.now();
  const { business_account_id, recipient_id, recipient_username, message_text } = req.body;

  try {
    if (!business_account_id || !recipient_id || !message_text) {
      return res.status(400).json({ error: 'Missing required fields: business_account_id, recipient_id, message_text' });
    }

    if (!/^\d+$/.test(String(recipient_id))) {
      return res.status(400).json({ error: 'Invalid recipient_id: must be a numeric IGSID' });
    }

    if (message_text.length > 1000) {
      return res.status(400).json({ error: 'message_text exceeds 1000 character limit' });
    }

    const redis = getRedisClient();
    if (!redis || redis.status !== 'ready') {
      return res.status(503).json({ error: 'Redis unavailable' });
    }

    const { intent_id, queue_id } = await emitMessagingIntent(business_account_id, 'send_dm', {
      recipient_id, recipient_username: recipient_username || null, message_text: message_text.trim(),
    });

    await logApiRequest({
      endpoint: '/send-dm', method: 'POST',
      business_account_id, success: true, latency: Date.now() - startTime,
      details: { intent_id, queued: true },
    });

    await logAudit({
      event_type: 'dm_queued', action: 'send',
      resource_type: 'instagram_dm', resource_id: null,
      details: { recipient_id, intent_id },
      success: true
    });

    res.json({ queued: true, intent_id, job_id: queue_id });

  } catch (error) {
    const errorMessage = error.message;
    await logApiRequest({
      endpoint: '/send-dm', method: 'POST',
      business_account_id, success: false, error: errorMessage,
      latency: Date.now() - startTime,
    });
    console.error('❌ /send-dm queue error:', errorMessage);
    res.status(500).json({ error: errorMessage });
  }
});

// ============================================
// GET /post-comments, /conversations, /conversation-messages
// (read-only acquisition — NOT publish; remain unchanged)
// ============================================

const { fetchAndStoreComments, fetchAndStoreConversations, fetchAndStoreMessages } = require('../../helpers/data-fetchers/messaging-fetchers');

router.get('/post-comments', async (req, res) => {
  const { business_account_id, media_id, limit } = req.query;
  if (!business_account_id || !media_id) {
    return res.status(400).json({ error: 'Missing required query params: business_account_id, media_id' });
  }
  if (!/^\d+$/.test(String(media_id))) {
    return res.status(400).json({ error: 'Invalid media_id format' });
  }
  const result = await fetchAndStoreComments(business_account_id, media_id, limit);
  if (!result.success) {
    return res.status(500).json({ error: result.error, code: result.code, retryable: result.retryable });
  }
  res.json({ success: true, data: result.comments, paging: result.paging, meta: { count: result.count } });
});

router.get('/conversations', async (req, res) => {
  const { business_account_id, limit } = req.query;
  if (!business_account_id) {
    return res.status(400).json({ error: 'Missing required query param: business_account_id' });
  }
  const result = await fetchAndStoreConversations(business_account_id, limit);
  if (!result.success) {
    return res.status(500).json({ error: result.error, code: result.code, retryable: result.retryable });
  }
  res.json({ success: true, data: result.conversations, paging: result.paging, meta: { count: result.count } });
});

router.get('/conversation-messages', async (req, res) => {
  const { business_account_id, conversation_id, limit } = req.query;
  if (!business_account_id || !conversation_id) {
    return res.status(400).json({ error: 'Missing required query params: business_account_id, conversation_id' });
  }
  if (!/^[\w-]+$/.test(String(conversation_id))) {
    return res.status(400).json({ error: 'Invalid conversation_id format' });
  }
  const result = await fetchAndStoreMessages(business_account_id, conversation_id, limit);
  if (!result.success) {
    return res.status(500).json({ error: result.error, code: result.code, retryable: result.retryable });
  }
  // Query-back DB rows (unchanged behavior)
  try {
    const supabase = getSupabaseAdmin();
    if (supabase) {
      const fetchLimit = Math.min(parseInt(limit) || 20, 100);
      const { data: conv } = await supabase
        .from('instagram_dm_conversations')
        .select('id')
        .eq('instagram_thread_id', conversation_id)
        .maybeSingle();
      if (conv?.id) {
        const { data: rows } = await supabase
          .from('instagram_dm_messages')
          .select('*')
          .eq('conversation_id', conv.id)
          .order('sent_at', { ascending: true })
          .limit(fetchLimit);
        if (rows) {
          return res.json({ success: true, data: rows, paging: result.paging, meta: { count: rows.length } });
        }
      }
    }
  } catch (qbErr) {
    console.warn('[engagement] conversation-messages query-back failed:', qbErr.message);
  }
  res.json({ success: true, data: result.messages, paging: result.paging, meta: { count: result.count } });
});

module.exports = router;
