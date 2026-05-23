// backend.api/routes/agents/engagement.js
// Engagement Monitor endpoints: /post-comments, /conversations, /conversation-messages,
//                               /reply-comment, /reply-dm, /send-dm

const express = require('express');
const router = express.Router();
const axios = require('axios');
const { getSupabaseAdmin, logApiRequest, logAudit } = require('../../config/supabase');
const {
  resolveAccountCredentials,
  ensureMediaRecord,
  categorizeIgError,
  logDataBusEvent,
  GRAPH_API_BASE,
  buildIdempotencyKey,
  insertQueueRow,
  updateQueueRow,
} = require('../../helpers/agent-helpers');
const {
  fetchAndStoreComments,
  fetchAndStoreConversations,
  fetchAndStoreMessages,
} = require('../../helpers/data-fetchers/messaging-fetchers');

// ============================================
// ENDPOINT 6: POST /reply-comment (Engagement Monitor)
// ============================================

/**
 * Replies to an Instagram comment.
 * Used by: Engagement monitor (scheduler/engagement_monitor.py via automation_tools.py)
 */
router.post('/reply-comment', async (req, res) => {
  const startTime = Date.now();
  const { business_account_id, comment_id, reply_text, post_id } = req.body;
  const fallbackEnabled = process.env.POST_FALLBACK_ENABLED === 'true';

  try {
    if (!business_account_id || !comment_id || !reply_text) {
      return res.status(400).json({
        error: 'Missing required fields: business_account_id, comment_id, reply_text'
      });
    }

    if (!/^\d+$/.test(String(comment_id))) {
      return res.status(400).json({ error: 'Invalid comment_id format' });
    }

    if (reply_text.length > 2200) {
      return res.status(400).json({ error: 'reply_text exceeds 2200 character limit' });
    }

    // --- Queue: pre-log intent before outgoing IG call ---
    let queueId = null;
    if (fallbackEnabled) {
      const supabase = getSupabaseAdmin();
      if (supabase) {
        queueId = await insertQueueRow(supabase, {
          business_account_id,
          action_type: 'reply_comment',
          payload: { comment_id, reply_text, post_id: post_id || null },
          idempotency_key: buildIdempotencyKey(`reply_comment:${comment_id}`)
        });
      }
    }

    const { igUserId, pageToken, userId } = await resolveAccountCredentials(business_account_id);

    const replyRes = await axios.post(`${GRAPH_API_BASE}/${comment_id}/replies`, null, {
      params: {
        message: reply_text.trim(),
        access_token: pageToken
      },
      timeout: 10000
    });

    const latency = Date.now() - startTime;

    // --- Queue: mark sent ---
    if (queueId) {
      await updateQueueRow(getSupabaseAdmin(), queueId, {
        status: 'sent',
        instagram_id: replyRes.data.id
      });
    }

    await logApiRequest({
      endpoint: '/reply-comment',
      method: 'POST',
      business_account_id,
      user_id: userId,
      success: true,
      latency
    });

    await logAudit({
      event_type: 'comment_reply_sent',
      action: 'reply',
      resource_type: 'instagram_comment',
      resource_id: replyRes.data.id,
      details: { comment_id, post_id, reply_text },
      success: true
    });

    // Supabase write-through: log outgoing comment reply
    try {
      const supabase = getSupabaseAdmin();
      if (supabase && replyRes.data.id) {
        const mediaUUID = post_id
          ? await ensureMediaRecord(supabase, post_id, business_account_id)
          : null;
        await supabase
          .from('instagram_comments')
          .upsert({
            instagram_comment_id: replyRes.data.id,
            text: reply_text.trim(),
            author_username: null,
            author_instagram_id: igUserId,
            media_id: mediaUUID,
            business_account_id,
            created_at: new Date().toISOString(),
            like_count: 0,
            processed_by_automation: true,
          }, { onConflict: 'instagram_comment_id', ignoreDuplicates: false });
      }
    } catch (wtErr) {
      console.warn('⚠️ Comment reply write-through error:', wtErr.message);
    }

    res.json({ success: true, id: replyRes.data.id, job_id: queueId });

  } catch (error) {
    const latency = Date.now() - startTime;
    const errorMessage = error.response?.data?.error?.message || error.message;
    const { retryable, error_category, retry_after_seconds } = categorizeIgError(error);

    // --- Queue: mark failed or dlq ---
    if (fallbackEnabled && queueId) {
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
      endpoint: '/reply-comment',
      method: 'POST',
      business_account_id,
      success: false,
      error: errorMessage,
      latency
    });

    console.error('❌ Comment reply failed:', errorMessage);
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
// ENDPOINT 7: POST /reply-dm (Engagement Monitor)
// ============================================

/**
 * Sends a reply into an existing DM conversation.
 * Used by: Engagement monitor (scheduler/engagement_monitor.py via automation_tools.py)
 */
router.post('/reply-dm', async (req, res) => {
  const startTime = Date.now();
  const { business_account_id, conversation_id, recipient_id, message_text } = req.body;
  const fallbackEnabled = process.env.POST_FALLBACK_ENABLED === 'true';

  try {
    if (!business_account_id || !conversation_id || !message_text) {
      return res.status(400).json({
        error: 'Missing required fields: business_account_id, conversation_id, message_text'
      });
    }

    if (!/^[\w-]+$/.test(String(conversation_id))) {
      return res.status(400).json({ error: 'Invalid conversation_id format' });
    }

    if (message_text.length > 1000) {
      return res.status(400).json({ error: 'message_text exceeds 1000 character limit' });
    }

    // --- Queue: pre-log intent ---
    let queueId = null;
    if (fallbackEnabled) {
      const supabase = getSupabaseAdmin();
      if (supabase) {
        const msgHash = buildIdempotencyKey(message_text).slice(0, 8);
        queueId = await insertQueueRow(supabase, {
          business_account_id,
          action_type: 'reply_dm',
          payload: { conversation_id, recipient_id: recipient_id || null, message_text },
          idempotency_key: buildIdempotencyKey(`reply_dm:${conversation_id}:${msgHash}`)
        });
      }
    }

    const { pageToken, userId, igUsername } = await resolveAccountCredentials(business_account_id);

    const dmRes = await axios.post(`${GRAPH_API_BASE}/${conversation_id}/messages`, null, {
      params: {
        message: message_text.trim(),
        access_token: pageToken
      },
      timeout: 10000
    });

    const latency = Date.now() - startTime;

    // --- Queue: mark sent ---
    if (queueId) {
      await updateQueueRow(getSupabaseAdmin(), queueId, {
        status: 'sent',
        instagram_id: dmRes.data.id
      });
    }

    await logApiRequest({
      endpoint: '/reply-dm',
      method: 'POST',
      business_account_id,
      user_id: userId,
      success: true,
      latency
    });

    await logAudit({
      event_type: 'dm_reply_sent',
      action: 'reply',
      resource_type: 'instagram_dm',
      resource_id: dmRes.data.id,
      details: { conversation_id, recipient_id },
      success: true
    });

    // Supabase write-through: log outgoing DM reply for conversation history
    try {
      const supabase = getSupabaseAdmin();
      if (supabase && dmRes.data.id) {
        // Resolve IG thread ID → Supabase UUID for conversation FK
        let conversationUUID = null;
        if (conversation_id) {
          const { data: conv } = await supabase
            .from('instagram_dm_conversations')
            .select('id')
            .eq('instagram_thread_id', conversation_id)
            .maybeSingle();
          conversationUUID = conv?.id || null;
        }

        const { error: msgErr } = await supabase
          .from('instagram_dm_messages')
          .upsert({
            instagram_message_id: dmRes.data.id,
            message_text: message_text.trim(),
            conversation_id: conversationUUID,
            business_account_id,
            is_from_business: true,
            sender_username: igUsername || null,
            recipient_instagram_id: recipient_id || '',
            sent_at: new Date().toISOString(),
            send_status: 'sent',
          }, { onConflict: 'instagram_message_id', ignoreDuplicates: false });
        if (msgErr) console.warn('⚠️ DM reply write-through failed:', msgErr.message);
      }
    } catch (wtErr) {
      console.warn('⚠️ DM reply write-through error:', wtErr.message);
    }

    res.json({ success: true, id: dmRes.data.id, job_id: queueId });

  } catch (error) {
    const latency = Date.now() - startTime;
    const errorMessage = error.response?.data?.error?.message || error.message;
    const { retryable, error_category, retry_after_seconds } = categorizeIgError(error);

    // --- Queue: mark failed or dlq ---
    if (fallbackEnabled && queueId) {
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
      endpoint: '/reply-dm',
      method: 'POST',
      business_account_id,
      success: false,
      error: errorMessage,
      latency
    });

    console.error('❌ DM reply failed:', errorMessage);
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
// ENDPOINT 9: GET /post-comments (Engagement Monitor)
// ============================================

/**
 * Fetches live comments for a specific Instagram media post.
 * Used by: Engagement monitor to read fresh comments before deciding to reply.
 * Delegates to fetchAndStoreComments() for Graph API + Supabase logic.
 */
router.get('/post-comments', async (req, res) => {
  const { business_account_id, media_id, limit } = req.query;

  // HTTP validation
  if (!business_account_id || !media_id) {
    return res.status(400).json({
      error: 'Missing required query params: business_account_id, media_id'
    });
  }

  if (!/^\d+$/.test(String(media_id))) {
    return res.status(400).json({ error: 'Invalid media_id format' });
  }

  const result = await fetchAndStoreComments(business_account_id, media_id, limit);

  if (!result.success) {
    return res.status(500).json({
      error: result.error,
      code: result.code,
      retryable: result.retryable,
      error_category: result.error_category,
      retry_after_seconds: result.retry_after_seconds
    });
  }

  res.json({
    success: true,
    data: result.comments,
    paging: result.paging,
    meta: { count: result.count }
  });
});

// ============================================
// ENDPOINT 10: GET /conversations (Engagement Monitor)
// ============================================

/**
 * Lists active DM conversations with 24-hour messaging window status.
 * Used by: Engagement monitor to find conversations eligible for replies.
 * Delegates to fetchAndStoreConversations() for Graph API + Supabase logic.
 */
router.get('/conversations', async (req, res) => {
  const { business_account_id, limit } = req.query;

  if (!business_account_id) {
    return res.status(400).json({
      error: 'Missing required query param: business_account_id'
    });
  }

  const result = await fetchAndStoreConversations(business_account_id, limit);

  if (!result.success) {
    return res.status(500).json({
      error: result.error,
      code: result.code,
      retryable: result.retryable,
      error_category: result.error_category,
      retry_after_seconds: result.retry_after_seconds
    });
  }

  res.json({
    success: true,
    data: result.conversations,
    paging: result.paging,
    meta: { count: result.count }
  });
});

// ============================================
// ENDPOINT 11: GET /conversation-messages (Engagement Monitor)
// ============================================

/**
 * Fetches messages for a specific DM conversation.
 * Used by: Engagement monitor to read thread history before crafting a reply.
 * Delegates to fetchAndStoreMessages() for Graph API + Supabase logic.
 */
router.get('/conversation-messages', async (req, res) => {
  const { business_account_id, conversation_id, limit } = req.query;

  if (!business_account_id || !conversation_id) {
    return res.status(400).json({
      error: 'Missing required query params: business_account_id, conversation_id'
    });
  }

  if (!/^[\w-]+$/.test(String(conversation_id))) {
    return res.status(400).json({ error: 'Invalid conversation_id format' });
  }

  const result = await fetchAndStoreMessages(business_account_id, conversation_id, limit);

  if (!result.success) {
    return res.status(500).json({
      error: result.error,
      code: result.code,
      retryable: result.retryable,
      error_category: result.error_category,
      retry_after_seconds: result.retry_after_seconds
    });
  }

  // Query-back: return DB-shaped rows so the frontend/agent gets status fields
  // (send_status, message_type, is_from_business, etc.). This is route-layer concern —
  // the shim only transforms raw messages; shaped rows live in DB after storeMessageBatches.
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
          return res.json({
            success: true,
            data: rows,
            paging: result.paging,
            meta: { count: rows.length }
          });
        }
      }
    }
  } catch (qbErr) {
    // Query-back failure is non-fatal — fall through to shim-transformed messages
    console.warn('[engagement] conversation-messages query-back failed:', qbErr.message);
  }

  // Fallback: return shim-transformed messages (correct shape, no DB status fields)
  res.json({
    success: true,
    data: result.messages,
    paging: result.paging,
    meta: { count: result.count }
  });
});

// ============================================
// ENDPOINT: POST /send-dm (UGC Permission DM)
// ============================================

/**
 * Sends a new DM to a user (initiates conversation, not a reply).
 * Used by: UGC pipeline (ugc_tools.send_permission_dm) to request creator repost permission.
 *
 * Agent payload: { business_account_id, recipient_id, recipient_username, message_text }
 *   recipient_id      — numeric IGSID of the creator to message
 *   recipient_username — for audit context only, not sent to Graph API
 */
router.post('/send-dm', async (req, res) => {
  const startTime = Date.now();
  const { business_account_id, recipient_id, recipient_username, message_text } = req.body;
  const fallbackEnabled = process.env.POST_FALLBACK_ENABLED === 'true';

  try {
    if (!business_account_id || !recipient_id || !message_text) {
      return res.status(400).json({
        error: 'Missing required fields: business_account_id, recipient_id, message_text'
      });
    }

    if (!/^\d+$/.test(String(recipient_id))) {
      return res.status(400).json({ error: 'Invalid recipient_id: must be a numeric IGSID' });
    }

    if (message_text.length > 1000) {
      return res.status(400).json({ error: 'message_text exceeds 1000 character limit' });
    }

    // --- Queue: pre-log intent ---
    let queueId = null;
    if (fallbackEnabled) {
      const supabase = getSupabaseAdmin();
      if (supabase) {
        const msgHash = buildIdempotencyKey(message_text).slice(0, 8);
        queueId = await insertQueueRow(supabase, {
          business_account_id,
          action_type: 'send_dm',
          payload: { recipient_id, recipient_username: recipient_username || null, message_text },
          idempotency_key: buildIdempotencyKey(`send_dm:${recipient_id}:${msgHash}`)
        });
      }
    }

    const { igUserId, pageToken, userId, pageId, igUsername } = await resolveAccountCredentials(business_account_id);

    // Meta docs: POST /{page-id}/messages — pageId = Facebook Page ID, NOT igUserId
    const dmNode = pageId || igUserId;
    const dmRes = await axios.post(`${GRAPH_API_BASE}/${dmNode}/messages`, {
      recipient: { id: String(recipient_id) },
      message: { text: message_text.trim() }
    }, {
      params: { access_token: pageToken },
      timeout: 10000
    });

    const latency = Date.now() - startTime;

    // --- Queue: mark sent ---
    if (queueId) {
      await updateQueueRow(getSupabaseAdmin(), queueId, {
        status: 'sent',
        instagram_id: dmRes.data.message_id || dmRes.data.id
      });
    }

    await logApiRequest({
      endpoint: '/send-dm',
      method: 'POST',
      business_account_id,
      user_id: userId,
      success: true,
      latency
    });

    await logAudit({
      event_type: 'dm_sent',
      action: 'send',
      resource_type: 'instagram_dm',
      resource_id: dmRes.data.message_id || dmRes.data.id,
      details: { recipient_id, recipient_username },
      success: true
    });

    // Supabase write-through: log outgoing DM (no conversation_id yet for new threads)
    try {
      const supabase = getSupabaseAdmin();
      if (supabase && (dmRes.data.message_id || dmRes.data.id)) {
        const { error: msgErr } = await supabase
          .from('instagram_dm_messages')
          .upsert({
            instagram_message_id: dmRes.data.message_id || dmRes.data.id,
            message_text: message_text.trim(),
            conversation_id: null,
            business_account_id,
            is_from_business: true,
            recipient_instagram_id: String(recipient_id),
            sent_at: new Date().toISOString(),
            send_status: 'sent',
            sender_username: igUsername || null,
          }, { onConflict: 'instagram_message_id', ignoreDuplicates: false });
        if (msgErr) console.warn('⚠️ Send-DM write-through failed:', msgErr.message);
      }
    } catch (wtErr) {
      console.warn('⚠️ Send-DM write-through error:', wtErr.message);
    }

    res.json({ success: true, message_id: dmRes.data.message_id || dmRes.data.id, job_id: queueId });

  } catch (error) {
    const latency = Date.now() - startTime;
    const errorMessage = error.response?.data?.error?.message || error.message;
    const { retryable, error_category, retry_after_seconds } = categorizeIgError(error);

    // --- Queue: mark failed or dlq ---
    if (fallbackEnabled && queueId) {
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

    logDataBusEvent('messaging', 'post_to_dm_failed', {
      payload_keys: Object.keys(req.body),
      media_url_present: !!req.body?.image_url,
      recipient_present: !!req.body?.recipient_id,
      error: errorMessage,
      success: false,
      account_id: business_account_id,
    }).catch(() => {});

    await logApiRequest({
      endpoint: '/send-dm',
      method: 'POST',
      business_account_id,
      success: false,
      error: errorMessage,
      latency
    });

    console.error('❌ Send DM failed:', errorMessage);
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
// INTERNAL: resolve active queue row id from idempotency key
module.exports = router;
