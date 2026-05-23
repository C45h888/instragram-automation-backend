// backend.api/routes/frontend/inbox.js
// Frontend read endpoints for the DM inbox and comment management pages.
// All routes read from the Supabase cache populated by the agent layer —
// no live Graph API calls, no queue logic, no agent machinery.
//
// Mounted at /api/instagram via instagram-api.js (before agent-proxy).
// Consumers: useDMInbox.ts, useComments.ts
//
// Agent write endpoints (reply-dm, reply-comment, send-dm) remain in
// agent-proxy → routes/agents/engagement.js — those are write actions
// shared by both the agent and the frontend dashboard.

const express = require('express');
const router = express.Router();
const { getSupabaseAdmin } = require('../../config/supabase');

// ============================================
// GET /dm-conversations
// ============================================

/**
 * Lists DM conversations for a business account from Supabase cache.
 * Computes frontend display fields (window_remaining_hours, can_send_messages, etc.)
 * from the DB rows written by the agent's fetchAndStoreConversations().
 *
 * Consumer: useDMInbox.ts fetchConversations()
 */
router.get('/dm-conversations', async (req, res) => {
  const { business_account_id, limit } = req.query;

  if (!business_account_id) {
    return res.status(400).json({
      success: false,
      error: 'Missing required query param: business_account_id'
    });
  }

  try {
    const supabase = getSupabaseAdmin();
    const fetchLimit = Math.min(parseInt(limit) || 50, 100);

    const { data: rows, error } = await supabase
      .from('instagram_dm_conversations')
      .select('*')
      .eq('business_account_id', business_account_id)
      .order('last_message_at', { ascending: false })
      .limit(fetchLimit);

    if (error) {
      console.error('[inbox] dm-conversations query error:', error.message);
      return res.status(500).json({ success: false, error: error.message });
    }

    const nowMs = Date.now();
    const data = (rows || []).map(row => {
      const expiresMs = row.window_expires_at
        ? new Date(row.window_expires_at).getTime()
        : null;
      const hoursRemaining = expiresMs && expiresMs > nowMs
        ? (expiresMs - nowMs) / (1000 * 60 * 60)
        : 0;

      return {
        ...row,
        // Override UUID with thread ID — hook passes this as conversation_id to /reply-dm
        id: row.instagram_thread_id,
        window_remaining_hours: parseFloat(hoursRemaining.toFixed(1)),
        can_send_messages: row.within_window || false,
        requires_template: !(row.within_window || false),
        priority: 'normal',
      };
    });

    res.json({
      success: true,
      data,
      meta: { count: data.length }
    });

  } catch (err) {
    console.error('[inbox] dm-conversations error:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ============================================
// GET /dm-messages
// ============================================

/**
 * Fetches messages for a single DM conversation from Supabase cache.
 * Resolves instagram_thread_id → UUID before querying instagram_dm_messages.
 *
 * Consumer: useDMInbox.ts fetchMessages()
 */
router.get('/dm-messages', async (req, res) => {
  const { business_account_id, conversation_id, limit } = req.query;

  if (!business_account_id || !conversation_id) {
    return res.status(400).json({
      success: false,
      error: 'Missing required query params: business_account_id, conversation_id'
    });
  }

  try {
    const supabase = getSupabaseAdmin();
    const fetchLimit = Math.min(parseInt(limit) || 50, 200);

    // Resolve instagram_thread_id → Supabase UUID
    const { data: conv, error: convErr } = await supabase
      .from('instagram_dm_conversations')
      .select('id')
      .eq('instagram_thread_id', conversation_id)
      .eq('business_account_id', business_account_id)
      .maybeSingle();

    if (convErr) {
      console.error('[inbox] dm-messages conversation lookup error:', convErr.message);
      return res.status(500).json({ success: false, error: convErr.message });
    }

    if (!conv) {
      // Conversation not yet synced to DB — agent hasn't fetched it yet
      return res.json({ success: true, data: [], meta: { count: 0 } });
    }

    const { data: messages, error: msgErr } = await supabase
      .from('instagram_dm_messages')
      .select('*')
      .eq('conversation_id', conv.id)
      .order('sent_at', { ascending: true })
      .limit(fetchLimit);

    if (msgErr) {
      console.error('[inbox] dm-messages query error:', msgErr.message);
      return res.status(500).json({ success: false, error: msgErr.message });
    }

    res.json({
      success: true,
      data: messages || [],
      meta: { count: (messages || []).length }
    });

  } catch (err) {
    console.error('[inbox] dm-messages error:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ============================================
// GET /comments
// ============================================

/**
 * Fetches comments for a specific media post from Supabase cache.
 * Resolves instagram_media_id → UUID before querying instagram_comments.
 *
 * Consumer: useComments.ts fetchComments()
 */
router.get('/comments', async (req, res) => {
  const { business_account_id, media_id, limit } = req.query;

  if (!business_account_id || !media_id) {
    return res.status(400).json({
      success: false,
      error: 'Missing required query params: business_account_id, media_id'
    });
  }

  try {
    const supabase = getSupabaseAdmin();
    const fetchLimit = Math.min(parseInt(limit) || 100, 200);

    // Resolve instagram_media_id → Supabase UUID
    const { data: media, error: mediaErr } = await supabase
      .from('instagram_media')
      .select('id')
      .eq('instagram_media_id', media_id)
      .eq('business_account_id', business_account_id)
      .maybeSingle();

    if (mediaErr) {
      console.error('[inbox] comments media lookup error:', mediaErr.message);
      return res.status(500).json({ success: false, error: mediaErr.message });
    }

    if (!media) {
      // Media not yet synced to DB
      return res.json({ success: true, data: [], meta: { count: 0 } });
    }

    const { data: comments, error: commErr } = await supabase
      .from('instagram_comments')
      .select('*')
      .eq('media_id', media.id)
      .order('created_at', { ascending: false })
      .limit(fetchLimit);

    if (commErr) {
      console.error('[inbox] comments query error:', commErr.message);
      return res.status(500).json({ success: false, error: commErr.message });
    }

    res.json({
      success: true,
      data: comments || [],
      meta: { count: (comments || []).length }
    });

  } catch (err) {
    console.error('[inbox] comments error:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

module.exports = router;
