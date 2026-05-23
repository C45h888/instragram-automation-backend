// backend.api/routes/agents/queue.js
// Post queue retry endpoint for the dashboard.
// Route: POST /post-queue/retry
// Auth: Supabase JWT (Authorization: Bearer <session.access_token>)
//
// Status and DLQ reads have moved to the frontend via direct Supabase queries
// (authenticated SELECT policy: post_queue_authenticated_select_policy).
// Only the retry mutation stays here because it requires service_role to UPDATE.

const express = require('express');
const router = express.Router();
const { getSupabaseAdmin } = require('../../config/supabase');

// ============================================
// POST /post-queue/retry
// ============================================

/**
 * Resets a single dlq or failed row back to 'pending' for immediate cron pickup.
 * Verifies the requesting user owns the business account the row belongs to.
 * Body: { queue_id: "<uuid>" }
 */
router.post('/post-queue/retry', async (req, res) => {
  // --- Supabase JWT auth ---
  const authHeader = req.headers['authorization'];
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return res.status(401).json({ error: 'Missing authorization header', code: 'MISSING_AUTH' });
  }

  const token = authHeader.slice(7);
  const supabase = getSupabaseAdmin();
  if (!supabase) return res.status(503).json({ error: 'Database unavailable' });

  const { data: { user }, error: authErr } = await supabase.auth.getUser(token);
  if (authErr || !user) {
    return res.status(401).json({ error: 'Invalid or expired session', code: 'INVALID_SESSION' });
  }

  const { queue_id } = req.body;
  if (!queue_id) {
    return res.status(400).json({ error: 'Missing required field: queue_id' });
  }

  try {
    // Fetch the row and verify ownership in one round-trip
    const { data: current } = await supabase
      .from('post_queue')
      .select('id, status, action_type, instagram_id, business_account_id')
      .eq('id', queue_id)
      .single();

    if (!current) {
      return res.status(404).json({ error: 'Queue row not found' });
    }

    // Confirm the row's business account belongs to the authenticated user
    const { data: account } = await supabase
      .from('instagram_business_accounts')
      .select('id')
      .eq('id', current.business_account_id)
      .eq('user_id', user.id)
      .maybeSingle();

    if (!account) {
      return res.status(403).json({ error: 'Access denied to this queue item', code: 'FORBIDDEN' });
    }

    if (current.status === 'sent') {
      return res.status(409).json({
        error: 'Row already sent — retry not allowed',
        action_type: current.action_type,
        instagram_id: current.instagram_id,
      });
    }

    if (!['dlq', 'failed'].includes(current.status)) {
      return res.status(409).json({
        error: `Row is in status '${current.status}' — only dlq/failed rows can be retried`,
      });
    }

    const { data, error } = await supabase
      .from('post_queue')
      .update({ status: 'pending', next_retry_at: null, error: null })
      .eq('id', queue_id)
      .select('id, action_type, retry_count')
      .single();

    if (error || !data) {
      return res.status(500).json({ error: 'Failed to reset queue row' });
    }

    res.json({
      success: true,
      queue_id: data.id,
      action_type: data.action_type,
      previous_retry_count: data.retry_count,
      message: 'Row reset to pending — will be picked up on next cron tick',
    });

  } catch (err) {
    console.error('❌ /post-queue/retry failed:', err.message);
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
