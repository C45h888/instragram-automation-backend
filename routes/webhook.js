// backend/routes/webhook.js - Meta Instagram Webhooks
// Receives Instagram events for frontend display (Path B)
// Agent receives same events directly from Meta (Path A) for automation
//
// COMPLIANCE: Law 13 / Law 14 — All agent-bound data flows through DB → Supabase Realtime.
// No direct HTTP forward to agent. All events written to audit_log + domain tables.

const express = require('express');
const router = express.Router();
const crypto = require('crypto');
const { getSupabaseAdmin, logAudit } = require('../config/supabase');
const { ensureMediaRecord } = require('../helpers/agent-helpers');

// ============================================
// META WEBHOOK SIGNATURE VERIFICATION
// ============================================

/**
 * Verifies Meta webhook signature using HMAC-SHA256
 * MANDATORY for Meta compliance - prevents spoofed events
 * @see https://developers.facebook.com/docs/messenger-platform/webhooks#security
 */
function verifyMetaWebhookSignature(req, res, next) {
  const signature = req.headers['x-hub-signature-256'];

  if (!signature) {
    console.warn('⚠️ Webhook received without signature - rejecting');
    return res.status(401).send('Missing signature');
  }

  // Use raw body if available (for accurate signature verification)
  const body = req.rawBody || JSON.stringify(req.body);

  const hmac = crypto.createHmac('sha256', process.env.META_APP_SECRET || '');
  hmac.update(body);
  const expectedSignature = `sha256=${hmac.digest('hex')}`;

  // Use timing-safe comparison to prevent timing attacks
  try {
    if (!crypto.timingSafeEqual(Buffer.from(signature), Buffer.from(expectedSignature))) {
      console.warn('❌ Invalid webhook signature');
      return res.status(401).send('Invalid signature');
    }
  } catch (error) {
    // Buffer length mismatch
    console.warn('❌ Signature comparison failed:', error.message);
    return res.status(401).send('Invalid signature');
  }

  console.log('✅ Webhook signature verified');
  next();
}

// ============================================
// META INSTAGRAM WEBHOOKS (Path B - Frontend Display)
// ============================================

// Webhook verification (Meta will call this during setup)
router.get('/instagram', (req, res) => {
  const mode = req.query['hub.mode'];
  const token = req.query['hub.verify_token'];
  const challenge = req.query['hub.challenge'];

  console.log('🔍 Meta Webhook Verification:', {
    mode,
    token: token ? 'provided' : 'missing',
    challenge: challenge ? 'provided' : 'missing'
  });

  if (mode === 'subscribe' && token === process.env.WEBHOOK_VERIFY_TOKEN) {
    console.log('✅ Meta webhook verified successfully');
    res.status(200).send(challenge);
  } else {
    console.log('❌ Meta webhook verification failed');
    res.sendStatus(403);
  }
});

// Instagram event handler (Meta sends events here for frontend display - Path B)
router.post('/instagram', verifyMetaWebhookSignature, async (req, res) => {
  const body = req.body;
  console.log('📨 Instagram webhook event (verified):', JSON.stringify(body, null, 2));

  if (body.object === 'instagram') {
    try {
      const supabase = getSupabaseAdmin();

      // Process each entry for frontend display (Path B)
      for (const entry of body.entry || []) {
        const igAccountId = entry.id; // Instagram account ID from Meta
        const changes = entry.changes || [];

        for (const change of changes) {
          const field = change.field;
          const value = change.value;

          console.log(`   Processing ${field} event for account ${igAccountId}`);

          // Always write to audit_log first for observability
          await logAudit({
            event_type: `webhook_${field}`,
            action: 'received',
            resource_type: 'instagram_webhook',
            resource_id: igAccountId,
            details: { field, value },
            success: true
          }).catch(err => console.error('Webhook audit log failed:', err.message));

          // Domain-table persistence: write to canonical tables
          // Agent subscribes via Supabase Realtime postgres_changes
          if (field === 'comments' && supabase) {
            await handleCommentEvent(supabase, igAccountId, value).catch(err =>
              console.error('Webhook comment write failed:', err.message)
            );
          } else if (field === 'mentions' && supabase) {
            await handleMentionEvent(supabase, igAccountId, value).catch(err =>
              console.error('Webhook mention write failed:', err.message)
            );
          } else if (field === 'story_mentions' && supabase) {
            await handleStoryMentionEvent(supabase, igAccountId, value).catch(err =>
              console.error('Webhook story mention write failed:', err.message)
            );
          }
          // messages field for IG DMs handled separately via IG Messaging API webhooks
        }
      }

      res.status(200).send('EVENT_RECEIVED');
    } catch (error) {
      console.error('Webhook processing error:', error);
      // Always return 200 to prevent Instagram retries
      res.status(200).send('EVENT_RECEIVED');
    }
  } else {
    res.sendStatus(404);
  }
});

// ============================================
// WEBHOOK DOMAIN EVENT HANDLERS
// All writes go through DB → Supabase Realtime → Agent (Law 13 / Law 14)
// ============================================

/**
 * Handles incoming comment webhook events.
 * Writes to instagram_comments table for agent consumption via Supabase Realtime.
 */
async function handleCommentEvent(supabase, igAccountId, value) {
  const commentId = value.id;
  const mediaId = value.media_id;
  const text = value.text;
  const username = value.from?.username || value.user || 'unknown';
  const timestamp = value.created_at || new Date().toISOString();

  if (!commentId) return;

  // Resolve igAccountId → business_account_id UUID
  const { data: account } = await supabase
    .from('instagram_credentials')
    .select('business_account_id')
    .eq('instagram_business_id', igAccountId)
    .maybeSingle();

  if (!account?.business_account_id) {
    console.warn(`[Webhook] No credentials mapping for IG account ${igAccountId}`);
    return;
  }

  const businessAccountId = account.business_account_id;

  // Ensure media record exists (needed for FK constraint on instagram_comments.media_id)
  const mediaUUID = await ensureMediaRecord(supabase, mediaId, businessAccountId);
  if (!mediaUUID) {
    console.warn(`[Webhook] Could not resolve media ${mediaId} for comment ${commentId}`);
  }

  // Upsert comment record (idempotent - uses instagram_comment_id as conflict target)
  const commentRecord = {
    instagram_comment_id: commentId,
    text: text || '',
    author_username: username,
    author_instagram_id: null,
    media_id: mediaUUID,
    business_account_id: businessAccountId,
    created_at: timestamp,
    like_count: value.like_count || 0,
    reply_count: 0,
  };

  await supabase
    .from('instagram_comments')
    .upsert(commentRecord, { onConflict: 'instagram_comment_id', ignoreDuplicates: true });

  console.log(`[Webhook] Comment ${commentId} written to instagram_comments`);
}

/**
 * Handles incoming mention webhook events.
 * Writes to audit_log + a webhook_events table for observability.
 */
async function handleMentionEvent(supabase, igAccountId, value) {
  const mentionId = value.id || `mention_${Date.now()}`;
  const mediaId = value.media_id || null;
  const username = value.from?.username || value.user || 'unknown';
  const timestamp = value.created_at || new Date().toISOString();
  const mentionType = value.mention_type || 'post';

  // Upsert to webhook_events table for agent consumption
  const { error } = await supabase
    .from('webhook_events')
    .upsert({
      event_id: mentionId,
      event_type: 'mention',
      instagram_account_id: igAccountId,
      media_id: mediaId,
      username,
      payload: value,
      received_at: timestamp,
    }, { onConflict: 'event_id', ignoreDuplicates: true });

  if (error) {
    console.warn(`[Webhook] Mention event write failed:`, error.message);
  } else {
    console.log(`[Webhook] Mention ${mentionId} written to webhook_events`);
  }
}

/**
 * Handles incoming story mention webhook events.
 * Writes to webhook_events table for agent consumption.
 */
async function handleStoryMentionEvent(supabase, igAccountId, value) {
  const mentionId = value.id || `story_mention_${Date.now()}`;
  const storyId = value.story_id || null;
  const username = value.from?.username || value.user || 'unknown';
  const timestamp = value.created_at || new Date().toISOString();

  const { error } = await supabase
    .from('webhook_events')
    .upsert({
      event_id: mentionId,
      event_type: 'story_mention',
      instagram_account_id: igAccountId,
      story_id: storyId,
      username,
      payload: value,
      received_at: timestamp,
    }, { onConflict: 'event_id', ignoreDuplicates: true });

  if (error) {
    console.warn(`[Webhook] Story mention event write failed:`, error.message);
  } else {
    console.log(`[Webhook] Story mention ${mentionId} written to webhook_events`);
  }
}

// ============================================
// TEST ENDPOINT
// ============================================

router.get('/test', (req, res) => {
  res.json({
    message: '✅ Webhook routes are working!',
    available_endpoints: [
      'GET /webhook/instagram (Meta verification)',
      'POST /webhook/instagram (Meta events → audit_log DB)'
    ],
    timestamp: new Date().toISOString()
  });
});

module.exports = router;
