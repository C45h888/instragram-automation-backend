// backend/routes/webhook.js - Meta Instagram Webhooks
// Receives Instagram events for frontend display (Path B)
// Agent receives same events directly from Meta (Path A) for automation

const express = require('express');
const router = express.Router();
const crypto = require('crypto');
const axios = require('axios');

// ============================================
// AGENT WEBHOOK FORWARDING (Path A)
// ============================================

/**
 * Fire-and-forget forward of a single normalized webhook change to the agent.
 * Re-signs with META_APP_SECRET so the agent's HMAC verification passes.
 * Non-fatal — Meta's 200 ACK must never be delayed by agent availability.
 *
 * Forwards a single-change normalized payload (not the raw multi-change body)
 * to avoid the agent processing only entry[0].changes[0] multiple times when
 * a webhook batch contains more than one change.
 *
 * @param {string} agentRoute - e.g. '/webhook/comment' or '/webhook/dm'
 * @param {object} singleChangeBody - { object, entry: [{ ...entry, changes: [change] }] }
 */
function forwardToAgent(agentRoute, singleChangeBody) {
  const agentUrl = process.env.AGENT_URL;
  if (!agentUrl) {
    console.warn('[WebhookForward] AGENT_URL not set — skipping agent forward');
    return;
  }

  const secret = process.env.META_APP_SECRET || process.env.INSTAGRAM_APP_SECRET || '';
  const serialised = JSON.stringify(singleChangeBody);
  const sig = secret
    ? `sha256=${crypto.createHmac('sha256', secret).update(serialised).digest('hex')}`
    : 'sha256=dev-bypass';

  axios.post(`${agentUrl}${agentRoute}`, singleChangeBody, {
    headers: {
      'Content-Type': 'application/json',
      'X-Hub-Signature-256': sig,
      'X-API-Key': process.env.AGENT_API_KEY || '',
    },
    timeout: 8000,
  }).catch(err =>
    console.warn(`[WebhookForward] ${agentRoute} failed (agent will catch on next poll):`, err.message)
  );
}

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
      const { getSupabaseAdmin } = require('../config/supabase');
      const supabase = getSupabaseAdmin();

      // Process each entry for frontend display (Path B)
      for (const entry of body.entry || []) {
        for (const change of entry.changes || []) {
          const field = change.field;
          const value = change.value;

          console.log(`   Processing ${field} event for frontend`);

          // Store in Supabase for archival
          if (supabase) {
            await supabase.from('audit_log').insert({
              event_type: `webhook_${field}`,
              action: 'received',
              resource_type: 'instagram_webhook',
              resource_id: entry.id,
              details: { field, value },
              success: true
            }).catch(err => console.error('Webhook log failed:', err.message));
          }

          // Broadcast to frontend realtime cache
          broadcastToFrontend(`webhook_${field}`, {
            instagram_business_id: entry.id,
            field,
            value,
            timestamp: new Date().toISOString()
          });

          // Path A: forward to agent for real-time automation.
          // Normalize to a single-change payload so the agent doesn't see a
          // multi-change batch and silently process only the first change.
          if (field === 'comments' || field === 'messages') {
            const agentRoute = field === 'comments' ? '/webhook/comment' : '/webhook/dm';
            const singleChangeBody = {
              object: body.object,
              entry: [{ ...entry, changes: [change] }],
            };
            forwardToAgent(agentRoute, singleChangeBody);
          }
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
// REAL-TIME FRONTEND COMMUNICATION
// ============================================

/**
 * Broadcasts webhook events to frontend via in-memory cache
 * Frontend polls /realtime-updates to retrieve new events
 */
function broadcastToFrontend(eventType, data) {
  // Initialize global cache if not exists
  if (!global.realtimeCache) {
    global.realtimeCache = [];
  }

  // Add event to cache
  global.realtimeCache.push({
    type: eventType,
    data: data,
    timestamp: new Date().toISOString()
  });

  // Keep only last 100 events
  if (global.realtimeCache.length > 100) {
    global.realtimeCache.shift();
  }
}

// Frontend polls for real-time updates
router.get('/realtime-updates', (req, res) => {
  const since = req.query.since; // timestamp

  if (!global.realtimeCache) {
    return res.json({ events: [], latest_timestamp: null });
  }

  let events = global.realtimeCache;

  if (since) {
    events = events.filter(event =>
      new Date(event.timestamp) > new Date(since)
    );
  }

  res.json({
    events: events,
    latest_timestamp: events.length > 0 ?
      events[events.length - 1].timestamp : null,
    total_cached: global.realtimeCache.length
  });
});

// ============================================
// TEST ENDPOINT
// ============================================

router.get('/test', (req, res) => {
  res.json({
    message: '✅ Webhook routes are working!',
    available_endpoints: [
      'GET /webhook/instagram (Meta verification)',
      'POST /webhook/instagram (Meta events for frontend)',
      'GET /webhook/realtime-updates (Frontend polling)'
    ],
    timestamp: new Date().toISOString()
  });
});

module.exports = router;
