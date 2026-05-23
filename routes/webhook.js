// backend/routes/webhook.js - Meta Instagram Webhooks
// Receives Instagram events for frontend display (Path B)
// Agent receives same events directly from Meta (Path A) for automation

const express = require('express');
const router = express.Router();
const crypto = require('crypto');

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
