// acquisition-kernel/routes/webhook.js
// Meta Instagram webhook endpoint — HTTP surface only.
//
// Owns: signature verification, GET handshake, 200-fast return.
// Does NOT own: entry shape inspection, worker selection, intentId
//               generation, persistence trigger, batch buffering.
//
// All heavy work (classification, normalization, FSM staging, DB write)
// runs async via setImmediate so Meta gets a 200 within ~10s.

const express = require('express');
const crypto = require('crypto');
const router = express.Router();

const webhookAcquisition =
  require('../substrates/webhook-acquisition-substrate');

// ── Meta signature verification (constant-time HMAC-SHA1/256) ───────────────
function verifyMetaSignature(rawBody, signatureHeader, appSecret) {
  if (!rawBody || !signatureHeader || !appSecret) return false;

  const [algo, expected] = String(signatureHeader).split('=', 2);
  if (!algo || !expected) return false;

  const supportedAlgos = { sha1: 'sha1', sha256: 'sha256' };
  const nodeAlgo = supportedAlgos[String(algo).toLowerCase()];
  if (!nodeAlgo) return false;

  const hmac = crypto.createHmac(nodeAlgo, appSecret);
  hmac.update(rawBody);
  const computed = hmac.digest('hex');

  const a = Buffer.from(computed, 'utf8');
  const b = Buffer.from(expected, 'utf8');
  if (a.length !== b.length) return false;
  return crypto.timingSafeEqual(a, b);
}

// ── Webhook verification handshake (GET) ───────────────────────────────────
router.get('/instagram', (req, res) => {
  const mode      = req.query['hub.mode'];
  const token     = req.query['hub.verify_token'];
  const challenge = req.query['hub.challenge'];
  const expected  = process.env.META_VERIFY_TOKEN || process.env.INSTAGRAM_WEBHOOK_VERIFY_TOKEN;

  if (mode === 'subscribe' && token && expected && token === expected) {
    console.log('[webhook] GET verification handshake OK');
    return res.status(200).send(challenge);
  }
  console.warn('[webhook] GET verification handshake FAILED', {
    mode, tokenPresent: !!token, expectedPresent: !!expected,
  });
  return res.status(403).json({ error: 'verification_failed' });
});

// ── Webhook event delivery (POST) ─────────────────────────────────────────
router.post('/instagram', (req, res) => {
  const rawBody   = req.rawBody; // set by bodyParser.verify hook in server.js
  const signature = req.headers['x-hub-signature'] || req.headers['X-Hub-Signature'];
  const appSecret = process.env.META_APP_SECRET || process.env.INSTAGRAM_APP_SECRET;

  // ── Signature verification ───────────────────────────────────────────
  if (appSecret) {
    if (!rawBody) {
      console.warn('[webhook] POST missing rawBody — bodyParser hook did not run?');
      return res.status(400).json({ error: 'missing_raw_body' });
    }
    if (!verifyMetaSignature(rawBody, signature, appSecret)) {
      console.warn('[webhook] POST signature verification FAILED', {
        signaturePresent: !!signature,
        requestId: req.requestId,
      });
      return res.status(401).json({ error: 'invalid_signature' });
    }
  } else {
    if (process.env.NODE_ENV === 'production') {
      console.error('[webhook] META_APP_SECRET not configured in production');
      return res.status(500).json({ error: 'server_misconfigured' });
    }
    console.warn('[webhook] META_APP_SECRET not set — skipping signature verification (dev only)');
  }

  const payload = req.body;
  if (!payload || typeof payload !== 'object') {
    return res.status(400).json({ error: 'invalid_payload' });
  }

  const firstEntry = Array.isArray(payload.entry) ? payload.entry[0] : null;
  const accountId = firstEntry?.id || null;

  if (!accountId) {
    console.warn('[webhook] POST has no entry[0].id — payload malformed', {
      requestId: req.requestId,
      hasEntry: Array.isArray(payload.entry),
    });
    return res.status(200).json({ received: true, warning: 'no_entry_id' });
  }

  // Delegate — substrate owns timing, classification, and persistence trigger.
  const result = webhookAcquisition.processWebhook(payload, accountId);

  return res.status(200).json({
    received: true,
    requestId: req.requestId,
    routing: result,
  });
});

module.exports = router;