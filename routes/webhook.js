// routes/webhook.js
// Meta Instagram webhook endpoint — Phase 1.
//
// Phase 1: signature verification + entry.id extraction + delegation to
//          the webhook-acquisition-substrate. The substrate routes the
//          payload to bounded workers (mounted on ig-reliability-substrate
//          for failure analysis). Workers normalize into canonical event
//          objects and dispatch them into the acquisition-fsm, which
//          holds them in _stagedEvents (in-memory).
//
// Phase 2: substrate's worker will resolve staged events into
//          DB_WRITE_REQUESTED → postgres-telemetry-kernel.
//
// All routes return 200 fast (Meta's webhook timeouts are ~10s). Substrate
// does fire-and-forget async work per entry via setImmediate.

const express = require('express');
const crypto = require('crypto');
const router = express.Router();

const webhookAcquisition =
  require('../acquisition-kernel/substrates/webhook-acquisition-substrate');

// ── Meta signature verification (constant-time HMAC-SHA1) ──────────────────
// Meta signs the raw request body with HMAC-SHA1 using META_APP_SECRET.
// Header: X-Hub-Signature (sha1=...) — newer docs use SHA256 but SHA1
// remains the canonical Meta signature for Instagram webhooks.
function verifyMetaSignature(rawBody, signatureHeader, appSecret) {
  if (!rawBody || !signatureHeader || !appSecret) return false;

  // Meta prefixes the signature with the algorithm, e.g. "sha1=abcdef..."
  const [algo, expected] = String(signatureHeader).split('=', 2);
  if (!algo || !expected) return false;

  const supportedAlgos = {
    sha1: 'sha1',
    sha256: 'sha256',
  };
  const nodeAlgo = supportedAlgos[String(algo).toLowerCase()];
  if (!nodeAlgo) return false;

  const hmac = crypto.createHmac(nodeAlgo, appSecret);
  hmac.update(rawBody);
  const computed = hmac.digest('hex');

  // Constant-time comparison to prevent timing attacks
  const a = Buffer.from(computed, 'utf8');
  const b = Buffer.from(expected, 'utf8');
  if (a.length !== b.length) return false;
  return crypto.timingSafeEqual(a, b);
}

// ── Webhook verification handshake (GET) ──────────────────────────────────
// Meta calls GET /webhook/instagram with hub.mode, hub.verify_token, hub.challenge
// when you first register the webhook in the app dashboard.
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
// Meta calls POST /webhook/instagram with the JSON body. We verify
// the HMAC signature against META_APP_SECRET, extract the IG account
// id from each entry, and hand the payload to the substrate.
//
// Signature verification is permissive in dev (no META_APP_SECRET → log
// and continue; Meta still delivers). In production, set
// META_APP_SECRET and the route will reject unsigned payloads.
router.post('/instagram', (req, res) => {
  const rawBody = req.rawBody; // set by bodyParser.verify hook in server.js
  const signature = req.headers['x-hub-signature'] || req.headers['X-Hub-Signature'];
  const appSecret = process.env.META_APP_SECRET || process.env.INSTAGRAM_APP_SECRET;

  // ── Signature verification ──────────────────────────────────────────
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
    // Dev mode: no secret configured — log and continue
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

  // ── Account id resolution (entry.id is the IG business account id) ──
  // Multiple entries can be in one payload; the substrate processes each.
  // We extract the first entry's id as the "primary" accountId for
  // observability. Workers receive per-entry accountId themselves.
  const firstEntry = Array.isArray(payload.entry) ? payload.entry[0] : null;
  const accountId = firstEntry?.id || null;

  if (!accountId) {
    console.warn('[webhook] POST has no entry[0].id — payload malformed', {
      requestId: req.requestId,
      hasEntry: Array.isArray(payload.entry),
    });
    return res.status(200).json({ received: true, warning: 'no_entry_id' });
  }

  // ── Delegate to substrate (fire-and-forget) ─────────────────────────
  const result = webhookAcquisition.processWebhook(payload, accountId);

  // Always 200 fast — Meta will retry on non-200
  return res.status(200).json({
    received: true,
    requestId: req.requestId,
    routing: result,
  });
});

// ── Health surface for the substrate (read-only) ─────────────────────────
// Useful for debugging without grepping logs.
router.get('/staged-events', (req, res) => {
  try {
    const acquisitionFsm = require('../acquisition-kernel/fsm');
    const accountId = req.query.accountId || null;
    if (accountId) {
      return res.status(200).json({
        accountId,
        events: acquisitionFsm.getStagedEvents(accountId),
      });
    }
    // No specific account — return state only
    return res.status(200).json({
      state: acquisitionFsm.getState(),
    });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

module.exports = router;
