// Phase 8 — Ingress Adapter.
// Parses webhook payloads from webhook-simulator and produces
// a normalized `IngressEvent` that the governance/FSM/worker
// surface can consume. Replaces server.js POST /webhook for
// hermetic phase-8 tests.

import crypto from 'node:crypto';

export function eventId(payload) {
  const text = JSON.stringify(payload || {});
  return 'evt_' + crypto.createHash('sha1').update(text).digest('hex').slice(0, 16);
}

export function inferType(payload) {
  if (!payload || payload.object !== 'instagram') return 'unknown';
  const e = (payload.entry && payload.entry[0]) || null;
  if (!e) return 'unknown';
  if (e.messaging && e.messaging.length) return 'message-created';
  if (e.changes) {
    for (const c of e.changes) {
      if (c.field === 'comments') return 'comment-created';
      if (c.field === 'mentions') return 'mention-created';
      if (c.field === 'story_insights') return 'story-reply';
      if (c.field === 'media') return 'media-update';
      if (c.field === 'conversations') return 'conversation-update';
    }
  }
  return 'unknown';
}

export function parse(payload) {
  const type = inferType(payload);
  return {
    event_id: eventId(payload),
    type,
    source: 'webhook',
    object: payload.object || null,
    entry: payload.entry || [],
    raw: payload,
  };
}

export default { parse, inferType, eventId };
