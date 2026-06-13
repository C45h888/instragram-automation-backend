// Phase 8 — Webhook Ingress Parser
// Validates that the 6 canonical fixtures are parseable and that
// the ingress adapter produces a normalized IngressEvent with a
// stable event_id per payload.

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import p8 from '../runtime/index.mjs';

const EXPECTED_TYPES = {
  'message-created':     'message-created',
  'comment-created':     'comment-created',
  'mention-created':     'mention-created',
  'story-reply':         'story-reply',
  'media-update':        'media-update',
  'conversation-update': 'conversation-update',
};

describe('webhook/ingress-parser', () => {
  let writer;
  beforeAll(async () => {
    writer = new p8.ReportWriter({ suite: 'webhook', testName: 'ingress-parser' });
    const h = await p8.webhook.health();
    expect(h.status).toBe(200);
  });
  afterAll(() => writer.finish());

  it('health + fixtures endpoints respond', async () => {
    const list = await p8.webhook.list();
    expect(list.status).toBe(200);
    expect(list.body.fixtures).toEqual(expect.arrayContaining(Object.keys(EXPECTED_TYPES)));
    writer.addExtra('fixtures', list.body.fixtures);
    writer.bumpAssertions(2);
  });

  it('parses all 6 canonical fixtures into IngressEvents', async () => {
    for (const [fixture, expected] of Object.entries(EXPECTED_TYPES)) {
      const res = await p8.webhook.deliver(fixture);
      expect(res.status, `fixture ${fixture}`).toBe(200);
      const parsed = p8.ingress.parse(res.body);
      expect(parsed.type, `fixture ${fixture}`).toBe(expected);
      expect(parsed.event_id, `fixture ${fixture}`).toMatch(/^evt_/);
      p8.recorder.ingress(parsed.event_id, res.body);
      writer.bumpAssertions(2);
    }
  });

  it('event_id is stable for identical payloads', () => {
    const a = p8.ingress.parse({ object: 'instagram', entry: [{ id: 'X', time: 1 }] });
    const b = p8.ingress.parse({ object: 'instagram', entry: [{ id: 'X', time: 1 }] });
    expect(a.event_id).toBe(b.event_id);
    writer.bumpAssertions();
  });

  it('replay delivers all 6 fixtures in sequence', async () => {
    const r = await p8.webhook.replay();
    expect(r.status).toBe(200);
    expect(r.body.replayed.length).toBe(Object.keys(EXPECTED_TYPES).length);
    writer.bumpAssertions(2);
  });
});
