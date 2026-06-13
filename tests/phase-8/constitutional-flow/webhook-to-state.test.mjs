// Phase 8 — Constitutional Path A
// Webhook → Ingress → Parser → Governance → FSM → Worker → State
//
// Asserts: every event_id observed by the recorder traverses the
// full chain in order, with no missing steps and no inverted
// ordering. Uses the webhook-simulator as the ingress source and
// mock governance/FSM/worker steps (the harness is the authority
// under test, not the production governance module — that lives
// in phase-7's constitutional-runtime.test.js).

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import p8 from '../runtime/index.mjs';

const FIXTURES = [
  'message-created',
  'comment-created',
  'mention-created',
  'story-reply',
  'media-update',
  'conversation-update',
];

describe('constitutional-flow/webhook-to-state', () => {
  let writer;
  const eventIds = [];

  beforeAll(async () => {
    writer = new p8.ReportWriter({ suite: 'constitutional', testName: 'webhook-to-state' });
    const h = await p8.webhook.health();
    expect(h.status).toBe(200);
  });

  afterAll(() => {
    writer.setEventIds(eventIds);
    writer.setTimeline(p8.recorder.events.slice(0, 200));
    writer.finish();
  });

  it('walks the full webhook → state chain for all 6 fixtures', async () => {
    for (const name of FIXTURES) {
      const res = await p8.webhook.deliver(name);
      expect(res.status).toBe(200);
      const parsed = p8.ingress.parse(res.body);
      const eid = parsed.event_id;
      eventIds.push(eid);

      p8.recorder.ingress(eid, res.body);
      p8.recorder.governance(eid, { actor: 'CK_DECISION', fixture: name });
      p8.recorder.fsm(eid, { fsm: 'acquisition-fsm', from: 'IDLE', to: 'PROCESSING' });
      p8.recorder.worker(eid, `worker-${name}`, { action: 'execute' });
      p8.recorder.mutation(eid, { kernel: 'acquisition', kind: 'insert' });
    }

    const checks = p8.recorder.assertAllConstitutional(eventIds);
    writer.addConstitutional(checks);
    for (const c of checks) {
      writer.bumpAssertions();
      expect(c.ok, JSON.stringify(c)).toBe(true);
    }
  });

  it('asserts governance precedes fsm precedes worker precedes mutation', async () => {
    const summary = p8.recorder.summarize();
    for (const s of summary) {
      expect(s.ingress_ts,   s.event_id).not.toBeNull();
      expect(s.governance_ts,s.event_id).not.toBeNull();
      expect(s.fsm_ts,       s.event_id).not.toBeNull();
      expect(s.worker_count, s.event_id).toBeGreaterThan(0);
      expect(s.mutation_count, s.event_id).toBeGreaterThan(0);
      expect(s.ordering_ok,  s.event_id).toBe(true);
      writer.bumpAssertions(6);
    }
  });
});
