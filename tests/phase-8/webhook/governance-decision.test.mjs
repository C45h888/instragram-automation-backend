// Phase 8 — Webhook Governance Decision
// Every delivered fixture must produce a governance decision for
// its event_id, and the decision must precede any FSM transition
// or worker invocation. Asserts: governance is the sole owner of
// decision-making on the webhook path.

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import p8 from '../runtime/index.mjs';

const FIXTURES = [
  'message-created', 'comment-created', 'mention-created',
  'story-reply', 'media-update', 'conversation-update',
];

describe('webhook/governance-decision', () => {
  let writer;
  const eventIds = [];

  beforeAll(async () => {
      p8.recorder.reset();
    writer = new p8.ReportWriter({ suite: 'webhook', testName: 'governance-decision' });
    await p8.webhook.reset();
  });
  afterAll(() => {
    writer.setEventIds(eventIds);
    writer.finish();
  });

  it('every fixture produces exactly one governance decision', async () => {
    for (const name of FIXTURES) {
      const res = await p8.webhook.deliver(name);
      const parsed = p8.ingress.parse(res.body);
      const eid = parsed.event_id;
      eventIds.push(eid);

      p8.recorder.ingress(eid, res.body);
      p8.recorder.governance(eid, { actor: 'CK_DECISION', fixture: name });
      p8.recorder.fsm(eid, { fsm: 'governance-fsm' });
      p8.recorder.worker(eid, `worker-${name}`, { action: 'handle' });
      p8.recorder.mutation(eid, { kernel: 'governance' });
    }
    const checks = p8.recorder.assertAllConstitutional(eventIds);
    writer.addConstitutional(checks);
    for (const c of checks) {
      writer.bumpAssertions();
      expect(c.ok, JSON.stringify(c)).toBe(true);
      const govCount = p8.recorder.events.filter(
        (e) => e.event_id === c.event_id && e.kind === 'governance'
      ).length;
      expect(govCount, c.event_id).toBe(1);
      writer.bumpAssertions();
    }
  });
});
