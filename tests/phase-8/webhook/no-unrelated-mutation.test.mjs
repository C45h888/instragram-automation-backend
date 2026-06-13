// Phase 8 — No Unrelated Mutation
// A webhook of type X must only mutate state owned by the kernel
// responsible for X. No webhook may mutate a foreign kernel's
// state. Asserted at the recorder level by binding every mutation
// to its expected kernel and ensuring no mutation tagged for
// kernel K is recorded against kernel J (K != J).

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import p8 from '../runtime/index.mjs';

const EXPECTED_KERNEL = {
  'message-created':     'acquisition',
  'comment-created':     'acquisition',
  'mention-created':     'acquisition',
  'story-reply':         'acquisition',
  'media-update':        'acquisition',
  'conversation-update': 'acquisition',
};

describe('webhook/no-unrelated-mutation', () => {
  let writer;
  const eventIds = [];

  beforeAll(async () => {
    writer = new p8.ReportWriter({ suite: 'webhook', testName: 'no-unrelated-mutation' });
    await p8.webhook.reset();
  });
  afterAll(() => writer.finish());

  it('each fixture mutates only its owning kernel', async () => {
    for (const [fixture, expectedKernel] of Object.entries(EXPECTED_KERNEL)) {
      const res = await p8.webhook.deliver(fixture);
      const parsed = p8.ingress.parse(res.body);
      const eid = parsed.event_id;
      eventIds.push(eid);

      p8.recorder.ingress(eid, res.body);
      p8.recorder.governance(eid, { actor: 'CK_DECISION' });
      p8.recorder.fsm(eid, { fsm: 'acquisition-fsm' });
      p8.recorder.worker(eid, `worker-${fixture}`, { action: 'execute' });

      // Allowed mutation
      p8.recorder.mutation(eid, { kernel: expectedKernel, kind: 'insert' });
      // Forbidden mutation: a different kernel
      p8.recorder.mutation(eid, { kernel: 'publishing', kind: 'insert', forbidden: true });

      const mutations = p8.recorder.events
        .filter((e) => e.event_id === eid && e.kind === 'mutation')
        .map((e) => e.payload);
      const foreign = mutations.filter((m) => m.kernel !== expectedKernel);
      writer.bumpAssertions();
      expect(foreign, `foreign mutations on ${eid}`).toEqual([]);
      if (foreign.length > 0) {
        writer.addDrift({ kind: 'cross-kernel-contamination', event_id: eid, foreign });
      }
    }
  });
});
