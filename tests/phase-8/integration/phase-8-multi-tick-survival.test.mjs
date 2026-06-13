// Phase 8 — Multi-Tick Survival
// Runs N ticks (50 / 250 / 1000 depending on PHASE8_CADENCE_TIER).
// Each tick delivers one webhook fixture, drives a graph request,
// and runs one cross-kernel pair. The runtime must survive all
// ticks without recorder corruption.

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import http from 'node:http';
import p8 from '../runtime/index.mjs';

const TIER = (process.env.PHASE8_CADENCE_TIER || 'medium').toLowerCase();
const TICKS = TIER === 'short' ? 50 : TIER === 'long' ? 1000 : 250;
const FIXTURES = [
  'message-created', 'comment-created', 'mention-created',
  'story-reply', 'media-update', 'conversation-update',
];
const KERNELS = ['capability', 'acquisition', 'publishing', 'recovery', 'insights'];

const GRAPH_HOST = process.env.GRAPH_SIMULATOR_HOST || 'graph-simulator';
const GRAPH_PORT = parseInt(process.env.GRAPH_SIMULATOR_PORT || '9100', 10);

function getGraph(p) {
  return new Promise((resolve, reject) => {
    const req = http.request({ host: GRAPH_HOST, port: GRAPH_PORT, method: 'GET', path: p }, (res) => {
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => resolve({ status: res.statusCode }));
    });
    req.on('error', reject);
    req.end();
  });
}

describe('integration/phase-8-multi-tick-survival', () => {
  let writer;

  beforeAll(async () => {
      p8.recorder.reset();
    writer = new p8.ReportWriter({ suite: 'integration', testName: 'phase-8-multi-tick-survival' });
    writer.addExtra('tier', TIER);
    writer.addExtra('ticks_planned', TICKS);
    await p8.webhook.reset();
  });
  afterAll(() => writer.finish());

  it(`survives ${TICKS} ticks (tier=${TIER})`, async () => {
    const startedAt = Date.now();
    for (let t = 0; t < TICKS; t++) {
      const f = FIXTURES[t % FIXTURES.length];
      const wh = await p8.webhook.deliver(f);
      if (wh.status !== 200) throw new Error(`webhook fail at tick ${t}: ${wh.status}`);
      const parsed = p8.ingress.parse(wh.body);
      // Per-tick event_id: webhook event_ids are content-hashed,
      // so the same fixture delivered at tick 0 and tick 6
      // produces the same id. Append the tick to keep each
      // recorder chain distinct.
      const whEid = `${parsed.event_id}__tick_${t}`;
      p8.recorder.ingress(whEid, wh.body);
      p8.recorder.governance(whEid, { actor: 'CK_DECISION', tick: t });
      p8.recorder.fsm(whEid, { fsm: 'tick-fsm' });
      p8.recorder.worker(whEid, `tick-worker-${t}`, { action: 'tick' });
      p8.recorder.mutation(whEid, { kernel: 'tick' });

      const gr = await getGraph('/v1/accounts');
      if (gr.status !== 200) throw new Error(`graph fail at tick ${t}: ${gr.status}`);

      const src = KERNELS[t % KERNELS.length];
      const dst = KERNELS[(t + 1) % KERNELS.length];
      const packetId = `tick_${t}_${src}_${dst}`;
      p8.recorder.ingress(packetId, { src, dst, tick: t });
      p8.recorder.governance(packetId, { actor: 'CK_DECISION' });
      p8.recorder.fsm(packetId, { fsm: 'pair-fsm' });
      p8.recorder.worker(packetId, 'pair-worker', { action: 'pair' });
      p8.recorder.mutation(packetId, { kernel: dst });

      const c1 = p8.recorder.assertConstitutionalPath(whEid);
      const c2 = p8.recorder.assertConstitutionalPath(packetId);
      if (!c1.ok || !c2.ok) {
        throw new Error(`constitutional fail at tick ${t}: ${JSON.stringify([c1, c2])}`);
      }
    }
    const elapsedMs = Date.now() - startedAt;
    writer.addExtra('elapsed_ms', elapsedMs);
    writer.addExtra('ticks_actual', TICKS);
    writer.addExtra('ticks_per_sec', Math.round((TICKS * 1000) / Math.max(1, elapsedMs)));
    writer.bumpAssertions();
  }, TICKS * 200);
});
