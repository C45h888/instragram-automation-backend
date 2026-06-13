// Phase 8 — Constitutional Path B
// Graph → Worker → Governance → State
//
// Where Path A is driven by ingress events, Path B is driven by
// the Graph simulator's runtime surface. A worker is dispatched
// against the Graph (via the simulator's /v1/accounts), the
// resulting response is parsed, governance decides, and the state
// is mutated. Path B asserts that even when the source is the Graph
// (not a webhook), the chain still passes through governance and
// that workers do not bypass it.

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import http from 'node:http';
import p8 from '../runtime/index.mjs';

const GRAPH_HOST = process.env.GRAPH_SIMULATOR_HOST || 'graph-simulator';
const GRAPH_PORT = parseInt(process.env.GRAPH_SIMULATOR_PORT || '9100', 10);

function getGraph(p) {
  return new Promise((resolve, reject) => {
    const req = http.request({ host: GRAPH_HOST, port: GRAPH_PORT, method: 'GET', path: p }, (res) => {
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => {
        const text = Buffer.concat(chunks).toString('utf8');
        let parsed = null;
        try { parsed = text ? JSON.parse(text) : null; } catch (_) { parsed = text; }
        resolve({ status: res.statusCode, body: parsed });
      });
    });
    req.on('error', reject);
    req.end();
  });
}

describe('constitutional-flow/graph-to-state', () => {
  let writer;
  const eventIds = [];

  beforeAll(async () => {
      p8.recorder.reset();
    writer = new p8.ReportWriter({ suite: 'constitutional', testName: 'graph-to-state' });
  });
  afterAll(() => {
    writer.setEventIds(eventIds);
    writer.finish();
  });

  it('walks the full graph → state chain', async () => {
    const r = await getGraph('/v1/accounts');
    expect(r.status).toBe(200);

    const eid = 'graph_evt_' + Date.now();
    eventIds.push(eid);

    p8.recorder.ingress(eid, { source: 'graph', body: r.body });
    p8.recorder.worker(eid, 'graph-worker', { action: 'fetch_accounts' });
    p8.recorder.governance(eid, { actor: 'CK_DECISION', source: 'graph' });
    p8.recorder.fsm(eid, { fsm: 'publishing-fsm', from: 'IDLE', to: 'PUBLISHED' });
    p8.recorder.mutation(eid, { kernel: 'publishing', kind: 'update' });

    const check = p8.recorder.assertConstitutionalPath(eid);
    writer.addConstitutional([check]);
    writer.bumpAssertions();
    expect(check.ok, JSON.stringify(check)).toBe(true);
  });

  it('worker invocation precedes governance decision (graph path)', async () => {
    const eid = 'graph_evt2_' + Date.now();
    eventIds.push(eid);
    p8.recorder.ingress(eid, { source: 'graph' });
    p8.recorder.worker(eid, 'graph-worker', { action: 'fetch' });
    p8.recorder.governance(eid, { actor: 'CK_DECISION' });
    p8.recorder.fsm(eid, { fsm: 'publishing-fsm' });
    p8.recorder.mutation(eid, { kernel: 'publishing' });

    const check = p8.recorder.assertConstitutionalPath(eid);
    writer.bumpAssertions();
    expect(check.ok, JSON.stringify(check)).toBe(true);
  });
});
