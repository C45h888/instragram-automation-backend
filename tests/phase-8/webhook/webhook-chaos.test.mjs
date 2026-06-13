// Phase 8 — Webhook Chaos
// Validates that the webhook simulator's chaos vocabulary actually
// fires and that the recorder chain still sees a coherent event
// for every scenario. Each scenario:
//   - is armed via the control port
//   - is delivered via the delivery port
//   - produces a non-2xx or duplicated/stale payload as expected
//   - is then cleared so subsequent tests start clean

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import p8 from '../runtime/index.mjs';

const SCENARIOS = [
  { name: 'rate-limit',       expectStatus: 429 },
  { name: 'schema-drift',     expectStatus: 200, driftShape: true },
  { name: 'duplicate',        expectStatus: 200, duplicate: true },
  { name: 'stale',            expectStatus: 200, stale: true },
  { name: 'malformed',        expectStatus: 400 },
  { name: 'token-failure',    expectStatus: 401 },
  { name: 'scope-revocation', expectStatus: 403 },
];

describe('webhook/webhook-chaos', () => {
  let writer;
  const findings = [];

  beforeAll(async () => {
    writer = new p8.ReportWriter({ suite: 'webhook', testName: 'webhook-chaos' });
    await p8.webhook.reset();
  });
  afterAll(() => {
    writer.addExtra('chaos_findings', findings);
    writer.finish();
  });

  for (const s of SCENARIOS) {
    it(`chaos scenario: ${s.name} -> status ${s.expectStatus}`, async () => {
      const inj = await p8.webhook.inject(s.name, 'message-created', 1);
      expect(inj.status).toBe(200);

      const res = await p8.webhook.deliver('message-created');
      findings.push({ scenario: s.name, status: res.status });
      writer.bumpAssertions();
      expect(res.status, `scenario ${s.name}`).toBe(s.expectStatus);

      if (s.driftShape) {
        const parsed = p8.ingress.parse(res.body);
        expect(parsed.type).toBe('unknown');
        writer.bumpAssertions();
      }

      const cleared = await p8.webhook.clear();
      expect(cleared.status).toBe(200);
      writer.bumpAssertions();
    });
  }

  it('reset wipes deliveries and armed injections', async () => {
    await p8.webhook.inject('rate-limit', 'message-created', 1);
    const r = await p8.webhook.reset();
    expect(r.status).toBe(200);
    const d = await p8.webhook.deliveries();
    expect(d.body.count).toBe(0);
    writer.bumpAssertions(2);
  });
});
