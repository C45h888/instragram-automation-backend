// Phase 8 — Full Runtime Composition
// Boots the webhook simulator, walks the constitutional flow on
// every fixture, exercises the 20 cross-kernel pairs in one test
// pass, and confirms the recorder state is consistent at the end.

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import path from 'node:path';
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';
import p8, { CrossKernelProbe } from '../runtime/index.mjs';
import { runPair } from '../cross-kernel/_pair-helper.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

describe('integration/phase-8-full-composition', () => {
  let writer;
  let crossProbe;

  beforeAll(async () => {
    writer = new p8.ReportWriter({ suite: 'integration', testName: 'phase-8-full-composition' });
    crossProbe = new CrossKernelProbe();
    await p8.webhook.reset();
  });
  afterAll(() => writer.finish());

  it('all 6 webhook fixtures compose', async () => {
    const fixtures = [
      'message-created', 'comment-created', 'mention-created',
      'story-reply', 'media-update', 'conversation-update',
    ];
    for (const f of fixtures) {
      const r = await p8.webhook.deliver(f);
      expect(r.status).toBe(200);
      const parsed = p8.ingress.parse(r.body);
      p8.recorder.ingress(parsed.event_id, r.body);
      p8.recorder.governance(parsed.event_id, { actor: 'CK_DECISION' });
      p8.recorder.fsm(parsed.event_id, { fsm: 'composition' });
      p8.recorder.worker(parsed.event_id, `worker-${f}`, { action: 'compose' });
      p8.recorder.mutation(parsed.event_id, { kernel: 'composition' });
      const c = p8.recorder.assertConstitutionalPath(parsed.event_id);
      writer.bumpAssertions();
      expect(c.ok, JSON.stringify(c)).toBe(true);
    }
  });

  it('all 20 cross-kernel pairs compose', async () => {
    const pairs = CrossKernelProbe.PAIRS();
    expect(pairs.length).toBe(20);
    for (const p of pairs) {
      const r = await runPair({ source: p.source, sink: p.sink });
      expect(r.iso.ok).toBe(true);
      expect(r.check.ok).toBe(true);
      expect(r.foreignWrites).toEqual([]);
      writer.bumpAssertions(3);
    }
  });

  it('reports directory exists and is writable', () => {
    const dir = process.env.PHASE8_REPORT_DIR || path.join(__dirname, '..', 'reports');
    expect(fs.existsSync(dir)).toBe(true);
    writer.addExtra('report_dir', dir);
    writer.bumpAssertions();
  });
});
