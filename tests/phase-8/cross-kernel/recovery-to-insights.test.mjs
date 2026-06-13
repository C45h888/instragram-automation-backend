// Phase 8 — Cross-Kernel Pair Test
// Source: recovery
// Sink:   insights
//
// Validates: (a) constitutional signal projection from recovery → insights
//            (b) sink never sees the source's internal sentinel
//            (c) governance precedes fsm precedes worker precedes mutation
//            (d) no foreign-kernel writes recorded

import { describe, it, expect } from 'vitest';
import { runPair } from './_pair-helper.mjs';

describe('cross-kernel/recovery-to-insights', () => {
  it('isolates sentinel and preserves constitutional path', async () => {
    const r = await runPair({ source: 'recovery', sink: 'insights' });
    expect(r.iso.ok, JSON.stringify(r.iso)).toBe(true);
    expect(r.check.ok, JSON.stringify(r.check)).toBe(true);
    expect(r.foreignWrites, JSON.stringify(r.foreignWrites)).toEqual([]);
  });
});
