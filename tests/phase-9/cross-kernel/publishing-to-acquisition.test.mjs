// Phase 9 — Cross-Kernel Pair
// Source: publishing  →  Sink: acquisition
//
// Validates: the runtime's CK routes the cross-kernel signal from
// publishing to acquisition constitutionally. The recorder-observer captures
// the chain. The test asserts the event was observed and the
// ownership record is present.

import { describe, it, expect } from 'vitest';
import { runPair } from './_pair-helper.mjs';

describe('cross-kernel/publishing-to-acquisition', () => {
  it('routes the signal constitutionally', async () => {
    const r = await runPair({ source: 'publishing', sink: 'acquisition' });
    expect(r.event, 'event not observed by runtime').toBeDefined();
    expect(r.ownership, 'ownership record missing').toBeDefined();
    expect(r.findings, JSON.stringify(r.findings)).toEqual([]);
  });
});
