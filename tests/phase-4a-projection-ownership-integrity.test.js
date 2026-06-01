import { describe, it, expect } from 'vitest';
import { readFileSync } from 'node:fs';
import path from 'node:path';

describe('Phase 4A: Projection Ownership Integrity', () => {
  it('telemetry workers do not import lineage-worker directly', () => {
    const workers = [
      'runtime-projection-worker.js',
      'integrity-projection-worker.js',
      'authority-projection-worker.js',
      'health-projection-worker.js',
      'systemic-pressure-projection-worker.js',
    ];
    for (const file of workers) {
      const src = readFileSync(path.resolve(process.cwd(), 'control-plane/telemetry-workers', file), 'utf8');
      expect(src.includes("governance/lineage-worker")).toBe(false);
    }
  });

  it('phase2-dumb-writer does not emit semantic_projection transitions — only writes PENDING ledger entries', () => {
    const src = readFileSync(path.resolve(process.cwd(), 'control-plane/telemetry-workers/phase2-dumb-writer.js'), 'utf8');
    // Phase 2 writer should write to ledger with PENDING status, not emit transitions itself
    expect(src.includes("entity: 'semantic_projection'")).toBe(false);
    expect(src.includes("constitutionalStatus: 'PENDING'")).toBe(true);
  });
});
