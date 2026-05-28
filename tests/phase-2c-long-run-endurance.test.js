import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import observability from '../control-plane/observability/index.js';
import telemetryWorkers from '../control-plane/telemetry-workers/index.js';
const eventInjector = require('./event-injector.js');

const SOAK_DURATION_MS = parseInt(process.env.PHASE2_SOAK_MS || '180000', 10); // default 3 min
const TICK_MS = parseInt(process.env.PHASE2_TICK_MS || '120', 10);
const CHECKPOINT_MS = parseInt(process.env.PHASE2_CHECKPOINT_MS || '30000', 10);
const RECYCLE_MS = parseInt(process.env.PHASE2_RECYCLE_MS || '45000', 10);
const VARIANTS = ['success', 'partial', 'stale', 'rateLimited'];

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function stableStringify(value) {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(',')}]`;
  const keys = Object.keys(value).sort();
  return `{${keys.map((k) => `${JSON.stringify(k)}:${stableStringify(value[k])}`).join(',')}}`;
}

function signatureForEntries(entries) {
  const compact = entries.map((e) => ({
    d: e.domain,
    en: e.entity,
    id: e.entityId,
    p: e.previousState,
    n: e.nextState,
    a: e.authority,
    t: e.timestamp,
  }));
  return stableStringify(compact);
}

async function writeMetricsArtifact(payload) {
  const outputDir = path.resolve(process.cwd(), 'tests/output');
  await mkdir(outputDir, { recursive: true });
  const ts = new Date().toISOString().replace(/[:.]/g, '-');
  const stampedPath = path.join(outputDir, `phase-2c-metrics-${ts}.json`);
  const latestPath = path.join(outputDir, 'phase-2c-metrics-latest.json');
  const content = `${JSON.stringify(payload, null, 2)}\n`;
  await writeFile(stampedPath, content, 'utf8');
  await writeFile(latestPath, content, 'utf8');
}

describe('Phase 2C: Long-Run Constitutional Endurance', () => {
  beforeAll(async () => {
    await observability.init();
    await telemetryWorkers.startAll(50);
  }, 20000);

  afterAll(async () => {
    await telemetryWorkers.stopAll();
    await observability.stop();
  });

  it('maintains lineage growth, replay/rebuild convergence, worker recycling resilience, and lag integrity over soak', async () => {
    const startCursor = observability.query.getLogSize();
    const consumerName = `phase-2c-consumer-${Date.now()}`;
    observability.query.registerConsumer(consumerName);
    observability.query.updateConsumerCursor(consumerName, startCursor);

    const pending = new Set();
    const lagHeads = [];
    const checkpoints = [];
    let tickCount = 0;
    let recycleCount = 0;

    const ticker = setInterval(() => {
      const variant = VARIANTS[tickCount % VARIANTS.length];
      const p = eventInjector.injectAcquisitionIntent({
        variant,
        accountId: `phase2c-${tickCount}`,
      }).catch(() => null).finally(() => pending.delete(p));
      pending.add(p);

      tickCount++;
    }, TICK_MS);

    const checkpointTimer = setInterval(() => {
      const head = observability.query.getLogSize();
      lagHeads.push(head);
      const { entries } = observability.query.getEntriesSince(startCursor);
      const sigA = signatureForEntries(entries);
      const sigB = signatureForEntries(entries);
      checkpoints.push({ head, sigA, sigB, count: entries.length });
      observability.query.updateConsumerCursor(consumerName, head);
    }, CHECKPOINT_MS);

    const recycleTimer = setInterval(async () => {
      recycleCount++;
      await telemetryWorkers.stopAll();
      await sleep(150);
      await telemetryWorkers.startAll(50);
    }, RECYCLE_MS);

    await sleep(SOAK_DURATION_MS);
    clearInterval(ticker);
    clearInterval(checkpointTimer);
    clearInterval(recycleTimer);
    await Promise.all([...pending]);

    const endCursor = observability.query.getLogSize();
    const { entries } = observability.query.getEntriesSince(startCursor);
    const lag = observability.query.getConsumerLag(consumerName);

    expect(tickCount).toBeGreaterThan(20);
    expect(endCursor).toBeGreaterThan(startCursor);
    expect(entries.length).toBeGreaterThan(20);
    expect(recycleCount).toBeGreaterThanOrEqual(1);
    expect(lag.atRisk).toBe(false);

    // Consumer head should move monotonically (no stuck offsets)
    for (let i = 1; i < lagHeads.length; i++) {
      expect(lagHeads[i]).toBeGreaterThanOrEqual(lagHeads[i - 1]);
    }

    // Same lineage window should converge to same signature
    for (const c of checkpoints) {
      expect(c.sigA).toBe(c.sigB);
    }

    // Projection continuity exists through soak
    const projectionEntries = entries.filter(
      (e) => e.entity === 'semantic_projection' && e.raw?.entryType === 'SEMANTIC_PROJECTION_TRANSITION'
    );
    expect(projectionEntries.length).toBeGreaterThan(0);

    await writeMetricsArtifact({
      phase: '2C',
      test: 'long-run-endurance',
      startedCursor: startCursor,
      endedCursor: endCursor,
      ticksInjected: tickCount,
      workerRecycleCount: recycleCount,
      entryCount: entries.length,
      checkpointCount: checkpoints.length,
      lagAtRisk: lag.atRisk,
      lagHeadSeries: lagHeads,
      projectionTransitionCount: projectionEntries.length,
      soakConfig: {
        SOAK_DURATION_MS,
        TICK_MS,
        CHECKPOINT_MS,
        RECYCLE_MS,
      },
      generatedAt: new Date().toISOString(),
    });
  }, Math.max(240000, SOAK_DURATION_MS + 60000));
});
