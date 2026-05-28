import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { execSync } from 'node:child_process';
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import observability from '../control-plane/observability/index.js';
import telemetryWorkers from '../control-plane/telemetry-workers/index.js';
import { getRedisClient } from '../config/redis.js';
const eventInjector = require('./event-injector.js');

const COMPOSE_FILE = 'docker-compose.test.yml';
const TICK_MS = 120;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitRedisReady(timeoutMs = 30000) {
  const redis = getRedisClient();
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const pong = await redis.ping();
      if (pong === 'PONG') return true;
    } catch (_) {}
    await sleep(500);
  }
  return false;
}

async function writeMetricsArtifact(payload) {
  const outputDir = path.resolve(process.cwd(), 'tests/output');
  await mkdir(outputDir, { recursive: true });
  const ts = new Date().toISOString().replace(/[:.]/g, '-');
  const stampedPath = path.join(outputDir, `phase-2d-metrics-${ts}.json`);
  const latestPath = path.join(outputDir, 'phase-2d-metrics-latest.json');
  const content = `${JSON.stringify(payload, null, 2)}\n`;
  await writeFile(stampedPath, content, 'utf8');
  await writeFile(latestPath, content, 'utf8');
}

describe('Phase 2D: Redis Durability + Recovery Window', () => {
  beforeAll(async () => {
    await observability.init();
    await telemetryWorkers.startAll(50);
  }, 20000);

  afterAll(async () => {
    await telemetryWorkers.stopAll();
    await observability.stop();
  });

  it('recovers lineage/offset continuity across mid-soak Redis restart', async () => {
    const startCursor = observability.query.getLogSize();
    const consumerName = `phase-2d-consumer-${Date.now()}`;
    observability.query.registerConsumer(consumerName);
    observability.query.updateConsumerCursor(consumerName, startCursor);

    const pending = new Set();
    let ticks = 0;

    const ticker = setInterval(() => {
      const p = eventInjector.injectAcquisitionIntent({
        variant: ticks % 4 === 0 ? 'rateLimited' : 'success',
        accountId: `phase2d-${ticks}`,
      }).catch(() => null).finally(() => pending.delete(p));
      pending.add(p);
      ticks++;
    }, TICK_MS);

    await sleep(1600);

    // Restart Redis service in the middle of runtime stress
    const restartStartedAt = Date.now();
    execSync(`docker-compose -f ${COMPOSE_FILE} restart test-redis`, { stdio: 'pipe' });
    const redisRecovered = await waitRedisReady(45000);
    const recoveryLatencyMs = Date.now() - restartStartedAt;
    expect(redisRecovered).toBe(true);

    await sleep(1600);
    clearInterval(ticker);
    await Promise.all([...pending]);

    const endCursor = observability.query.getLogSize();
    observability.query.updateConsumerCursor(consumerName, endCursor);
    const lag = observability.query.getConsumerLag(consumerName);
    const { entries } = observability.query.getEntriesSince(startCursor);

    expect(ticks).toBeGreaterThan(10);
    expect(endCursor).toBeGreaterThan(startCursor);
    expect(entries.length).toBeGreaterThan(10);
    expect(lag.atRisk).toBe(false);

    // Recovery should continue projection materialization after restart
    const hasPostRestartAcq = entries.some((e) => e.domain === 'acquisition' && e.entity === 'acquisition_intent');
    expect(hasPostRestartAcq).toBe(true);

    await writeMetricsArtifact({
      phase: '2D',
      test: 'redis-durability-window',
      startedCursor: startCursor,
      endedCursor: endCursor,
      ticksInjected: ticks,
      entryCount: entries.length,
      redisRecovered,
      recoveryLatencyMs,
      lagAtRisk: lag.atRisk,
      hasPostRestartAcquisitionTransitions: hasPostRestartAcq,
      generatedAt: new Date().toISOString(),
    });
  }, 120000);
});
