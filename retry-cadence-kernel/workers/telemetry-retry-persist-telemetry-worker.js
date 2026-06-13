// retry-cadence-kernel/workers/telemetry-retry-persist-telemetry-worker.js
// Telemetry Retry Worker — namespace: persist-telemetry.
// Mirrors telemetry-retry-capability-worker.js. Drains
// lineage:projection-staging:persist-telemetry and re-emits each entry
// through the canonical observability.transition() path.

const crypto = require('crypto');
const { getRedisClient } = require('../../config/redis');

const NAMESPACE = 'persist-telemetry';
const STAGING_KEY = `lineage:projection-staging:${NAMESPACE}`;

let _emitter = null;
function _getEmitter() {
  if (!_emitter) {
    try {
      _emitter = require('../../control-plane/observability/emitters/transition-emitter');
    } catch (_) {
      _emitter = null;
    }
  }
  return _emitter;
}

async function execute(params = {}) {
  const redis = getRedisClient();
  if (!redis || redis.status !== 'ready') {
    return { ok: false, reason: 'REDIS_UNAVAILABLE', drainedCount: 0 };
  }

  const emitter = _getEmitter();
  if (!emitter || typeof emitter.transition !== 'function') {
    return { ok: false, reason: 'EMITTER_UNAVAILABLE', drainedCount: 0 };
  }

  let drainedCount = 0;
  let failedCount = 0;
  const items = await redis.lrange(STAGING_KEY, 0, -1);
  for (let i = 0; i < items.length; i++) {
    const raw = items[i];
    let entry;
    try {
      entry = JSON.parse(raw);
    } catch (_) {
      await redis.lrem(STAGING_KEY, 1, raw);
      failedCount++;
      continue;
    }
    const newTraceId = crypto.randomUUID();
    const originalTraceId = entry.traceId || null;
    const originalProjectionId = entry.projectionId || null;
    try {
      await emitter.transition({
        domain: NAMESPACE,
        entity: 'semantic_projection',
        entityId: entry.projectionType || `${NAMESPACE}-projection`,
        previousState: `${NAMESPACE}:coordinated`,
        nextState: `${NAMESPACE}:projected`,
        authority: 'persist-telemetry-projection-worker',
        traceId: newTraceId,
        correlationId: entry.correlationId || newTraceId,
        causationId: originalTraceId,
        raw: {
          entryType: 'SEMANTIC_PROJECTION_TRANSITION',
          projectionId: newTraceId,
          projectionType: entry.projectionType || 'PERSIST_TELEMETRY_PROJECTION',
          projectionVersion: entry.projectionVersion || '1.0.0',
          projectionNamespace: NAMESPACE,
          projectionPayload: entry.projectionPayload || {},
          confidence: entry.confidence,
          integrityScore: entry.integrityScore,
          sourceTelemetryWindow: entry.sourceTelemetryWindow || {},
          parentReference: { originalProjectionId, originalTraceId },
          retryOf: 'staging_buffer_replay',
          retriedAt: Date.now(),
        },
      });
      await redis.lrem(STAGING_KEY, 1, raw);
      drainedCount++;
    } catch (err) {
      failedCount++;
    }
  }

  return {
    ok: true,
    drainedCount,
    failedCount,
    remaining: await redis.llen(STAGING_KEY),
  };
}

module.exports = {
  execute,
  NAMESPACE,
  STAGING_KEY,
};
