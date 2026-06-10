// retry-cadence-kernel/workers/telemetry-retry-runtime-worker.js
// Telemetry Retry Worker — namespace: authority.
//
// Bounded executor for the runtime namespace staging buffer.
// Drains lineage:projection-staging:runtime and re-emits each entry
// through the canonical observability.transition() path.
//
// CONSTITUTIONAL CONTRACT:
//   - One bounded I/O drain per execute(). LRANGEs the staging list,
//     re-emits each entry, LREMs only on success. Failed re-emits stay
//     in the list for the next retry.
//   - Re-emits use NEW projectionId + traceId; original IDs carried
//     in raw.parentReference. correlationId preserved.
//   - Emits WORKER_OUTCOME_REPORTED. Does NOT classify, schedule, or
//     mutate FSM state.

const crypto = require('crypto');
const { getRedisClient } = require('../../config/redis');

const NAMESPACE = 'authority';
const STAGING_KEY = `lineage:projection-staging:${NAMESPACE}`;

// Lazy import to avoid circular dep at module load time.
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

async function execute(domain, accountId, intentId, params, retryCount, maxRetries, governance) {
  // Dual-signature shim
  if (typeof domain === 'object' && domain !== null) {
    ({ domain, accountId, intentId, params, retryCount, maxRetries, governance } = domain);
  }

  const startTime = Date.now();
  const emitter = _getEmitter();
  const redis = getRedisClient();

  if (!emitter || !emitter.transition) {
    return _report(governance, { accountId, intentId, domain, startTime, retryCount,
      status: 'failed',
      error: 'observability emitter unavailable',
      errorShape: { category: 'permanent', code: null, retryable: false, retryAfterSeconds: null },
    });
  }

  if (!redis || redis.status !== 'ready') {
    return _report(governance, { accountId, intentId, domain, startTime, retryCount,
      status: 'failed',
      error: 'redis not ready',
      errorShape: { category: 'transient', code: null, retryable: true, retryAfterSeconds: null },
    });
  }

  let entries;
  try {
    entries = await redis.lrange(STAGING_KEY, 0, -1);
  } catch (err) {
    return _report(governance, { accountId, intentId, domain, startTime, retryCount,
      status: 'failed',
      error: `lrange failed: ${err.message}`,
      errorShape: { category: 'transient', code: err.code || null, retryable: true, retryAfterSeconds: null },
    });
  }

  if (entries.length === 0) {
    return _report(governance, { accountId, intentId, domain, startTime, retryCount,
      status: 'completed',
      result: { namespace: NAMESPACE, drained: 0, succeeded: 0, failed: 0, remaining: 0 },
    });
  }

  let succeeded = 0;
  let failed = 0;
  const errors = [];

  for (const serialized of entries) {
    let entry;
    try {
      entry = JSON.parse(serialized);
    } catch (parseErr) {
      // Corrupt entry — skip via LREM to prevent infinite retries
      try { await redis.lrem(STAGING_KEY, 1, serialized); } catch (_) {}
      failed++;
      errors.push({ type: 'parse_error', message: parseErr.message });
      continue;
    }

    try {
      await emitter.transition({
        domain: entry.namespace || NAMESPACE,
        entity: 'projection_intent',
        entityId: entry.projectionType,
        previousState: null,
        nextState: 'PROJECTION_INTENT',
        authority: `telemetry-retry-${NAMESPACE}-worker`,
        raw: {
          intentType: 'PROJECTION_INTENT',
          projectionNamespace: entry.namespace || NAMESPACE,
          projectionType: entry.projectionType,
          projectionVersion: entry.projectionVersion,
          projectionPayload: entry.projectionPayload,
          confidence: entry.confidence,
          integrityScore: entry.integrityScore,
          sourceTelemetryWindow: entry.sourceTelemetryWindow,
          traceId: crypto.randomUUID(),
          correlationId: entry.correlationId,
          parentReference: {
            originalProjectionId: entry.projectionId,
            originalTraceId: entry.traceId,
            retryCount,
            replayedAt: Date.now(),
          },
        },
      });
      // Success — LREM the original serialized value
      await redis.lrem(STAGING_KEY, 1, serialized);
      succeeded++;
    } catch (emitErr) {
      // Failed re-emit — leave entry in place for next retry
      failed++;
      errors.push({ type: 'emit_error', message: emitErr.message });
    }
  }

  const status = failed === 0 ? 'completed' : (succeeded === 0 ? 'failed' : 'partial');

  return _report(governance, { accountId, intentId, domain, startTime, retryCount,
    status,
    result: {
      namespace: NAMESPACE,
      drained: entries.length,
      succeeded,
      failed,
      remaining: failed,
    },
    error: failed > 0 ? `${failed} entries failed re-emit` : null,
    errorShape: failed > 0 ? { category: 'transient', code: null, retryable: true, retryAfterSeconds: null } : null,
    extra: errors.length > 0 ? { errors: errors.slice(0, 5) } : null,
  });
}

function _report(governance, opts) {
  const { accountId, intentId, domain, startTime, retryCount, status, result, error, errorShape, extra } = opts;
  const payload = {
    type: 'WORKER_OUTCOME_REPORTED',
    accountId, intentId, domain,
    status,
    result: result || null,
    error: error || null,
    errorShape: errorShape || null,
    latencyMs: Date.now() - startTime,
    retryCount,
  };
  if (extra) Object.assign(payload, extra);
  if (governance && typeof governance.dispatch === 'function') {
    (governance.dispatchGlobal || governance.dispatch)(payload);
  }
}

module.exports = { execute };
