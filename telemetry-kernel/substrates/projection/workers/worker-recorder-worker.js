// telemetry-kernel/substrates/projection/workers/worker-recorder-worker.js
// Worker Recorder: records all WORKER_EXECUTED transitions to the
// canonical worker ledger (lineage:worker:entries).
//
// This is NOT a projection worker — it does not extend BaseProjectionWorker.
// It subscribes to observability.onWrite() and writes every worker
// execution entry to Redis. The snapshot deriver reads lineage:worker:entries
// to populate worker_count, giving tests a deterministic way to assert
// that workers were invoked by the FSM.

const { getRedisClient } = require('../../../../config/redis');

const WORKER_LEDGER_KEY = 'lineage:worker:entries';

let _unsubscribe = null;
let _startedAt = null;
let _recordedCount = 0;

function getHealth() {
  return {
    workerName: 'worker-recorder',
    running: _unsubscribe !== null,
    recordedCount: _recordedCount,
    uptimeMs: _startedAt ? Date.now() - _startedAt : 0,
  };
}

async function start() {
  if (_unsubscribe) return;
  _startedAt = Date.now();

  try {
    // eslint-disable-next-line global-require
    const observability = require('../../../../control-plane/observability');
    _unsubscribe = observability.onWrite(async (transition) => {
      // Only record worker_execution entries emitted by CK.invokeWorker
      if (transition.entity !== 'worker_execution') return;
      if (transition.nextState !== 'WORKER_EXECUTED') return;

      try {
        const redis = getRedisClient();
        if (!redis || redis.status !== 'ready') return;

        const entry = {
          workerName: transition.raw?.workerName || 'unknown',
          domain: transition.domain || 'unknown',
          accountId: transition.raw?.accountId || null,
          intentId: transition.raw?.intentId || null,
          outcome: transition.raw?.outcome || 'completed',
          invokedAt: transition.raw?.invokedAt || Date.now(),
          recordedAt: Date.now(),
          traceId: transition.traceId || null,
        };
        await redis.rpush(WORKER_LEDGER_KEY, JSON.stringify(entry));
        _recordedCount++;
      } catch (_) {
        // Best-effort — worker recording is non-critical
      }
    });
    console.log('[worker-recorder] Started — subscribing to WORKER_EXECUTED events');
  } catch (err) {
    console.error('[worker-recorder] Failed to start:', err.message);
  }
}

async function stop() {
  if (_unsubscribe) {
    try { _unsubscribe(); } catch (_) {}
    _unsubscribe = null;
  }
  console.log(`[worker-recorder] Stopped — recorded ${_recordedCount} worker executions`);
}

/**
 * Read all recorded worker entries from the Redis ledger.
 * Used by the snapshot deriver to populate worker_count.
 */
async function readAll() {
  const redis = getRedisClient();
  if (!redis || redis.status !== 'ready') return [];
  try {
    const raw = await redis.lrange(WORKER_LEDGER_KEY, 0, -1);
    return raw.map((s) => { try { return JSON.parse(s); } catch { return null; } }).filter(Boolean);
  } catch {
    return [];
  }
}

module.exports = { start, stop, getHealth, readAll, WORKER_LEDGER_KEY };
