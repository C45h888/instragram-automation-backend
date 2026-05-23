// backend/workers/acquisition-worker.js
// Redis-driven AcquisitionWorker — passive consumer of AcquisitionIntents.
//
// Spawned by server.js when ACQUISITION_MODE=redis.
// Scans instagram_business_accounts for active accounts and spawns one
// per-account BRPOP loop. Each loop consumes from:
//   supervisor:acquisitions:{account_id}
//
// On receiving an intent, routes by fetch_type to the corresponding
// scoped domain sync function, then writes the result to:
//   supervisor:acquisition_results:{account_id}:{intent_id}
//
// This replaces the agent's Python AcquisitionWorker calling the backend
// via HTTP. The backend now owns acquisition execution directly.

const { getRedisClient } = require('../config/redis');
const { getSupabaseAdmin } = require('../config/supabase');
const { validateIntent } = require('../contracts/acquisition-intents');
const {
  syncCommentsForAccount,
  syncEngagementForAccount,
  syncUgcForAccount,
  syncMediaForAccount,
  syncInsightsForAccount,
  proactiveHeartbeatFailover,
} = require('../services/sync');

// ── Constants ────────────────────────────────────────────────────────────────

const REDIS_QUEUE_PREFIX = 'supervisor:acquisitions:';
const REDIS_RESULT_PREFIX = 'supervisor:acquisition_results:';
const BRPOP_TIMEOUT_SEC = 30;
const RESULT_TTL_SEC = 3600; // 1 hour
const ACCOUNT_REFRESH_INTERVAL_MS = 5 * 60 * 1000; // refresh account list every 5 min
const WORKER_RECONNECT_DELAY_MS = 5000;

// ── Module state ─────────────────────────────────────────────────────────────

let _workerLoops = new Map();  // accountId → { task: Promise, abort: AbortController }
let _stopping = false;

// ── Intent router ────────────────────────────────────────────────────────────

/**
 * Maps a fetch_type to the corresponding scoped domain function.
 * Returns null for unknown types.
 *
 * @param {string} fetchType
 * @returns {Function|null}
 */
function _resolveHandler(fetchType) {
  const map = {
    account_insights: syncInsightsForAccount,
    media_insights:  syncMediaForAccount,  // media_insights → media sync (insights + media are co-located)
    ugc_discovery:   syncUgcForAccount,
    post_comments:   syncCommentsForAccount,
  };
  return map[fetchType] || null;
}

// ── Result writer ────────────────────────────────────────────────────────────

/**
 * Writes the acquisition result to Redis for the agent to consume.
 *
 * Key: supervisor:acquisition_results:{account_id}:{intent_id}
 * TTL: 1 hour
 *
 * @param {import('ioredis').Redis} redis
 * @param {string} accountId
 * @param {string} intentId
 * @param {string} fetchType
 * @param {'completed'|'failed'} status
 * @param {object|null} resultData
 * @param {string|null} error
 */
async function _writeResult(redis, accountId, intentId, fetchType, status, resultData, error) {
  const key = `${REDIS_RESULT_PREFIX}${accountId}:${intentId}`;
  const payload = {
    intent_id: intentId,
    account_id: accountId,
    fetch_type: fetchType,
    status,
    result: resultData || null,
    error: error || null,
    completed_at: new Date().toISOString(),
  };

  try {
    await redis.set(key, JSON.stringify(payload), 'EX', RESULT_TTL_SEC);
    console.log(`[AcquisitionWorker] Result written: ${key} status=${status}`);
  } catch (err) {
    console.error(`[AcquisitionWorker] Failed to write result ${key}:`, err.message);
  }
}

// ── Per-account loop ─────────────────────────────────────────────────────────

/**
 * Persistent BRPOP loop for a single account.
 * Consumes AcquisitionIntents from supervisor:acquisitions:{account_id}.
 * Runs until abort signal is triggered.
 *
 * @param {string} accountId
 * @param {AbortSignal} signal
 */
async function _runAccountLoop(accountId, signal) {
  const queueKey = `${REDIS_QUEUE_PREFIX}${accountId}`;
  console.log(`[AcquisitionWorker] Starting loop for account ${accountId} on ${queueKey}`);

  while (!signal.aborted) {
    let redis;
    try {
      redis = getRedisClient();
      if (!redis || redis.status !== 'ready') {
        console.warn(`[AcquisitionWorker] Redis not ready for ${accountId}, waiting ${WORKER_RECONNECT_DELAY_MS}ms...`);
        await _sleep(WORKER_RECONNECT_DELAY_MS, signal);
        continue;
      }

      // BRPOP — blocks up to BRPOP_TIMEOUT_SEC, returns [key, value] or null on timeout
      const result = await redis.brpop(queueKey, BRPOP_TIMEOUT_SEC);

      if (signal.aborted) break;
      if (!result) continue; // timeout — no intent received, loop again

      const [, raw] = result;
      let intent;

      try {
        intent = JSON.parse(raw);
      } catch {
        console.error(`[AcquisitionWorker] ${accountId}: invalid JSON in queue, skipping`);
        continue;
      }

      const validation = validateIntent(intent);
      if (!validation.valid) {
        console.error(`[AcquisitionWorker] ${accountId}: invalid intent — ${validation.error}`, intent);
        continue;
      }

      const { intent_id, fetch_type, priority } = intent;
      console.log(`[AcquisitionWorker] ${accountId}: received intent ${intent_id} (${fetch_type}, ${priority || 'normal'})`);

      const handler = _resolveHandler(fetch_type);
      if (!handler) {
        console.warn(`[AcquisitionWorker] ${accountId}: no handler for fetch_type=${fetch_type}, writing failed result`);
        await _writeResult(redis, accountId, intent_id, fetch_type, 'failed', null, `unknown_fetch_type: ${fetch_type}`);
        continue;
      }

      // Execute the scoped domain sync function
      const startTime = Date.now();
      let syncResult;
      try {
        syncResult = await handler(accountId, intent.parameters || {});
      } catch (handlerErr) {
        console.error(`[AcquisitionWorker] ${accountId}: handler threw for ${intent_id}:`, handlerErr.message);
        syncResult = { success: false, count: 0, error: handlerErr.message };
      }
      const latencyMs = Date.now() - startTime;

      const status = syncResult.success ? 'completed' : 'failed';
      await _writeResult(redis, accountId, intent_id, fetch_type, status, {
        count: syncResult.count,
        latency_ms: latencyMs,
      }, syncResult.error);

      console.log(`[AcquisitionWorker] ${accountId}: intent ${intent_id} ${status} (${syncResult.count} items, ${latencyMs}ms)`);

    } catch (err) {
      if (signal.aborted) break;
      console.error(`[AcquisitionWorker] ${accountId}: loop error:`, err.message);
      await _sleep(WORKER_RECONNECT_DELAY_MS, signal);
    }
  }

  console.log(`[AcquisitionWorker] Loop stopped for account ${accountId}`);
}

/**
 * Sleep for ms milliseconds, respecting the abort signal.
 *
 * @param {number} ms
 * @param {AbortSignal} signal
 */
function _sleep(ms, signal) {
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, ms);
    const onAbort = () => { clearTimeout(timer); resolve(); };
    signal.addEventListener('abort', onAbort, { once: true });
  });
}

// ── Account discovery ────────────────────────────────────────────────────────

/**
 * Fetches active accounts from instagram_business_accounts.
 * Cached — refresh logic lives in the account refresh loop.
 *
 * @returns {Promise<Array<{id: string}>>}
 */
async function _getActiveAccounts() {
  const supabase = getSupabaseAdmin();
  if (!supabase) {
    console.error('[AcquisitionWorker] Supabase not available — cannot discover accounts');
    return [];
  }

  const { data, error } = await supabase
    .from('instagram_business_accounts')
    .select('id')
    .eq('is_active', true);

  if (error) {
    console.error('[AcquisitionWorker] Failed to fetch active accounts:', error.message);
    return [];
  }

  return data || [];
}

/**
 * Periodically refreshes the worker pool — starts loops for new accounts,
 * stops loops for removed accounts.
 */
async function _refreshWorkerPool() {
  if (_stopping) return;

  const accounts = await _getActiveAccounts();
  const currentIds = new Set(accounts.map(a => a.id));

  // Start loops for new accounts
  for (const accountId of currentIds) {
    if (!_workerLoops.has(accountId)) {
      const controller = new AbortController();
      const task = _runAccountLoop(accountId, controller.signal);
      _workerLoops.set(accountId, { task, controller });
      console.log(`[AcquisitionWorker] Spawned worker for account ${accountId}`);
    }
  }

  // Stop loops for accounts that are no longer active
  for (const [accountId, { controller }] of _workerLoops) {
    if (!currentIds.has(accountId)) {
      console.log(`[AcquisitionWorker] Stopping worker for removed account ${accountId}`);
      controller.abort();
      _workerLoops.delete(accountId);
    }
  }
}

// ── Operational checks ──────────────────────────────────────────────────────

/**
 * Runs operational checks: heartbeat failover.
 * Called once per refresh interval alongside the worker pool refresh.
 * Non-fatal — errors are logged, not thrown.
 */
async function _runOperationalChecks() {
  const supabase = getSupabaseAdmin();
  if (!supabase) return;

  const HEARTBEAT_STALE_MINUTES = parseInt(process.env.HEARTBEAT_STALE_MINUTES || '30', 10);

  try {
    await proactiveHeartbeatFailover(supabase, HEARTBEAT_STALE_MINUTES);
  } catch (err) {
    console.error('[AcquisitionWorker] Heartbeat failover error:', err.message);
  }
}

/**
 * Starts all acquisition workers — one per active account.
 * Spawns a background refresh loop that periodically syncs the worker pool
 * with the current set of active accounts in the DB.
 *
 * @returns {Promise<void>}
 */
async function startAcquisitionWorkers() {
  if (_stopping) return;
  console.log('[AcquisitionWorker] Starting...');

  const redis = getRedisClient();
  if (!redis) {
    console.error('[AcquisitionWorker] Redis unavailable — cannot start');
    return;
  }

  // Initial pool
  await _refreshWorkerPool();

  // Background refresh loop — also runs operational checks (heartbeat failover, stale domains)
  const refreshLoop = async () => {
    while (!_stopping) {
      await _sleep(ACCOUNT_REFRESH_INTERVAL_MS, new AbortController().signal);
      if (_stopping) break;
      await _refreshWorkerPool().catch(err =>
        console.error('[AcquisitionWorker] Refresh error:', err.message)
      );
      await _runOperationalChecks().catch(err =>
        console.error('[AcquisitionWorker] Operational checks error:', err.message)
      );
    }
  };

  // Fire-and-forget the refresh loop (runs in background)
  refreshLoop().catch(err =>
    console.error('[AcquisitionWorker] Refresh loop crashed:', err.message)
  );

  const workerCount = _workerLoops.size;
  console.log(`[AcquisitionWorker] Started ${workerCount} account workers`);
}

/**
 * Gracefully stops all acquisition workers.
 * Aborts all per-account loops and waits for them to finish.
 *
 * @returns {Promise<void>}
 */
async function stopAcquisitionWorkers() {
  console.log('[AcquisitionWorker] Stopping all workers...');
  _stopping = true;

  const entries = Array.from(_workerLoops.entries());
  for (const [accountId, { controller, task }] of entries) {
    console.log(`[AcquisitionWorker] Stopping worker for ${accountId}`);
    controller.abort();
    try {
      await task;
    } catch {
      // task may have already completed
    }
    _workerLoops.delete(accountId);
  }

  console.log('[AcquisitionWorker] All workers stopped');
}

module.exports = { startAcquisitionWorkers, stopAcquisitionWorkers };
