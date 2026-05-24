// control-plane/runtime/lifecycle.js
// Worker Pool Lifecycle: bounded worker pool management.
//
// Owns: spawning/stopping per-account domain workers, periodic pool refresh,
//        account discovery for pool membership.
// Does NOT own: evaluation, emission, signal intake, operational safety.
//
// Contract:
//   workerPool.spawn(accountId)    → start 6 domain workers for account
//   workerPool.stop(accountId)     → abort workers for account
//   workerPool.refresh()           → discover accounts, spawn new, stop removed
//   workerPool.stopAll()           → stop all workers
//   workerPool.size()              → total worker count

const { getRedisClient } = require('../../config/redis');
const { getActiveAccounts } = require('../../substrates/persistence');

// Domain worker constructors — each exports startWorker(accountId, signal)
const commentsWorker  = require('../../workers/comments-worker');
const messagesWorker  = require('../../workers/messages-worker');
const mediaWorker     = require('../../workers/media-worker');
const insightsWorker  = require('../../workers/insights-worker');
const ugcWorker       = require('../../workers/ugc-worker');
const publishWorker   = require('../../workers/publish-worker');

const DOMAIN_WORKERS = [
  ['comments', commentsWorker],
  ['messages', messagesWorker],
  ['media', mediaWorker],
  ['insights', insightsWorker],
  ['ugc', ugcWorker],
  ['publish', publishWorker],
];

/**
 * accountId → {
 *   comments:  { task: Promise, controller: AbortController },
 *   messages:  { task: Promise, controller: AbortController },
 *   ...
 * }
 */
const _workerPools = new Map();

/** Called when an account is removed so other modules can clean up. */
let _onRemove = null;

/**
 * Returns live runtime state. Deterministic, no side effects.
 * @returns {{ accounts: number, totalWorkers: number }}
 */
function status() {
  let totalWorkers = 0;
  for (const entry of _workerPools.values()) {
    totalWorkers += Object.keys(entry).length;
  }
  return {
    accounts: _workerPools.size,
    totalWorkers,
  };
}

/**
 * Register a callback invoked when an account is removed during refresh.
 * @param {Function} fn — (accountId: string) => void
 * @throws {Error} if fn is not a function
 */
function onRemove(fn) {
  if (typeof fn !== 'function') {
    throw new Error(`[worker-pool] onRemove handler must be a function, got ${typeof fn}`);
  }
  _onRemove = fn;
}

/**
 * Spawn all 6 domain workers for one account.
 * Idempotent — calling for an already-spawned account is a no-op.
 * @param {string} accountId
 * @throws {Error} if accountId is not a non-empty string
 */
function spawn(accountId) {
  if (typeof accountId !== 'string' || !accountId) {
    throw new Error(`[worker-pool] accountId must be a non-empty string, got ${typeof accountId}`);
  }
  if (_workerPools.has(accountId)) return;

  const entry = {};
  for (const [domain, worker] of DOMAIN_WORKERS) {
    const controller = new AbortController();
    const task = worker.startWorker(accountId, controller.signal);
    entry[domain] = { task, controller };
  }
  _workerPools.set(accountId, entry);
  console.log(`[worker-pool] Spawned 6 domain workers for account ${accountId}`);
}

/**
 * Abort all domain workers for one account.
 * Idempotent — calling for an unknown account is a no-op.
 * @param {string} accountId
 */
function stop(accountId) {
  const entry = _workerPools.get(accountId);
  if (!entry) return;

  for (const [domain, { controller }] of Object.entries(entry)) {
    controller.abort();
  }
  _workerPools.delete(accountId);
  console.log(`[worker-pool] Stopped workers for account ${accountId}`);
}

/**
 * Discover active accounts, spawn workers for new ones, stop workers for
 * removed ones. Notifies the onRemove callback for cleanup in other modules.
 *
 * @returns {Promise<{ok: boolean, error?: string, spawned: number, stopped: number}>}
 *   ok=false when Redis is unavailable (refresh skipped). spawned/stopped are 0.
 */
async function refresh() {
  const redis = getRedisClient();
  if (!redis || redis.status !== 'ready') {
    console.warn('[worker-pool] Redis not ready — skipping refresh');
    return { ok: false, error: 'redis unavailable', spawned: 0, stopped: 0 };
  }

  const accounts = await getActiveAccounts();
  const currentIds = new Set(accounts.map(a => a.id));

  let spawned = 0;
  let stopped = 0;

  // Spawn workers for new accounts
  for (const accountId of currentIds) {
    if (!_workerPools.has(accountId)) {
      spawn(accountId);
      spawned++;
    }
  }

  // Stop workers for removed accounts
  for (const accountId of _workerPools.keys()) {
    if (!currentIds.has(accountId)) {
      stop(accountId);
      stopped++;
      if (_onRemove) _onRemove(accountId);
    }
  }

  return { ok: true, spawned, stopped };
}

/**
 * Stop all workers across all accounts.
 */
function stopAll() {
  for (const accountId of _workerPools.keys()) {
    stop(accountId);
  }
  console.log('[worker-pool] All workers stopped');
}

/**
 * Return the total number of running workers.
 */
function size() {
  return [..._workerPools.values()].reduce((sum, e) => sum + Object.keys(e).length, 0);
}

module.exports = { status, spawn, stop, refresh, stopAll, size, onRemove };
