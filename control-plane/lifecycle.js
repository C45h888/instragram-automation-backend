// control-plane/lifecycle.js
// Control Plane: worker pool governance.
//
// Owns: discovering active accounts, spawning/stopping per-domain per-account
//        BRPOP workers, periodic refresh, operational checks.
// Does NOT own: individual domain execution, Instagram API, persistence.
//
// Each active account gets 5 domain workers — one per Instagram API domain.
// A rate limit on comments never blocks messages. Quota exhaustion on
// insights never delays UGC discovery.

const { getRedisClient } = require('../config/redis');
const { getActiveAccounts } = require('../substrates/persistence');
const { proactiveHeartbeatFailover } = require('../services/sync');
const evaluator = require('../control-plane/evaluator');
const realtime = require('../substrates/realtime');

// Domain worker constructors — each exports startWorker(accountId, signal)
const commentsWorker  = require('../workers/comments-worker');
const messagesWorker  = require('../workers/messages-worker');
const mediaWorker     = require('../workers/media-worker');
const insightsWorker  = require('../workers/insights-worker');
const ugcWorker       = require('../workers/ugc-worker');
const publishWorker   = require('../workers/publish-worker');

// ── Constants ────────────────────────────────────────────────────────────────

const ACCOUNT_REFRESH_INTERVAL_MS = 5 * 60 * 1000; // 5 min
const WORKER_RECONNECT_DELAY_MS   = 5000;

// ── Module state ─────────────────────────────────────────────────────────────

/**
 * accountId → {
 *   comments:  { task: Promise, controller: AbortController },
 *   messages:  { task: Promise, controller: AbortController },
 *   media:     { task: Promise, controller: AbortController },
 *   insights:  { task: Promise, controller: AbortController },
 *   ugc:       { task: Promise, controller: AbortController },
 *   publish:   { task: Promise, controller: AbortController },
 * }
 */
let _workerPools = new Map();
let _stopping = false;

// ── Worker lifecycle ─────────────────────────────────────────────────────────

function _spawnDomainWorkers(accountId) {
  const entry = {};

  for (const [domain, worker] of [
    ['comments', commentsWorker],
    ['messages', messagesWorker],
    ['media', mediaWorker],
    ['insights', insightsWorker],
    ['ugc', ugcWorker],
    ['publish', publishWorker],
  ]) {
    const controller = new AbortController();
    const task = worker.startWorker(accountId, controller.signal);
    entry[domain] = { task, controller };
    console.log(`[Lifecycle] Spawned ${domain} worker for account ${accountId}`);
  }

  _workerPools.set(accountId, entry);
}

function _stopDomainWorkers(accountId) {
  const entry = _workerPools.get(accountId);
  if (!entry) return;

  for (const [domain, { controller }] of Object.entries(entry)) {
    console.log(`[Lifecycle] Stopping ${domain} worker for account ${accountId}`);
    controller.abort();
  }
  _workerPools.delete(accountId);
}

// ── Pool refresh ─────────────────────────────────────────────────────────────

async function _refreshWorkerPool() {
  if (_stopping) return;

  const redis = getRedisClient();
  if (!redis || redis.status !== 'ready') {
    console.warn('[Lifecycle] Redis not ready — skipping worker pool refresh');
    return;
  }

  const accounts = await getActiveAccounts();
  const currentIds = new Set(accounts.map(a => a.id));

  // Spawn workers for new accounts
  for (const accountId of currentIds) {
    if (!_workerPools.has(accountId)) {
      _spawnDomainWorkers(accountId);
    }
  }

  // Stop workers for removed accounts
  for (const accountId of _workerPools.keys()) {
    if (!currentIds.has(accountId)) {
      console.log(`[Lifecycle] Removing workers for deactivated account ${accountId}`);
      _stopDomainWorkers(accountId);
    }
  }
}

// ── Operational checks ───────────────────────────────────────────────────────

async function _runOperationalChecks() {
  const supabase = require('../config/supabase').getSupabaseAdmin();
  if (!supabase) return;

  const HEARTBEAT_STALE_MINUTES = parseInt(process.env.HEARTBEAT_STALE_MINUTES || '30', 10);

  try {
    await proactiveHeartbeatFailover(supabase, HEARTBEAT_STALE_MINUTES);
  } catch (err) {
    console.error('[Lifecycle] Heartbeat failover error:', err.message);
  }
}

// ── Sleep ────────────────────────────────────────────────────────────────────

function _sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ── Public API ───────────────────────────────────────────────────────────────

async function startAllWorkers() {
  if (_stopping) return;
  console.log('[Lifecycle] Starting all domain workers...');

  const redis = getRedisClient();
  if (!redis) {
    console.error('[Lifecycle] Redis unavailable — cannot start workers');
    return;
  }

  // ── Start evaluator (10s loop: buffers realtime events, emits publish intents) ──
  await evaluator.startEvaluator();

  // ── Start realtime subscriptions for all active accounts ────────────────
  const accounts = await getActiveAccounts();
  await realtime.startRealtime(accounts);
  console.log(`[Lifecycle] Realtime subscriptions started for ${accounts.length} account(s)`);

  // Initial pool
  await _refreshWorkerPool();

  // Background refresh loop
  const refreshLoop = async () => {
    while (!_stopping) {
      await _sleep(ACCOUNT_REFRESH_INTERVAL_MS);
      if (_stopping) break;
      await _refreshWorkerPool().catch(err =>
        console.error('[Lifecycle] Refresh error:', err.message)
      );
      await _runOperationalChecks().catch(err =>
        console.error('[Lifecycle] Operational check error:', err.message)
      );
    }
  };

  refreshLoop().catch(err =>
    console.error('[Lifecycle] Refresh loop crashed:', err.message)
  );

  const totalWorkers = [..._workerPools.values()].reduce((sum, e) => sum + Object.keys(e).length, 0);
  console.log(`[Lifecycle] Started ${totalWorkers} domain workers across ${_workerPools.size} accounts`);
}

async function stopAllWorkers() {
  console.log('[Lifecycle] Stopping all workers...');
  _stopping = true;

  // Stop evaluator loop and realtime subscriptions
  evaluator.stopEvaluator();
  await realtime.stopRealtime();

  for (const accountId of _workerPools.keys()) {
    _stopDomainWorkers(accountId);
  }

  console.log('[Lifecycle] All workers stopped');
}

module.exports = { startAllWorkers, stopAllWorkers };
