// control-plane/lifecycle.js
// Unified Runtime Lifecycle: sole control-plane entry point.
//
// Owns: account discovery, worker pool lifecycle, realtime subscriptions,
//       publishing evaluation, intent emission, operational safety checks.
// Does NOT own: Instagram API, domain-specific execution, persistence writes.
//
// Architecture:
//   Supabase Realtime → signalBus.emit('db:insert', ...)
//                              ↓
//                     bufferEvent() — accumulates, debounces 500ms per account
//                              ↓
//                     _evaluateAndEmit(accountId)
//                       ├─ publishingPolicy.evaluateRecord()
//                       ├─ dedup.isInFlight()
//                       ├─ mutationSubstrate.applyMutation()  (mark_failed)
//                       └─ _emitIntents() → Redis LPUSH → worker BRPOP
//
//   3-min background loop:
//     → _refreshWorkerPool()   — spawn/stop per-account domain workers
//     → _runOperationalChecks() — heartbeat failover (governance safety net)

const { getRedisClient } = require('../config/redis');
const { getActiveAccounts } = require('../substrates/persistence');
const { proactiveHeartbeatFailover } = require('../services/sync');
const realtime = require('../substrates/realtime');
const signalBus = require('./signal-bus');
const dedup = require('./governance/dedup');
const mutationSubstrate = require('./mutation-substrate');
const publishingPolicy = require('./policies/publishing');

// Domain worker constructors — each exports startWorker(accountId, signal)
const commentsWorker  = require('../workers/comments-worker');
const messagesWorker  = require('../workers/messages-worker');
const mediaWorker     = require('../workers/media-worker');
const insightsWorker  = require('../workers/insights-worker');
const ugcWorker       = require('../workers/ugc-worker');
const publishWorker   = require('../workers/publish-worker');

// ── Constants ────────────────────────────────────────────────────────────────

const ACCOUNT_REFRESH_INTERVAL_MS = 3 * 60 * 1000; // 3 min (was 5)
const EVALUATION_DEBOUNCE_MS = 500;
const RESULT_TTL_SEC = 3600;

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

/**
 * accountId → [
 *   { table: string, record: object, receivedAt: number }
 * ]
 * Event buffer for publishing evaluation. Events are drained on debounced
 * evaluation — no 10s poll, evaluation triggers on signal arrival.
 */
const _buffer = new Map(); // accountId → event[]

/** Per-account debounce timers for event-driven evaluation. */
const _debounceTimers = new Map(); // accountId → Timeout

// ── Publishing evaluation (collapsed from evaluator.js) ──────────────────────

function domainForAction(actionType) {
  if (actionType === 'publish_post') return 'media';
  if (actionType === 'repost_ugc') return 'ugc';
  return 'messaging';
}

function fetchTypeForAction(actionType) {
  if (actionType === 'publish_media') return 'publish_media';
  if (actionType === 'publish_ugc') return 'publish_ugc';
  if (actionType === 'publish_messaging') return 'publish_messaging';
  if (actionType === 'publish_post') return 'publish_media';
  if (actionType === 'repost_ugc') return 'publish_ugc';
  return 'publish_messaging';
}

/**
 * Called by signalBus when a db:insert event fires.
 * Accumulates the event into buffer and triggers debounced evaluation
 * per account — replaces the old 10s evaluator poll loop.
 */
function bufferEvent(data) {
  const { accountId, table, record } = data;
  if (!accountId || !table || !record) return;

  if (!_buffer.has(accountId)) {
    _buffer.set(accountId, []);
  }
  _buffer.get(accountId).push({ table, record, receivedAt: Date.now() });

  // Event-driven: reset debounce timer per account.
  // Rapid inserts on the same account batch into a single evaluation cycle.
  if (_debounceTimers.has(accountId)) {
    clearTimeout(_debounceTimers.get(accountId));
  }
  _debounceTimers.set(accountId, setTimeout(() => {
    _debounceTimers.delete(accountId);
    _evaluateAndEmit(accountId).catch(err =>
      console.error(`[lifecycle] Evaluation error for account ${accountId}:`, err.message)
    );
  }, EVALUATION_DEBOUNCE_MS));
}

/**
 * Evaluates all buffered events for one account.
 * Delegates business logic to publishingPolicy.
 * Handles mutation outcomes via mutationSubstrate.
 * Emits valid intents to Redis queues.
 */
async function _evaluateAndEmit(accountId) {
  const redis = getRedisClient();
  if (!redis || redis.status !== 'ready') return;

  const events = _buffer.get(accountId);
  if (!events || events.length === 0) return;

  _buffer.delete(accountId);

  const intents = [];
  for (const { table, record } of events) {
    const outcome = publishingPolicy.evaluateRecord(table, record);

    if (outcome.action === 'skip') {
      continue;
    }

    if (outcome.action === 'mark_failed') {
      await mutationSubstrate.applyMutation(
        table,
        record.id,
        outcome.updates,
        outcome.reason
      );
      continue;
    }

    if (outcome.action === 'emit') {
      const { intent } = outcome;
      const resourceId = record.id;

      if (dedup.isInFlight(accountId, intent.action_type, resourceId)) {
        continue;
      }
      dedup.markInFlight(accountId, intent.action_type, resourceId);

      intents.push({
        account_id: accountId,
        fetch_type: fetchTypeForAction(intent.action_type),
        action_type: intent.action_type,
        resource_id: resourceId,
        payload: intent.payload,
        queue_row_id: intent.queue_row_id || null,
        scheduled_post_id: intent.scheduled_post_id || null,
        intent_type: table === 'scheduled_posts' ? 'scheduled_post' : 'post_queue',
      });
    }
  }

  if (intents.length > 0) {
    await _emitIntents(redis, intents);
  }
  dedup.clearTick();
}

/**
 * Emits intents to Redis publish queues (LPUSH for each domain).
 * Each intent gets a result key for worker consumption observability.
 */
async function _emitIntents(redis, intents) {
  for (const intent of intents) {
    const { account_id, fetch_type, action_type, resource_id, payload, queue_row_id, scheduled_post_id, intent_type } = intent;

    const intent_id = require('crypto').randomUUID();
    const domain = domainForAction(action_type);

    const queueIntent = {
      intent_id,
      account_id,
      fetch_type,
      action_type,
      payload,
      priority: 'normal',
      issued_at: new Date().toISOString(),
      queue_row_id: queue_row_id || null,
      scheduled_post_id: scheduled_post_id || payload?.scheduled_post_id || null,
      intent_type,
    };

    const queueKey = `supervisor:acquisitions:publish:${domain}:${account_id}`;
    await redis.lpush(queueKey, JSON.stringify(queueIntent));

    const resultKey = `supervisor:acquisition_results:publish:${domain}:${account_id}:${intent_id}`;
    await redis.set(resultKey, JSON.stringify({
      intent_id,
      account_id,
      action_type,
      domain,
      status: 'queued',
      resource_id,
      queued_at: new Date().toISOString(),
    }), 'EX', RESULT_TTL_SEC);

    console.log(`[lifecycle] Emitted publish:${domain} intent ${intent_id} for ${intent_type} ${resource_id}`);
  }
}

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
    console.log(`[lifecycle] Spawned ${domain} worker for account ${accountId}`);
  }

  _workerPools.set(accountId, entry);
}

function _stopDomainWorkers(accountId) {
  const entry = _workerPools.get(accountId);
  if (!entry) return;

  for (const [domain, { controller }] of Object.entries(entry)) {
    console.log(`[lifecycle] Stopping ${domain} worker for account ${accountId}`);
    controller.abort();
  }
  _workerPools.delete(accountId);
}

// ── Pool refresh ─────────────────────────────────────────────────────────────

async function _refreshWorkerPool() {
  if (_stopping) return;

  const redis = getRedisClient();
  if (!redis || redis.status !== 'ready') {
    console.warn('[lifecycle] Redis not ready — skipping worker pool refresh');
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
      console.log(`[lifecycle] Removing workers for deactivated account ${accountId}`);
      _stopDomainWorkers(accountId);

      // Clean up buffered events and debounce timers for removed accounts
      _buffer.delete(accountId);
      const timer = _debounceTimers.get(accountId);
      if (timer) {
        clearTimeout(timer);
        _debounceTimers.delete(accountId);
      }
    }
  }
}

// ── Operational checks ───────────────────────────────────────────────────────

/**
 * TODO: extract proactiveHeartbeatFailover out of lifecycle.
 * Heartbeat failover is a governance decision (moving posts to queue when
 * agent is down), not an operational concern. It should live in a dedicated
 * safety-net module or the agent repo's supervisor.
 */
async function _runOperationalChecks() {
  const supabase = require('../config/supabase').getSupabaseAdmin();
  if (!supabase) return;

  const HEARTBEAT_STALE_MINUTES = parseInt(process.env.HEARTBEAT_STALE_MINUTES || '30', 10);

  try {
    await proactiveHeartbeatFailover(supabase, HEARTBEAT_STALE_MINUTES);
  } catch (err) {
    console.error('[lifecycle] Heartbeat failover error:', err.message);
  }
}

// ── Sleep ────────────────────────────────────────────────────────────────────

function _sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ── Public API ───────────────────────────────────────────────────────────────

async function startAllWorkers() {
  if (_stopping) return;
  console.log('[lifecycle] Starting all domain workers...');

  const redis = getRedisClient();
  if (!redis) {
    console.error('[lifecycle] Redis unavailable — cannot start workers');
    return;
  }

  // ── Subscribe to signal bus for event-driven publishing evaluation ─────
  signalBus.subscribe('db:insert', bufferEvent);

  // ── Start realtime subscriptions (Supabase WebSocket → signal bus) ─────
  const accounts = await getActiveAccounts();
  await realtime.startRealtime(accounts);
  console.log(`[lifecycle] Realtime subscriptions started for ${accounts.length} account(s)`);

  // Initial pool
  await _refreshWorkerPool();

  // 3-minute background loop: pool refresh + operational safety checks
  const refreshLoop = async () => {
    while (!_stopping) {
      await _sleep(ACCOUNT_REFRESH_INTERVAL_MS);
      if (_stopping) break;
      await _refreshWorkerPool().catch(err =>
        console.error('[lifecycle] Refresh error:', err.message)
      );
      await _runOperationalChecks().catch(err =>
        console.error('[lifecycle] Operational check error:', err.message)
      );
    }
  };

  refreshLoop().catch(err =>
    console.error('[lifecycle] Refresh loop crashed:', err.message)
  );

  const totalWorkers = [..._workerPools.values()].reduce((sum, e) => sum + Object.keys(e).length, 0);
  console.log(`[lifecycle] Started ${totalWorkers} domain workers across ${_workerPools.size} accounts`);
}

async function stopAllWorkers() {
  console.log('[lifecycle] Stopping all workers...');
  _stopping = true;

  // Unsubscribe from signal bus
  signalBus.unsubscribe('db:insert', bufferEvent);

  // Clear all debounce timers and buffers
  for (const timer of _debounceTimers.values()) {
    clearTimeout(timer);
  }
  _debounceTimers.clear();
  _buffer.clear();

  // Stop realtime subscriptions
  await realtime.stopRealtime();

  // Stop all domain workers
  for (const accountId of _workerPools.keys()) {
    _stopDomainWorkers(accountId);
  }

  console.log('[lifecycle] All workers stopped');
}

module.exports = { startAllWorkers, stopAllWorkers };
