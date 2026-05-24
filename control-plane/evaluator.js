// control-plane/evaluator.js
// Control Plane: deterministic publishing evaluation loop.
//
// Owns: buffering events via signal bus, 10s evaluation cadence,
//        intent emission to Redis queues.
// Does NOT own: IG API calls, persistence writes, retry logic,
//               business-domain evaluation semantics, deduplication semantics.
//
// Architecture:
//   Supabase Realtime ──► signalBus.emit('db:insert', {...})
//                              ↓
//                     signalBus (decoupled bus)
//                              ↓
//                     bufferEvent() [subscribed]
//                              ↓
//                     10s tick ──► _evaluateAccount()
//                                          │
//                     ┌────────────────────┴────────────────────┐
//                     │  policy.evaluateRecord() — business logic  │ ← policies/
//                     │  dedup.isInFlight()        — dedup         │ ← governance/
//                     │  mutationSubstrate.apply() — state mut.    │ ← mutation-substrate
//                     └────────────────────┬────────────────────┘
//                                          │
//                     emitIntent() ──► Redis queues
//
// Only one intent per (account, action_type, resourceId) per tick.

const { getRedisClient } = require('../config/redis');
const { getSupabaseAdmin } = require('../config/supabase');
const { getActiveAccounts } = require('../substrates/persistence');
const signalBus = require('./signal-bus');
const dedup = require('./governance/dedup');
const mutationSubstrate = require('./mutation-substrate');
const publishingPolicy = require('./policies/publishing');

const EVALUATOR_INTERVAL_MS = parseInt(process.env.EVALUATOR_INTERVAL_MS || '10000', 10); // 10s
const RESULT_TTL_SEC = 3600;

// ── Action type → publish domain mapping ─────────────────────────────────────

function domainForAction(actionType) {
  if (actionType === 'publish_post') return 'media';
  if (actionType === 'repost_ugc') return 'ugc';
  return 'messaging';
}

function fetchTypeForAction(actionType) {
  if (actionType === 'publish_media') return 'publish_media';
  if (actionType === 'publish_ugc') return 'publish_ugc';
  if (actionType === 'publish_messaging') return 'publish_messaging';
  // Fallback for raw action types
  if (actionType === 'publish_post') return 'publish_media';
  if (actionType === 'repost_ugc') return 'publish_ugc';
  return 'publish_messaging';
}

// ── Event buffer ──────────────────────────────────────────────────────────────

/**
 * accountId → [
 *   { table: string, record: object, receivedAt: number }
 * ]
 * Buffered events accumulate between ticks. Cleared after each evaluation.
 */
const _buffer = new Map(); // accountId → event[]

/**
 * Called by signalBus when a db:insert event fires.
 * Accumulates events into the buffer for the next evaluation tick.
 */
function bufferEvent(data) {
  const { accountId, table, record } = data;
  if (!accountId || !table || !record) return;
  if (!_buffer.has(accountId)) {
    _buffer.set(accountId, []);
  }
  _buffer.get(accountId).push({ table, record, receivedAt: Date.now() });
}

// ── Evaluation ────────────────────────────────────────────────────────────────

/**
 * Evaluates buffered events for one account.
 * Delegates business logic to publishingPolicy.
 * Handles mutation outcomes via mutationSubstrate.
 * Returns an array of intent objects to emit.
 */
async function _evaluateAccount(accountId, events) {
  const intents = [];
  const supabase = getSupabaseAdmin();

  for (const { table, record } of events) {
    const outcome = publishingPolicy.evaluateRecord(table, record);

    if (outcome.action === 'skip') {
      continue;
    }

    if (outcome.action === 'mark_failed') {
      // Delegate state mutation to mutation substrate — evaluator does NOT mutate persistence
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

      // Check dedup via governance substrate
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

  return intents;
}

// ── Intent emission ──────────────────────────────────────────────────────────

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

    console.log(`[evaluator] Emitted publish:${domain} intent ${intent_id} for ${intent_type} ${resource_id}`);
  }
}

// ── Tick loop ────────────────────────────────────────────────────────────────

async function _tick() {
  const redis = getRedisClient();
  if (!redis || redis.status !== 'ready') {
    console.warn('[evaluator] Redis not ready — skipping tick');
    return;
  }

  const accounts = await getActiveAccounts();
  const accountIds = new Set(accounts.map(a => a.id));

  // Prune buffer entries for de-activated accounts
  for (const bufferedAccountId of _buffer.keys()) {
    if (!accountIds.has(bufferedAccountId)) {
      _buffer.delete(bufferedAccountId);
    }
  }

  // Evaluate each account's buffered events
  // NOTE: Serial evaluation — bounded deterministic concurrency is future work.
  for (const [accountId, events] of _buffer.entries()) {
    if (events.length === 0) continue;

    try {
      const intents = await _evaluateAccount(accountId, events);
      if (intents.length > 0) {
        await _emitIntents(redis, intents);
      }
    } catch (err) {
      console.error(`[evaluator] Error evaluating account ${accountId}:`, err.message);
    }
  }

  // Clear buffer and dedup state after evaluation tick
  _buffer.clear();
  dedup.clearTick();
}

// ── Lifecycle ─────────────────────────────────────────────────────────────────

let _running = false;
let _stopRequested = false;

function _sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function _evaluatorLoop() {
  console.log(`[evaluator] Started — evaluating every ${EVALUATOR_INTERVAL_MS}ms`);

  // Subscribe to signal bus — decouples evaluator from realtime topology
  signalBus.subscribe('db:insert', bufferEvent);

  while (!_stopRequested) {
    await _sleep(EVALUATOR_INTERVAL_MS);
    if (_stopRequested) break;

    try {
      await _tick();
    } catch (err) {
      console.error('[evaluator] Tick error:', err.message);
    }
  }

  console.log('[evaluator] Stopped');
}

async function startEvaluator() {
  if (_running) {
    console.log('[evaluator] Already running');
    return;
  }

  _running = true;
  _stopRequested = false;

  _evaluatorLoop().catch(err =>
    console.error('[evaluator] Loop crashed:', err.message)
  );
}

function stopEvaluator() {
  console.log('[evaluator] Stopping...');
  _stopRequested = true;
  _running = false;
  // Unsubscribe from signal bus
  signalBus.unsubscribe('db:insert', bufferEvent);
}

module.exports = {
  startEvaluator,
  stopEvaluator,
  // Exported for testing
  _bufferEvent: bufferEvent,
  _evaluateAccount,
};
