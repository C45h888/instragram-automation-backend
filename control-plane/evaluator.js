// control-plane/evaluator.js
// Control Plane: deterministic publishing evaluation loop.
//
// Owns: buffering realtime events, evaluating deterministic conditions per tick,
//        emitting publish intents to Redis queues.
// Does NOT own: IG API calls, persistence writes, retry logic.
//
// Architecture:
//   Supabase Realtime ── INSERT events ──► evaluator buffer (in-memory Map)
//                                           │
//                        10s tick ──────────┘ evaluate
//                                           │
//                        emit intent ──► Redis queue (supervisor:acquisitions:publish:{domain}:{account})
//
// Evaluation conditions are deterministic and run every EVALUATOR_INTERVAL_MS.
// Only one intent per (account, action_type, resource_id) per tick — dedup via in-flight Set.

const { getRedisClient } = require('../config/redis');
const { getSupabaseAdmin } = require('../config/supabase');
const { getActiveAccounts } = require('../substrates/persistence');
const realtime = require('../substrates/realtime');
const { buildIdempotencyKey } = require('../helpers/agent-helpers');

const EVALUATOR_INTERVAL_MS = parseInt(process.env.EVALUATOR_INTERVAL_MS || '10000', 10); // 10s
const RESULT_TTL_SEC = 3600;

// ── Action type → publish domain mapping ─────────────────────────────────────

function domainForAction(actionType) {
  if (actionType === 'publish_post') return 'media';
  if (actionType === 'repost_ugc') return 'ugc';
  return 'messaging';
}

function fetchTypeForAction(actionType) {
  if (actionType === 'publish_post') return 'publish_media';
  if (actionType === 'repost_ugc') return 'publish_ugc';
  return 'publish_messaging';
}

// ── In-flight deduplication ───────────────────────────────────────────────────

/**
 * In-flight intents: prevents double-emitting the same resource in one tick.
 * Key: `${accountId}:${actionType}:${resourceId}`
 * Cleared after every EVALUATOR_INTERVAL_MS tick.
 */
const _inFlight = new Set();

function _markInFlight(accountId, actionType, resourceId) {
  _inFlight.add(`${accountId}:${actionType}:${resourceId}`);
}

function _isInFlight(accountId, actionType, resourceId) {
  return _inFlight.has(`${accountId}:${actionType}:${resourceId}`);
}

function _clearInFlight() {
  _inFlight.clear();
}

// ── Event buffer ──────────────────────────────────────────────────────────────

/**
 * accountId → [
 *   { table: 'scheduled_posts'|'post_queue', record: {...}, receivedAt: ms }
 * ]
 * Buffered events accumulate between ticks. Cleared after each evaluation.
 */
const _buffer = new Map(); // accountId → event[]

/**
 * Called by realtime substrate on every INSERT event.
 * Accumulates events into the buffer for the next evaluation tick.
 */
function bufferEvent(accountId, table, record) {
  if (!_buffer.has(accountId)) {
    _buffer.set(accountId, []);
  }
  _buffer.get(accountId).push({ table, record, receivedAt: Date.now() });
}

// ── Evaluation ────────────────────────────────────────────────────────────────

/**
 * Evaluates buffered events for one account.
 * Returns an array of intent objects to emit.
 */
async function _evaluateAccount(accountId, events) {
  const intents = [];

  for (const { table, record } of events) {
    if (table === 'scheduled_posts') {
      // Only process approved posts
      if (record.status !== 'approved') continue;

      const resourceId = record.id;
      if (_isInFlight(accountId, 'publish_post', resourceId)) continue;

      // Inline asset resolution — fetch from instagram_assets before emitting intent
      const supabase = getSupabaseAdmin();
      if (!supabase) continue;

      const { data: asset } = await supabase
        .from('instagram_assets')
        .select('storage_path, media_type, caption')
        .eq('id', record.asset_id)
        .single();

      if (!asset?.storage_path) {
        console.warn(`[evaluator] Scheduled post ${record.id} missing asset — marking failed`);
        await supabase
          .from('scheduled_posts')
          .update({ status: 'failed' })
          .eq('id', record.id)
          .eq('status', 'approved');
        continue;
      }

      _markInFlight(accountId, 'publish_post', resourceId);

      intents.push({
        account_id: accountId,
        fetch_type: 'publish_media',
        action_type: 'publish_post',
        resource_id: resourceId,
        payload: {
          image_url: asset.storage_path,
          caption: asset.caption || '',
          media_type: asset.media_type || 'IMAGE',
          asset_id: record.asset_id,
          scheduled_post_id: record.id,
        },
        intent_type: 'scheduled_post',
      });

    } else if (table === 'post_queue') {
      if (!['pending', 'failed'].includes(record.status)) continue;

      const resourceId = record.id;
      if (_isInFlight(accountId, record.action_type, resourceId)) continue;

      // Skip if next_retry_at is in the future
      if (record.next_retry_at && new Date(record.next_retry_at) > new Date()) continue;

      _markInFlight(accountId, record.action_type, resourceId);

      intents.push({
        account_id: accountId,
        fetch_type: fetchTypeForAction(record.action_type),
        action_type: record.action_type,
        resource_id: resourceId,
        payload: record.payload || {},
        queue_row_id: record.id,
        intent_type: 'post_queue',
      });
    }
  }

  return intents;
}

// ── Intent emission ──────────────────────────────────────────────────────────

/**
 * Serialises and emits intents to Redis queues.
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

    // Write result key for traceability
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

  // Remove buffer entries for accounts that no longer exist
  for (const bufferedAccountId of _buffer.keys()) {
    if (!accountIds.has(bufferedAccountId)) {
      _buffer.delete(bufferedAccountId);
    }
  }

  // Evaluate each account's buffered events
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

  // Clear buffer and in-flight set after evaluation
  _buffer.clear();
  _clearInFlight();
}

// ── Lifecycle ─────────────────────────────────────────────────────────────────

let _running = false;
let _stopRequested = false;
let _intervalHandle = null;

function _sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function _evaluatorLoop() {
  console.log(`[evaluator] Started — evaluating every ${EVALUATOR_INTERVAL_MS}ms`);

  // Register buffer callback so realtime knows where to send events
  realtime.registerBufferCallback(bufferEvent);

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

/**
 * Starts the evaluator loop.
 * Idempotent — no-op if already running.
 */
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

/**
 * Stops the evaluator loop.
 */
function stopEvaluator() {
  console.log('[evaluator] Stopping...');
  _stopRequested = true;
  _running = false;
}

module.exports = {
  startEvaluator,
  stopEvaluator,
  // Exported for testing
  _bufferEvent: bufferEvent,
  _evaluateAccount,
};
