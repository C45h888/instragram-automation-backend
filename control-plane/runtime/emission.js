// control-plane/runtime/emission.js
// Emission: bounded intent emission to Redis and mutation application.
//
// Owns: constructing Redis queue intents, LPUSH to domain queues,
//        writing result keys, applying mutations via mutation-substrate.
// Does NOT own: evaluation, buffering, worker lifecycle, signal intake.
//
// Contract:
//   emitter.emit(intents)        → LPUSH each intent to its domain queue
//   emitter.emitMutation(mut)    → apply state mutation via mutation-substrate

const { getRedisClient } = require('../../config/redis');
const mutationSubstrate = require('../mutation-substrate');

const RESULT_TTL_SEC = 3600;

// ── Routing helpers ──────────────────────────────────────────────────────────

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

// ── Intent emission ──────────────────────────────────────────────────────────

/**
 * Emit intents to Redis publish queues. Each intent is LPUSHed to its
 * domain-specific queue key. A result key is set for observability.
 * @param {Array<object>} intents — from evaluator.evaluate()
 */
async function emit(intents) {
  const redis = getRedisClient();
  if (!redis || redis.status !== 'ready') {
    console.warn('[emission] Redis not ready — cannot emit intents');
    return;
  }

  for (const intent of intents) {
    const { account_id, action_type, resource_id, payload, queue_row_id, scheduled_post_id, intent_type } = intent;

    const intent_id = require('crypto').randomUUID();
    const domain = domainForAction(action_type);
    const fetch_type = fetchTypeForAction(action_type);

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

    console.log(`[emission] Emitted publish:${domain} intent ${intent_id} for ${intent_type} ${resource_id}`);
  }
}

/**
 * Apply a state mutation via the mutation substrate.
 * @param {{ table: string, id: string, updates: object, reason: string }} mut
 */
async function emitMutation(mut) {
  await mutationSubstrate.applyMutation(mut.table, mut.id, mut.updates, mut.reason);
}

module.exports = { emit, emitMutation };
