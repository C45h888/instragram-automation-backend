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
const { domainForAction, fetchTypeForAction } = require('../execution/domain-registry');

const RESULT_TTL_SEC = 3600;

// ── Status ───────────────────────────────────────────────────────────────────

/**
 * Returns live runtime state. Checks Redis connection health.
 * @returns {{ redis: 'connected'|'disconnected' }}
 */
function status() {
  const redis = getRedisClient();
  return {
    redis: (redis && redis.status === 'ready') ? 'connected' : 'disconnected',
  };
}

// ── Intent emission ──────────────────────────────────────────────────────────

/**
 * Emit intents to Redis publish queues. Each intent is LPUSHed to its
 * domain-specific queue key. A result key is set for observability.
 *
 * @param {Array<object>} intents — from evaluator.evaluate(), must be an array
 * @returns {Promise<{ok: boolean, error?: string, count: number}>}
 *   ok=false and error is set when Redis is unavailable.
 */
async function emit(intents) {
  if (!Array.isArray(intents)) {
    return { ok: false, error: `intents must be an array, got ${typeof intents}`, count: 0 };
  }
  if (intents.length === 0) {
    return { ok: true, count: 0 };
  }

  const redis = getRedisClient();
  if (!redis || redis.status !== 'ready') {
    console.warn('[emission] Redis not ready — cannot emit intents');
    return { ok: false, error: 'redis unavailable', count: 0 };
  }

  let emitted = 0;
  for (const intent of intents) {
    const { account_id, action_type, resource_id, payload, queue_row_id, scheduled_post_id, intent_type } = intent;

    const intent_id = require('crypto').randomUUID();
    const domain = domainForAction(action_type);
    const fetch_type = fetchTypeForAction(action_type);

    // Observability: queue intent state transition
    _emitTransition({
      domain: 'emission',
      entity: 'queue_intent',
      entityId: intent_id,
      previousState: 'PENDING',
      nextState: 'QUEUED',
      authority: 'emission-runtime',
      raw: { account_id, action_type, domain },
    });

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
    emitted++;
  }

  return { ok: true, count: emitted };
}

/**
 * Apply a state mutation via the mutation substrate.
 *
 * @param {{ table: string, id: string, updates: object, reason: string }} mut
 * @returns {Promise<{ok: boolean, error?: string}>}
 *   ok=false when mutation substrate fails or input is invalid.
 */
async function emitMutation(mut) {
  if (!mut || typeof mut !== 'object') {
    return { ok: false, error: `mutation must be an object, got ${typeof mut}` };
  }
  if (!mut.table || !mut.id || !mut.updates) {
    return { ok: false, error: 'mutation requires { table, id, updates }' };
  }

  // Observability: mutation state transition
  _emitTransition({
    domain: 'emission',
    entity: 'mutation',
    entityId: mut.id,
    previousState: 'PENDING',
    nextState: 'APPLIED',
    authority: 'emission-runtime',
    raw: { table: mut.table, reason: mut.reason },
  });

  await mutationSubstrate.applyMutation(mut.table, mut.id, mut.updates, mut.reason);
  return { ok: true };
}

/**
 * Emit an observability transition. Wrapped in try/catch — never disrupts emission.
 */
function _emitTransition(params) {
  try {
    const observability = require('../observability/emitters/transition-emitter');
    observability.transition(params);
  } catch (err) {
    console.warn('[emission] Observability transition error:', err.message);
  }
}

module.exports = { status, emit, emitMutation };
