// dedup-kernel/substrates/dedup-mutation/index.js
// Mutation + Emission dedup substrate: Redis-backed idempotency for mutation
// and emission layers (Layer 2 and Layer 3 of the dedup chain).
//
// Architecture (Phase 8 — Mutation/Emission Dedup Extension):
//   Layer 1 (existing): Event intake pre-filter
//     Key: governance:dedup:{accountId}:{actionType}:{resourceId}:{intentId}
//   Layer 2 (new): Mutation dedup gate
//     Key: governance:dedup:mutation:{accountId}:{actionType}:{resourceId}:{intentId}
//   Layer 3 (new): Emission dedup gate
//     Key: governance:dedup:emission:{accountId}:{actionType}:{resourceId}:{intentId}
//
// Belt-and-suspenders: mutation dedup ALSO checks the Layer 1 intake key
// (governance:dedup:). If an intent was allowed at intake but something went
// wrong in evaluation, the mutation dedup will catch a replay and block.
//
// Observability emissions:
//   dedup_entry: PENDING → IN_FLIGHT (mutation/emission layer)
//
// Constants
const { getRedisClient } = require('../../../config/redis');

const MUTATION_KEY_PREFIX = 'governance:dedup:mutation:';
const EMISSION_KEY_PREFIX = 'governance:dedup:emission:';
const INTAKE_KEY_PREFIX = 'governance:dedup:';   // shared with dedup/index.js
const TTL_SECONDS = 120;
const MAX_LOCAL_ENTRIES = 2000;

// Local read-through cache: identityKey → { intentId, ts }
const _mutationCache = new Map();
const _emissionCache = new Map();

function _makeMutationKey(accountId, actionType, resourceId, intentId) {
  return `${MUTATION_KEY_PREFIX}${accountId}:${actionType}:${resourceId}:${intentId}`;
}

function _makeEmissionKey(accountId, actionType, resourceId, intentId) {
  return `${EMISSION_KEY_PREFIX}${accountId}:${actionType}:${resourceId}:${intentId}`;
}

function _makeIntakeKey(accountId, actionType, resourceId, intentId) {
  return `${INTAKE_KEY_PREFIX}${accountId}:${actionType}:${resourceId}:${intentId}`;
}

function _evictOldest(cache) {
  if (cache.size < MAX_LOCAL_ENTRIES) return;
  const entries = [...cache.keys()];
  const evictCount = Math.floor(MAX_LOCAL_ENTRIES * 0.2);
  for (let i = 0; i < evictCount; i++) cache.delete(entries[i]);
}

// ── Observability helper ──────────────────────────────────────────────────────
function _emitTransition(params) {
  try {
    const observability = require('../../../../control-plane/observability/emitters/transition-emitter');
    observability.transition(params);
  } catch (_) {}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Public API — Mutation layer
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * isInFlightMutation — checks mutation-layer dedup for a given intent.
 *
 * Belt-and-suspenders: also checks the intake key (Layer 1) so that
 * an intent allowed at intake but not yet fully processed cannot be
 * mutated again if evaluation ran twice.
 *
 * @returns {{ blocked: boolean, reason: 'duplicate'|'intake_duplicate'|null,
 *             existingIntentId: string|null }}
 */
async function isInFlightMutation(accountId, actionType, resourceId, intentId) {
  // ── 1. Belt-and-suspenders: check intake key (Layer 1) ──────────────
  const intakeKey = _makeIntakeKey(accountId, actionType, resourceId, intentId);
  const redis = getRedisClient();

  if (redis && redis.status === 'ready') {
    const intakeExists = await redis.exists(intakeKey).catch(() => 0);
    if (intakeExists) {
      return { blocked: true, reason: 'intake_duplicate', existingIntentId: intentId };
    }
  }

  // ── 2. Check mutation-layer identity key ───────────────────────────
  const mutationKey = _makeMutationKey(accountId, actionType, resourceId, intentId);
  if (_mutationCache.has(mutationKey)) {
    return { blocked: true, reason: 'duplicate', existingIntentId: intentId };
  }

  if (redis && redis.status === 'ready') {
    const exists = await redis.exists(mutationKey).catch(() => 0);
    if (exists) {
      _mutationCache.set(mutationKey, { intentId, ts: Date.now() });
      _evictOldest(_mutationCache);
      return { blocked: true, reason: 'duplicate', existingIntentId: intentId };
    }
  }

  return { blocked: false, reason: null, existingIntentId: null };
}

/**
 * markInFlightMutation — marks an intent as in-flight at the mutation layer.
 * Writes ONLY to the mutation namespace (Layer 2).
 * Layer 1 (intake) key is left untouched — it expires on its own TTL.
 */
async function markInFlightMutation(accountId, actionType, resourceId, opts = {}) {
  const intentId = opts.intentId || 'legacy';
  const now = Date.now();
  const mutationKey = _makeMutationKey(accountId, actionType, resourceId, intentId);

  _mutationCache.set(mutationKey, { intentId, ts: now });
  _evictOldest(_mutationCache);

  _emitTransition({
    domain: 'dedup',
    entity: 'mutation_dedup_entry',
    entityId: mutationKey,
    previousState: 'PENDING',
    nextState: 'IN_FLIGHT',
    authority: 'dedup-mutation-substrate',
    raw: { accountId, actionType, resourceId, intentId },
  });

  const redis = getRedisClient();
  if (redis && redis.status === 'ready') {
    redis.set(mutationKey, String(now), 'EX', TTL_SECONDS, 'NX').catch(() => {});
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Public API — Emission layer
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * isInFlightEmission — checks emission-layer dedup for a given intent.
 * Also checks Layer 1 intake key (belt-and-suspenders).
 *
 * @returns {{ blocked: boolean, reason: 'duplicate'|'intake_duplicate'|null,
 *             existingIntentId: string|null }}
 */
async function isInFlightEmission(accountId, actionType, resourceId, intentId) {
  const intakeKey = _makeIntakeKey(accountId, actionType, resourceId, intentId);
  const redis = getRedisClient();

  if (redis && redis.status === 'ready') {
    const intakeExists = await redis.exists(intakeKey).catch(() => 0);
    if (intakeExists) {
      return { blocked: true, reason: 'intake_duplicate', existingIntentId: intentId };
    }
  }

  const emissionKey = _makeEmissionKey(accountId, actionType, resourceId, intentId);
  if (_emissionCache.has(emissionKey)) {
    return { blocked: true, reason: 'duplicate', existingIntentId: intentId };
  }

  if (redis && redis.status === 'ready') {
    const exists = await redis.exists(emissionKey).catch(() => 0);
    if (exists) {
      _emissionCache.set(emissionKey, { intentId, ts: Date.now() });
      _evictOldest(_emissionCache);
      return { blocked: true, reason: 'duplicate', existingIntentId: intentId };
    }
  }

  return { blocked: false, reason: null, existingIntentId: null };
}

/**
 * markInFlightEmission — marks an intent as in-flight at the emission layer.
 */
async function markInFlightEmission(accountId, actionType, resourceId, opts = {}) {
  const intentId = opts.intentId || 'legacy';
  const now = Date.now();
  const emissionKey = _makeEmissionKey(accountId, actionType, resourceId, intentId);

  _emissionCache.set(emissionKey, { intentId, ts: now });
  _evictOldest(_emissionCache);

  _emitTransition({
    domain: 'dedup',
    entity: 'emission_dedup_entry',
    entityId: emissionKey,
    previousState: 'PENDING',
    nextState: 'IN_FLIGHT',
    authority: 'dedup-mutation-substrate',
    raw: { accountId, actionType, resourceId, intentId },
  });

  const redis = getRedisClient();
  if (redis && redis.status === 'ready') {
    redis.set(emissionKey, String(now), 'EX', TTL_SECONDS, 'NX').catch(() => {});
  }
}

// ── Snapshot ─────────────────────────────────────────────────────────────────
function getMutationSnapshot() {
  return { count: _mutationCache.size };
}

function getEmissionSnapshot() {
  return { count: _emissionCache.size };
}

module.exports = {
  isInFlightMutation,
  markInFlightMutation,
  isInFlightEmission,
  markInFlightEmission,
  getMutationSnapshot,
  getEmissionSnapshot,
};