// control-plane/observability/projection.js
// Projection: in-memory live state projection from normalized transitions.
//
// Owns: maintaining current state per entity, capped transition log,
//        Redis snapshot for crash survival, and full query interface.
//
// Does NOT own: normalization, context propagation, emission logic.
//
// The projection is the STATE STORE of the observability plane.
// It receives normalized STATE_TRANSITION entries from the normalizer
// and updates its in-memory indexes accordingly.
//
// Query interface:
//   getState(domain, entity, entityId)      → current state string
//   getDomainState(domain)                  → Map<entity, Map<entityId, state>>
//   getTransitionLog(domain, entity, n)     → last n transitions
//   getCrossDomain(domains)                 → states across multiple domains
//   getFullSnapshot()                       → entire projection for CK reconciliation
//   snapshot()                              → trigger Redis persistence

const { getRedisClient } = require('../../config/redis');

const REDIS_KEY = 'governance:observability:projection';
const REDIS_TTL_S = 300; // 5 minutes
const MAX_LOG_ENTRIES = 10_000;
const SNAPSHOT_INTERVAL_MS = 30_000;

// ── In-memory indexes ───────────────────────────────────────────────────────────

// Primary state index: "domain:entity:entityId" → state string
const _stateIndex = new Map();

// Domain-scoped index: domain → entity → entityId → state
const _domainIndex = new Map();

// Append-only transition log
const _transitionLog = []; // Array<normalized transition>

// Domain → entity → entityId → last N transitions (sliding window per entity)
const _entityLog = new Map(); // "domain:entity:entityId" → Array<transition>

// ── Persistence ───────────────────────────────────────────────────────────────

let _snapshotTimer = null;
let _redisHealthy = false;

function _redisCheck() {
  const redis = getRedisClient();
  _redisHealthy = redis && redis.status === 'ready';
  return _redisHealthy;
}

/**
 * Persist the current projection state to Redis.
 * Called periodically (every SNAPSHOT_INTERVAL_MS) and on graceful shutdown.
 */
async function _persistSnapshot() {
  if (!_redisCheck()) {
    console.warn('[projection] Redis unavailable — skipping snapshot');
    return;
  }

  try {
    const redis = getRedisClient();
    const snapshot = {
      stateIndex: Object.fromEntries(_stateIndex),
      domainIndex: Object.fromEntries(
        [..._domainIndex.entries()].map(([d, eMap]) => [
          d,
          Object.fromEntries([...eMap.entries()].map(([e, idMap]) => [e, Object.fromEntries(idMap)])),
        ])
      ),
      logSize: _transitionLog.length,
      lastTs: _transitionLog.length > 0 ? _transitionLog[_transitionLog.length - 1].timestamp : null,
      snapshotAt: Date.now(),
    };
    await redis.set(REDIS_KEY, JSON.stringify(snapshot), 'EX', REDIS_TTL_S);
  } catch (err) {
    console.error('[projection] Snapshot persist error:', err.message);
  }
}

/**
 * Load a persisted projection snapshot from Redis on boot.
 * Merges recovered state into in-memory indexes.
 */
async function _rehydrate() {
  if (!_redisCheck()) {
    console.warn('[projection] Redis unavailable — starting fresh');
    return;
  }

  try {
    const redis = getRedisClient();
    const raw = await redis.get(REDIS_KEY);
    if (!raw) return;

    const snap = JSON.parse(raw);
    if (!snap || !snap.stateIndex) return;

    // Restore state index
    for (const [key, state] of Object.entries(snap.stateIndex)) {
      _stateIndex.set(key, state);
    }

    // Restore domain index
    for (const [domain, eMap] of Object.entries(snap.domainIndex || {})) {
      if (!_domainIndex.has(domain)) _domainIndex.set(domain, new Map());
      for (const [entity, idMap] of Object.entries(eMap)) {
        if (!_domainIndex.get(domain).has(entity)) _domainIndex.get(domain).set(entity, new Map());
        for (const [entityId, state] of Object.entries(idMap)) {
          _domainIndex.get(domain).get(entity).set(entityId, state);
        }
      }
    }

    console.log(`[projection] Rehydrated ${snap.logSize} entries from Redis (last ts: ${snap.lastTs})`);
  } catch (err) {
    console.warn('[projection] Rehydration error:', err.message);
  }
}

// ── Core projection write ──────────────────────────────────────────────────────

/**
 * Project a normalized transition into the live in-memory indexes.
 * Called by the transition emitter after normalization.
 *
 * @param {object} transition — canonical STATE_TRANSITION from normalizer
 */
function project(transition) {
  const { domain, entity, entityId, nextState, previousState } = transition;

  // Build composite key
  const key = _makeKey(domain, entity, entityId);

  // Update primary state index — nextState becomes the current state
  if (nextState) {
    _stateIndex.set(key, nextState);
  }

  // Update domain-scoped index
  if (domain) {
    if (!_domainIndex.has(domain)) _domainIndex.set(domain, new Map());
    const eMap = _domainIndex.get(domain);
    if (!eMap.has(entity)) eMap.set(entity, new Map());
    if (entityId && nextState) {
      eMap.get(entity).set(entityId, nextState);
    }
  }

  // Append to global transition log
  _transitionLog.push(transition);
  if (_transitionLog.length > MAX_LOG_ENTRIES) {
    _transitionLog.splice(0, _transitionLog.length - MAX_LOG_ENTRIES);
  }

  // Update entity-scoped sliding log
  if (entity && entityId) {
    if (!_entityLog.has(key)) _entityLog.set(key, []);
    const log = _entityLog.get(key);
    log.push(transition);
    if (log.length > 100) log.splice(0, log.length - 100);
  }
}

function _makeKey(domain, entity, entityId) {
  return `${domain || 'unknown'}:${entity || 'unknown'}:${entityId || '_root'}`;
}

// ── Query interface ───────────────────────────────────────────────────────────

/**
 * Get the current state of a specific entity.
 *
 * @param {string} domain
 * @param {string} entity
 * @param {string} entityId
 * @returns {string|null} current state or null if never observed
 */
function getState(domain, entity, entityId) {
  return _stateIndex.get(_makeKey(domain, entity, entityId)) || null;
}

/**
 * Get all entity states within a domain.
 *
 * @param {string} domain
 * @returns {{ [entity]: { [entityId]: string } }} nested map of entity states
 */
function getDomainState(domain) {
  const eMap = _domainIndex.get(domain);
  if (!eMap) return {};
  const result = {};
  for (const [entity, idMap] of eMap) {
    result[entity] = Object.fromEntries(idMap);
  }
  return result;
}

/**
 * Get the last N transitions for a specific entity.
 *
 * @param {string} domain
 * @param {string} entity
 * @param {string} entityId
 * @param {number} [n=10]
 * @returns {Array<object>} last n transitions, oldest first
 */
function getTransitionLog(domain, entity, entityId, n = 10) {
  const log = _entityLog.get(_makeKey(domain, entity, entityId));
  if (!log) return [];
  return log.slice(-n);
}

/**
 * Get a full snapshot of the entire projection.
 * Used by the constitutional kernel for reconciliation (wired in Pass 3).
 *
 * @returns {object} full projection snapshot
 */
function getFullSnapshot() {
  const domains = {};
  for (const [domain, eMap] of _domainIndex) {
    domains[domain] = {};
    for (const [entity, idMap] of eMap) {
      domains[domain][entity] = Object.fromEntries(idMap);
    }
  }
  return {
    domains,
    globalStateIndex: Object.fromEntries(_stateIndex),
    transitionCount: _transitionLog.length,
    lastTransitionTs: _transitionLog.length > 0 ? _transitionLog[_transitionLog.length - 1].timestamp : null,
  };
}

/**
 * Get state across multiple domains (for cross-domain queries by FSMs).
 *
 * @param {Array<string>} domains
 * @returns {object} merged state from all requested domains
 */
function getCrossDomain(domains) {
  const result = {};
  for (const d of domains) {
    result[d] = getDomainState(d);
  }
  return result;
}

/**
 * Trigger an immediate snapshot persist to Redis.
 * Called by the observability plane index on shutdown.
 */
async function snapshot() {
  await _persistSnapshot();
}

/**
 * Start the periodic snapshot timer.
 * Call once at boot after rehydration.
 */
function startSnapshotTimer() {
  if (_snapshotTimer) return;
  _snapshotTimer = setInterval(() => {
    _persistSnapshot().catch(() => {});
  }, SNAPSHOT_INTERVAL_MS);
  _snapshotTimer.unref();
  console.log(`[projection] Snapshot timer started — every ${SNAPSHOT_INTERVAL_MS / 1000}s`);
}

/**
 * Stop the periodic snapshot timer and persist final snapshot.
 */
async function stopSnapshotTimer() {
  if (_snapshotTimer) {
    clearInterval(_snapshotTimer);
    _snapshotTimer = null;
  }
  await _persistSnapshot();
}

/**
 * Initialize the projection: rehydrate from Redis and start snapshot timer.
 * Call once at system boot.
 */
async function init() {
  await _rehydrate();
  startSnapshotTimer();
}

module.exports = {
  project,
  getState,
  getDomainState,
  getTransitionLog,
  getFullSnapshot,
  getCrossDomain,
  snapshot,
  init,
  startSnapshotTimer,
  stopSnapshotTimer,
};
