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
// Interpreter integration for bounded consumer access:
//   getFSMEntriesSince(domain, cursor)  — FSM-bounded filtered entries
//   getHSMEntriesSince(cursor)          — HSM full observability
//   getReconEntriesSince(cursor)        — Recon full observability
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

// ── Consumer cursor registry (Gap 3: truncation protection) ───────────────────
// Registered consumers (e.g., lineage worker) track their read position.
// Before truncating old log entries, the projection checks that no consumer's
// cursor lags behind the truncation point. If a consumer is behind, truncation
// is skipped and a stall warning is emitted.
const _consumerCursors = new Map(); // consumerName → cursor index (0-based)
const STALL_WARNING_THRESHOLD = MAX_LOG_ENTRIES * 0.8; // warn at 80% cap

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

  // Consumer-aware truncation (Gap 3 fix)
  // Before truncating old entries, check that no registered consumer
  // would lose unconsumed data.
  if (_transitionLog.length > MAX_LOG_ENTRIES) {
    const excess = _transitionLog.length - MAX_LOG_ENTRIES;
    const minConsumerCursor = _getMinConsumerCursor();

    if (minConsumerCursor >= 0 && minConsumerCursor < excess) {
      // At least one consumer is behind the truncation point.
      // Skip truncation to avoid data loss and emit a stall warning.
      if (_transitionLog.length % 100 === 0) {
        console.warn(
          `[projection] Truncation blocked — consumer(s) stalled at cursor ${minConsumerCursor}, ` +
          `log head at ${_transitionLog.length - 1}, would truncate ${excess} entries. ` +
          `Consumer names: ${[..._consumerCursors.entries()].filter(([, c]) => c < excess).map(([n]) => n).join(', ')}`
        );
      }
    } else {
      _transitionLog.splice(0, excess);
    }
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

// ── Cursor-based sequential consumption (Gap 1 fix) ───────────────────────────

/**
 * Return transition log entries from a given index onward.
 * The caller maintains a cursor (0-based index into _transitionLog).
 * Returns the matching entries and the next cursor position.
 *
 * @param {number} includeIndex — 0-based index to start reading from (inclusive)
 * @returns {{ entries: Array<object>, nextCursor: number, totalSize: number }}
 */
function getEntriesSince(includeIndex) {
  const totalSize = _transitionLog.length;
  const start = Math.max(0, Math.min(includeIndex, totalSize));
  const entries = _transitionLog.slice(start);
  return { entries, nextCursor: totalSize, totalSize };
}

/**
 * Return the current total size of the transition log.
 * Callers use this to bootstrap their initial cursor.
 *
 * @returns {number}
 */
function getLogSize() {
  return _transitionLog.length;
}

/**
 * Return a single transition entry by its index in the global log.
 * Used for parentTransitionId derivation (Gap 5).
 *
 * @param {number} index — 0-based index
 * @returns {object|null}
 */
function getEntryByIndex(index) {
  if (index < 0 || index >= _transitionLog.length) return null;
  return _transitionLog[index];
}

/**
 * Find the most recent transition entry matching a predicate, searching
 * backward from the end of the log. Used for parentTransitionId derivation.
 *
 * @param {Function} predicate — (entry) => boolean
 * @returns {object|null} the matching entry or null
 */
function findLastEntry(predicate) {
  for (let i = _transitionLog.length - 1; i >= 0; i--) {
    if (predicate(_transitionLog[i])) return _transitionLog[i];
  }
  return null;
}

// Bounded consumer query methods — namespace-filtered views

/**
 * Get FSM-bounded entries from a given cursor position.
 * Filters entries to only those within FSM's domain jurisdiction.
 *
 * @param {string} domain — FSM domain ('acquisition' | 'publishing' | 'scheduling')
 * @param {number} includeIndex — 0-based index to start reading from
 * @returns {{ entries: Array<object>, nextCursor: number, totalSize: number }}
 */
function getFSMEntriesSince(domain, includeIndex) {
  const result = getEntriesSince(includeIndex);
  const filtered = result.entries.filter(entry => entry.domain === domain);
  return { entries: filtered, nextCursor: result.nextCursor, totalSize: result.totalSize };
}

/**
 * Get HSM-bounded entries from a given cursor position.
 * HSM has full observability — no filtering applied.
 *
 * @param {number} includeIndex — 0-based index to start reading from
 * @returns {{ entries: Array<object>, nextCursor: number, totalSize: number }}
 */
function getHSMEntriesSince(includeIndex) {
  return getEntriesSince(includeIndex);
}

/**
 * Get Recon-bounded entries from a given cursor position.
 * Recon has full observability — no filtering applied.
 *
 * @param {number} includeIndex — 0-based index to start reading from
 * @returns {{ entries: Array<object>, nextCursor: number, totalSize: number }}
 */
function getReconEntriesSince(includeIndex) {
  return getEntriesSince(includeIndex);
}

// ── Consumer cursor registry (Gap 3 fix) ──────────────────────────────────────

function _getMinConsumerCursor() {
  if (_consumerCursors.size === 0) return -1;
  let min = Infinity;
  for (const cursor of _consumerCursors.values()) {
    if (cursor < min) min = cursor;
  }
  return min === Infinity ? -1 : min;
}

/**
 * Register a named consumer for truncation protection.
 * The consumer's initial cursor is set to the current log tail.
 *
 * @param {string} name — unique consumer name, e.g. 'lineage-worker'
 */
function registerConsumer(name) {
  if (!name || typeof name !== 'string') {
    throw new Error('[projection] registerConsumer requires a non-empty name string');
  }
  if (_consumerCursors.has(name)) {
    console.warn(`[projection] Consumer '${name}' already registered — overwriting cursor`);
  }
  _consumerCursors.set(name, 0);
  console.log(`[projection] Consumer '${name}' registered — initial cursor 0, log size ${_transitionLog.length}`);
}

/**
 * Unregister a named consumer. Its cursor is removed and no longer
 * protected from truncation.
 *
 * @param {string} name
 */
function unregisterConsumer(name) {
  _consumerCursors.delete(name);
}

/**
 * Update a consumer's cursor position to indicate entries up to this
 * index have been successfully consumed.
 *
 * @param {string} name — consumer name
 * @param {number} cursor — new cursor position (0-based index of last consumed entry + 1)
 */
function updateConsumerCursor(name, cursor) {
  if (!_consumerCursors.has(name)) {
    console.warn(`[projection] updateConsumerCursor: consumer '${name}' not registered`);
    return;
  }
  const prev = _consumerCursors.get(name);
  _consumerCursors.set(name, Math.max(prev, cursor));
}

/**
 * Get the lag for a registered consumer: how many entries behind the log
 * head the consumer is. Returns -1 if consumer not found.
 *
 * @param {string} name
 * @returns {{ cursor: number, head: number, lag: number, atRisk: boolean }}
 */
function getConsumerLag(name) {
  const cursor = _consumerCursors.get(name);
  if (cursor === undefined) return { cursor: -1, head: _transitionLog.length, lag: -1, atRisk: false };
  const lag = _transitionLog.length - cursor;
  return {
    cursor,
    head: _transitionLog.length,
    lag,
    atRisk: lag > STALL_WARNING_THRESHOLD,
  };
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
  getEntriesSince,
  getLogSize,
  getEntryByIndex,
  findLastEntry,
  registerConsumer,
  unregisterConsumer,
  updateConsumerCursor,
  getConsumerLag,
  // Bounded consumer query methods
  getFSMEntriesSince,
  getHSMEntriesSince,
  getReconEntriesSince,
};
