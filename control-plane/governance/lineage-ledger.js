// control-plane/governance/lineage-ledger.js
// Lineage Ledger: append-only governance event log.
//
// Owns: immutable event recording, lineage retrieval, state materialization,
//        lineage persistence to Redis, lineage rehydration on boot.
//
// Does NOT own: state transitions, governance policy, action emission,
//               domain FSM state — those belong to constitutional-kernel
//               and domain FSMs respectively.
//
// Architectural invariant:
//   - Lineage worker is the sole writer via recordWorkerEntry()
//   - All consumers (CK, reconciliation engine, FSMs) read via getLineage()
//   - Lineage is the canonical source of truth; runtime state is a projection
//   - Single Redis key: lineage:ledger:entries (worker-produced canonical ledger)
//   - record() and createEpoch() are deprecated no-op stubs — all writes route via worker
//
// Contract:
//   ledger.getLineage([n])           → Array<ledgerEntry>    (async, from Redis worker key)
//   ledger.getSize()                  → number                 (async, from Redis worker key)
//   ledger.materializeState(entries)  → { globalState, domains, lastEvent, entryCount } (worker format)
//   ledger.getDomainLineage(n)        → Array<ledgerEntry>     (filtered by domain field)
//   ledger.computeHash()              → string                  (SHA-256)
//   ledger.rehydrate()                → { loaded, latestTs }    (async)
//   ledger.recordWorkerEntry(entry)   → { id, ts }              (worker-only, sole write path)

const crypto = require('crypto');

// ═══════════════════════════════════════════════════════════════════════════════
// Constitutional Error Types
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Thrown when lineage write authority is unavailable.
 * Failure is a valid constitutional state — it must be exposed, not swallowed.
 * This enables explicit failure classification in Phase 6 collapse mapping.
 */
class LineageUnavailableError extends Error {
  constructor(message = 'Lineage write authority unavailable') {
    super(message);
    this.name = 'LineageUnavailableError';
    this.constitutional = true;
  }
}

// Redis client — lazy initialization
let _redis = null;

function _getRedis() {
  if (!_redis) {
    // eslint-disable-next-line global-require
    const { getRedisClient } = require('../../config/redis');
    _redis = getRedisClient();
  }
  return _redis;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Redis key for worker-produced canonical ledger
// ═══════════════════════════════════════════════════════════════════════════════

// Single canonical ledger — all entries written by lineage worker via recordWorkerEntry()
const REDIS_KEY_WORKER = 'lineage:ledger:entries';

// Worker operational keys
const WORKER_KEYS = {
  cursor: 'lineage:worker:cursor',
  health: 'lineage:worker:health',
  divergences: 'lineage:worker:divergences',
  projectionSnapshot: 'lineage:projection:snapshot',
};

// ═══════════════════════════════════════════════════════════════════════════════
// Core API — read path
// All reads from the worker-backed canonical ledger (Redis)
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Parse a JSON string or object from Redis.
 * @param {string|object} item
 * @returns {object|null}
 */
function _parseEntry(item) {
  try {
    return typeof item === 'string' ? JSON.parse(item) : item;
  } catch {
    return null;
  }
}

/**
 * Returns lineage entries from the worker-produced canonical ledger.
 * Reads from lineage:ledger:entries (Redis).
 *
 * @param {number} [n] — number of recent entries to return (default: all)
 * @returns {Promise<Array<object>>}
 */
async function getLineage(n) {
  const redis = _getRedis();
  if (!redis || redis.status !== 'ready' || typeof redis.lrange !== 'function') return [];
  try {
    let raw;
    if (typeof n === 'number' && n > 0) {
      raw = await redis.lrange(REDIS_KEY_WORKER, -n, -1);
      if (!Array.isArray(raw)) return [];
      // lrange -n,-1 returns entries newest-first; reverse to chronological
      return raw.map(item => _parseEntry(item)).filter(Boolean).reverse();
    }
    raw = await redis.lrange(REDIS_KEY_WORKER, 0, -1);
    if (!raw || !Array.isArray(raw)) return [];
    return raw.map(item => _parseEntry(item)).filter(Boolean);
  } catch (err) {
    console.error('[lineage-ledger] getLineage error:', err.message);
    return [];
  }
}

/**
 * Returns the last N lineage entries from the worker-produced canonical ledger.
 * Used by the lineage worker for buffer rehydration on boot.
 * Returns entries in chronological order (oldest first).
 *
 * @param {number} n — number of recent entries to return
 * @returns {Promise<Array<object>>}
 */
async function getWorkerLineage(n) {
  const redis = _getRedis();
  if (!redis || redis.status !== 'ready' || typeof redis.lrange !== 'function') return [];
  if (typeof n !== 'number' || n <= 0) n = 500;
  try {
    const raw = await redis.lrange(REDIS_KEY_WORKER, -n, -1);
    if (!Array.isArray(raw)) return [];
    // lrange -n,-1 returns entries newest-first; reverse to chronological
    return raw.map(item => _parseEntry(item)).filter(Boolean).reverse();
  } catch (err) {
    console.error('[lineage-ledger] getWorkerLineage error:', err.message);
    return [];
  }
}

/**
 * Returns total number of recorded lineage events in the worker ledger.
 *
 * @returns {Promise<number>}
 */
async function getSize() {
  const redis = _getRedis();
  if (!redis || redis.status !== 'ready' || typeof redis.llen !== 'function') return 0;
  try {
    return await redis.llen(REDIS_KEY_WORKER);
  } catch {
    return 0;
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// State Materialization — computes current state from lineage entries
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Materialize the current global and domain states from worker-format lineage entries.
 * This is a PURE function — it does not mutate any state.
 *
 * Worker format:
 *   globalState     ← last entry where domain='governance' AND entity='runtime' → nextState
 *   domains.{name}  ← last entry where domain='{name}' → nextState
 *
 * @param {Array<object>} entries — worker-format lineage entries
 * @returns {{ globalState: string, domains: { acquisition: string, publishing: string, scheduling: string, dedup: string, reconciliation: string }, lastEvent: object|null, entryCount: number }}
 */
function materializeState(entries) {
  if (!entries || entries.length === 0) {
    return {
      globalState: 'BOOTING',
      domains: { acquisition: 'IDLE', publishing: 'IDLE', scheduling: 'IDLE', dedup: 'IDLE', reconciliation: 'IDLE' },
      lastEvent: null,
      entryCount: 0,
    };
  }

  let globalState = 'BOOTING';
  const domains = { acquisition: 'IDLE', publishing: 'IDLE', scheduling: 'IDLE', dedup: 'IDLE', reconciliation: 'IDLE' };
  let lastEvent = null;

  for (const entry of entries) {
    if (!entry || typeof entry.nextState !== 'string') continue;

    // Governance runtime: domain='governance', entity='runtime'
    if (entry.domain === 'governance' && entry.entity === 'runtime') {
      globalState = entry.nextState;
      lastEvent = entry;
    }

    // Domain FSM: domain matches a known constitutional domain
    if (entry.domain && domains.hasOwnProperty(entry.domain)) {
      domains[entry.domain] = entry.nextState;
      lastEvent = entry;
    }
  }

  return { globalState, domains, lastEvent, entryCount: entries.length };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Reconciliation Epoch — snapshot marker for reconciliation cycles
// ═══════════════════════════════════════════════════════════════════════════════

// ═══════════════════════════════════════════════════════════════════════════════
// Deprecated — reconciliation emits through observability → worker → ledger
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * @deprecated Reconciliation now generates epoch IDs locally and emits
 *             EPOCH_CREATED through the observability plane. The lineage
 *             worker ingests this and writes to the ledger. This function
 *             is kept as a compatibility stub.
 *
 * @returns {Promise<{ epochId: string, lineagePosition: number }>}
 */
async function createEpoch() {
  return { epochId: crypto.randomUUID(), lineagePosition: -1 };
}

/**
 * Compute a deterministic SHA-256 constitutional hash from current lineage.
 * Hash includes: entry count, global state, domain projections, last event timestamp.
 *
 * @returns {Promise<string>} hex-encoded SHA-256 hash
 */
async function computeHash() {
  const entries = await getLineage();
  const materialized = materializeState(entries);
  const payload = JSON.stringify({
    count: entries.length,
    globalState: materialized.globalState,
    domains: materialized.domains,
    lastTs: materialized.lastEvent ? materialized.lastEvent.ts : null,
  });
  return crypto.createHash('sha256').update(payload).digest('hex');
}

/**
 * Returns the last N lineage entries for a specific domain.
 * Filters by domain field (worker format), not authority pattern.
 *
 * @param {string} domainName — 'acquisition' | 'publishing' | 'scheduling' | 'dedup' | 'reconciliation'
 * @param {number} [n] — number of recent entries to return (default: all)
 * @returns {Promise<Array<object>>}
 */
async function getDomainLineage(domainName, n) {
  const all = await getLineage(n);
  return all.filter(e => e.domain === domainName);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Rehydration — load persisted lineage from Redis on boot
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Load all persisted lineage entries from Redis into memory for boot rehydration.
 * Called by constitutional-kernel on boot to reconstruct state.
 *
 * @returns {Promise<{ loaded: number, latestTs: number | null }>}
 */
async function rehydrate() {
  const entries = await getLineage();
  if (!entries || entries.length === 0) {
    return { loaded: 0, latestTs: null };
  }
  const loaded = entries.length;
  let latestTs = null;
  for (const entry of entries) {
    if (latestTs === null || entry.ts > latestTs) latestTs = entry.ts;
  }
  console.log(`[lineage-ledger] Rehydrated ${loaded} entries from Redis`);
  return { loaded, latestTs };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Worker-produced canonical ledger
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Record a lineage entry produced by the lineage worker.
 * Writes to the worker's dedicated Redis key: lineage:ledger:entries.
 * Async — must be awaited by the caller to guarantee persistence.
 *
 * @param {object} entry — canonical ledger entry from the lineage worker
 * @returns {Promise<{ id: string, ts: number }>}
 */
async function recordWorkerEntry(entry) {
  const redis = _getRedis();
  if (!redis || redis.status !== 'ready' || typeof redis.rpush !== 'function') {
    throw new LineageUnavailableError(
      `Redis status=${redis?.status || 'null'} — constitutional write authority absent`
    );
  }
  await redis.rpush(REDIS_KEY_WORKER, JSON.stringify(entry));
  return { id: entry.ledgerId || entry.id, ts: entry.timestamp || entry.ts };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Worker delegation — ledger-owned persistence for lineage worker state
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Persist the worker's consumption cursor.
 *
 * @param {number} cursor — the worker's current read position
 * @param {number} [ttlSeconds=60] — Redis key TTL
 */
function persistWorkerCursor(cursor, ttlSeconds = 60) {
  try {
    const redis = _getRedis();
    if (redis && redis.status === 'ready') {
      redis.set(WORKER_KEYS.cursor, String(cursor), 'EX', ttlSeconds);
    }
  } catch (_) {}
}

/**
 * Retrieve the worker's persisted cursor from Redis.
 *
 * @returns {Promise<number>} the cursor value, or 0 if unavailable
 */
async function getWorkerCursor() {
  try {
    const redis = _getRedis();
    if (redis && redis.status === 'ready') {
      const raw = await redis.get(WORKER_KEYS.cursor);
      if (raw != null) {
        const parsed = parseInt(raw, 10);
        if (!isNaN(parsed) && parsed >= 0) return parsed;
      }
    }
  } catch (_) {}
  return 0;
}

/**
 * Persist the worker's health signals.
 *
 * @param {object} health — health snapshot from the worker
 * @param {number} [ttlSeconds=30] — Redis key TTL
 */
function persistWorkerHealth(health, ttlSeconds = 30) {
  try {
    const redis = _getRedis();
    if (redis && redis.status === 'ready') {
      redis.set(WORKER_KEYS.health, JSON.stringify(health), 'EX', ttlSeconds);
    }
  } catch (_) {}
}

/**
 * Persist the worker's projection snapshot.
 *
 * @param {object} projections — full projection snapshot from the worker
 * @param {number} [ttlSeconds=60] — Redis key TTL
 */
function persistWorkerProjection(projections, ttlSeconds = 60) {
  try {
    const redis = _getRedis();
    if (redis && redis.status === 'ready') {
      redis.set(WORKER_KEYS.projectionSnapshot, JSON.stringify(projections), 'EX', ttlSeconds);
    }
  } catch (_) {}
}

/**
 * Persist the worker's divergence log.
 *
 * @param {Array<object>} divergences — recent divergence entries
 * @param {number} [ttlSeconds=90] — Redis key TTL
 */
function persistWorkerDivergences(divergences, ttlSeconds = 90) {
  try {
    const redis = _getRedis();
    if (redis && redis.status === 'ready') {
      redis.set(WORKER_KEYS.divergences, JSON.stringify(divergences), 'EX', ttlSeconds);
    }
  } catch (_) {}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Deprecated stubs — all write authority now routes via lineage worker
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * @deprecated All writes route via lineage worker. Callers emit through
 *             observability.transition() which the worker consumes and
 *             persists to lineage:ledger:entries. Kept as no-op stub.
 *
 * @param {object} entry — unused
 * @returns {Promise<{ id: string, ts: number }>}
 */
async function record(entry) {
  return { id: 'deprecated-stub', ts: Date.now() };
}

module.exports = {
  LineageUnavailableError,
  record,
  getLineage,
  getWorkerLineage,
  getSize,
  materializeState,
  rehydrate,
  createEpoch,
  computeHash,
  getDomainLineage,
  REDIS_KEY_WORKER,
  WORKER_KEYS,
  recordWorkerEntry,
  persistWorkerCursor,
  getWorkerCursor,
  persistWorkerHealth,
  persistWorkerProjection,
  persistWorkerDivergences,
};
