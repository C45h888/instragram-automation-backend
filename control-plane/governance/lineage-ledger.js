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
// Redis keys for worker-produced canonical ledger
// ═══════════════════════════════════════════════════════════════════════════════

// Single canonical ledger — all entries written by lineage worker via recordWorkerEntry()
const REDIS_KEY_WORKER = 'lineage:ledger:entries';

// Domain-partitioned keys — materialized projections of the canonical ledger.
// Each key is a chronological Redis list for a single domain. The lineage worker
// writes to BOTH the global key AND the domain-specific key in the same tick.
// The global list remains the canonical source of truth; domain keys are read-
// optimized projections that preserve bounded authority isolation.
const DOMAIN_KEYS = {
  acquisition: 'lineage:ledger:domain:acquisition',
  publishing: 'lineage:ledger:domain:publishing',
  scheduling: 'lineage:ledger:domain:scheduling',
  dedup: 'lineage:ledger:domain:dedup',
  engagement: 'lineage:ledger:domain:engagement',
};

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
 * Compute a deterministic SHA-256 constitutional hash from provided entries.
 * No Redis access — caller provides the entries snapshot.
 * Used when the caller already has the entries and wants a hash without re-reading.
 *
 * @param {Array<object>} entries — lineage entries (already fetched)
 * @returns {string} hex-encoded SHA-256 hash
 */
function computeHashFromEntries(entries) {
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
 * Compute a deterministic SHA-256 constitutional hash from current lineage.
 * Hash includes: entry count, global state, domain projections, last event timestamp.
 *
 * @param {Array<object>} [entriesArg] — optional pre-fetched entries (avoids Redis re-read)
 * @returns {Promise<string>} hex-encoded SHA-256 hash
 */
async function computeHash(entriesArg) {
  const entries = entriesArg || await getLineage();
  return computeHashFromEntries(entries);
}

/**
 * Returns atomic snapshot: { entries, hash } in a single call.
 * Used by reconciliation trigger to capture the immutable constitutional plane.
 * Captures entries once and computes hash locally — no second Redis round-trip.
 *
 * @returns {Promise<{ entries: Array<object>, hash: string }>}
 */
async function getLineageWithHash() {
  const entries = await getLineage();
  const hash = computeHashFromEntries(entries);
  return { entries, hash };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Domain-Partitioned Lineage — bounded authority reads
//
// Domain keys are materialized projections of the canonical global ledger.
// The lineage worker writes to both simultaneously. Domain-specific reads
// are isolated from other domains — cross-domain coupling is eliminated.
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Record a lineage entry to a domain-specific key.
 * Called by the lineage worker alongside recordWorkerEntry().
 * Domain keys are materialized projections — the global list remains canonical.
 *
 * @param {string} domainName — constitutional domain name
 * @param {object} entry — canonical ledger entry
 * @returns {Promise<{ id: string, ts: number }>}
 */
async function recordWorkerDomainEntry(domainName, entry) {
  const redis = _getRedis();
  if (!redis || redis.status !== 'ready' || typeof redis.rpush !== 'function') {
    throw new LineageUnavailableError(
      `Redis status=${redis?.status || 'null'} — domain write authority absent for ${domainName}`
    );
  }
  const domainKey = DOMAIN_KEYS[domainName];
  if (!domainKey) return { id: entry.ledgerId, ts: entry.timestamp }; // unrecognized domain — skip silently
  await redis.rpush(domainKey, JSON.stringify(entry));
  return { id: entry.ledgerId || entry.id, ts: entry.timestamp || entry.ts };
}

/**
 * Returns lineage entries for a specific domain from the domain-partitioned key.
 * Reads directly from lineage:ledger:domain:{domainName} — no global-list-then-filter.
 *
 * @param {string} domainName — constitutional domain name
 * @param {number} [n] — number of recent entries (default: all)
 * @returns {Promise<Array<object>>}
 */
async function getDomainLineage(domainName, n) {
  const redis = _getRedis();
  if (!redis || redis.status !== 'ready' || typeof redis.lrange !== 'function') return [];
  const domainKey = DOMAIN_KEYS[domainName];
  if (!domainKey) return [];
  try {
    let raw;
    if (typeof n === 'number' && n > 0) {
      raw = await redis.lrange(domainKey, -n, -1);
      if (!Array.isArray(raw)) return [];
      return raw.map(item => _parseEntry(item)).filter(Boolean).reverse();
    }
    raw = await redis.lrange(domainKey, 0, -1);
    if (!raw || !Array.isArray(raw)) return [];
    return raw.map(item => _parseEntry(item)).filter(Boolean);
  } catch (err) {
    console.error(`[lineage-ledger] getDomainLineage(${domainName}) error:`, err.message);
    return [];
  }
}

/**
 * Returns domain lineage entries since a given timestamp.
 * Bounded by governance window (last RECONCILIATION_TICK), not arbitrary count.
 * Scans the domain-specific key and returns entries with timestamp >= sinceTimestamp.
 *
 * @param {string} domainName — constitutional domain name
 * @param {number} sinceTimestamp — epoch ms timestamp; entries with ts >= this are returned
 * @returns {Promise<Array<object>>}
 */
async function getDomainLineageSince(domainName, sinceTimestamp) {
  const redis = _getRedis();
  if (!redis || redis.status !== 'ready' || typeof redis.lrange !== 'function') return [];
  const domainKey = DOMAIN_KEYS[domainName];
  if (!domainKey) return [];

  // If no boundary, fetch all domain entries
  if (!sinceTimestamp || sinceTimestamp <= 0) {
    return getDomainLineage(domainName);
  }

  try {
    // Fetch all domain entries (domain keys are bounded by reconciliation cycles)
    // then filter by timestamp. For large ledgers, this can be optimized with
    // a binary search or Redis sorted sets in a future iteration.
    const raw = await redis.lrange(domainKey, 0, -1);
    if (!raw || !Array.isArray(raw)) return [];
    const entries = raw.map(item => _parseEntry(item)).filter(Boolean);
    return entries.filter(e => (e.timestamp || e.ts || 0) >= sinceTimestamp);
  } catch (err) {
    console.error(`[lineage-ledger] getDomainLineageSince(${domainName}) error:`, err.message);
    return [];
  }
}

/**
 * Find the timestamp of the most recent RECONCILIATION_TICK entry for a domain.
 * Scans the domain list backward to locate the governance boundary.
 * Returns 0 if no RECONCILIATION_TICK is found — caller fetches all entries.
 *
 * @param {string} domainName — constitutional domain name
 * @returns {Promise<number>} epoch ms timestamp of last RECONCILIATION_TICK, or 0
 */
async function getLastReconciliationTickTs(domainName) {
  const redis = _getRedis();
  if (!redis || redis.status !== 'ready' || typeof redis.lrange !== 'function') return 0;
  const domainKey = DOMAIN_KEYS[domainName];
  if (!domainKey) return 0;

  try {
    // Scan last 200 entries backward looking for RECONCILIATION_TICK
    const raw = await redis.lrange(domainKey, -200, -1);
    if (!Array.isArray(raw)) return 0;
    const entries = raw.map(item => _parseEntry(item)).filter(Boolean);
    // Entries are newest-first from lrange -200,-1; scan for the tick
    for (const entry of entries) {
      if (entry && entry.nextState === 'RECONCILIATION_TICK') {
        return entry.timestamp || entry.ts || 0;
      }
    }
    return 0;
  } catch (err) {
    console.error(`[lineage-ledger] getLastReconciliationTickTs(${domainName}) error:`, err.message);
    return 0;
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// TEST ONLY — REMOVE AFTER GAP TESTS COMPLETE
// Direct ledger write for test injection (bypasses lineage worker)
// ═══════════════════════════════════════════════════════════════════════════════

async function injectTestEntry(entry) {
  const redis = _getRedis();
  if (!redis || redis.status !== 'ready') return;
  const timestamp = entry._timestampOverride || Date.now();
  const ledgerEntry = {
    ledgerId: entry.ledgerId || `test-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    traceId: entry.traceId || `trace-${Date.now()}`,
    domain: entry.domain,
    entity: entry.entity,
    entityId: entry.entityId,
    previousState: entry.previousState || null,
    nextState: entry.nextState,
    authority: entry.authority || 'test-injector',
    raw: { ...entry.raw, entryType: 'TEST_INJECTED' },
    timestamp,
    ts: timestamp,
    parentTransitionId: entry.parentTransitionId || null,
    correlationId: entry.correlationId || null,
  };
  await redis.rpush(REDIS_KEY_WORKER, JSON.stringify(ledgerEntry));
  return ledgerEntry;
}

// TEST ONLY — REMOVE AFTER GAP TESTS COMPLETE
async function clearDomainLineage(domainName) {
  const redis = _getRedis();
  if (!redis || redis.status !== 'ready') return;
  const all = await getLineage();
  const toDelete = all.filter(e => e.domain === domainName);
  for (const entry of toDelete) {
    await redis.lrem(REDIS_KEY_WORKER, 1, JSON.stringify(entry));
  }
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
  computeHashFromEntries,
  getLineageWithHash,
  getDomainLineage,
  getDomainLineageSince,
  getLastReconciliationTickTs,
  injectTestEntry,
  clearDomainLineage,
  REDIS_KEY_WORKER,
  DOMAIN_KEYS,
  WORKER_KEYS,
  recordWorkerEntry,
  recordWorkerDomainEntry,
  persistWorkerCursor,
  getWorkerCursor,
  persistWorkerHealth,
  persistWorkerProjection,
  persistWorkerDivergences,
};
