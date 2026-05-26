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
//   - ONLY constitutional-kernel.js reads and writes this ledger directly
//   - Domain FSMs write via ctx.recordLineage() passed from CK (mediated)
//   - Domain FSMs CANNOT read from this ledger — state is inferred via CK
//   - Lineage is the canonical source of truth; runtime state is a projection
//
// Contract:
//   ledger.record(entry)          → lineageId
//   ledger.getLineage([n])         → Array<GovernanceEvent>
//   ledger.getSize()              → number
//   ledger.materializeState(enries)→ { globalState, domains, lastEvent, entryCount }
//   ledger.rehydrate()            → { loaded, latestTs } (async, Redis-backed)

const crypto = require('crypto');

const _lineage = []; // append-only, never mutated or deleted

// Redis client — lazy initialization
let _redis = null;

function _getRedis() {
  if (!_redis) {
    // eslint-disable-next-line global-require
    const lib = require('../../config/redis');
    _redis = lib.redis || lib;
  }
  return _redis;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Persistence — write-through to Redis
// ═══════════════════════════════════════════════════════════════════════════════

const REDIS_KEY = 'governance:lineage';

/**
 * Persist a single entry to Redis as a JSON string.
 * Called immediately after appending to _lineage[] on every record().
 *
 * @param {object} entry — the lineage entry to persist
 */
function _persist(entry) {
  try {
    const redis = _getRedis();
    if (redis && typeof redis.rpush === 'function') {
      redis.rpush(REDIS_KEY, JSON.stringify(entry));
    }
  } catch (err) {
    // If Redis write fails, we still have in-memory record.
    // Governance does not proceed if record() throws — fail-safe.
    console.error('[lineage-ledger] Redis persist error:', err.message);
    throw err;
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Core API
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Record a governance event in the append-only lineage log.
 * Write-through to Redis ensures durability.
 *
 * @param {{ authority: string, layer: 'constitutional'|'domain',
 *           intent: string, priorState: string, resultantState: string,
 *           legitimacy?: object, meta?: object }} entry
 * @returns {{ id: string, ts: number }} the recorded entry identifiers
 * @throws if Redis persist fails (governance should not proceed)
 */
function record(entry) {
  const event = {
    id: crypto.randomUUID(),
    ts: Date.now(),
    authority: entry.authority || 'unknown',
    layer: entry.layer || 'domain',
    intent: entry.intent,
    priorState: entry.priorState,
    resultantState: entry.resultantState,
    legitimacy: entry.legitimacy || null,
    meta: entry.meta || {},
  };
  _lineage.push(event);
  _persist(event); // write-through — throws if Redis fails
  return { id: event.id, ts: event.ts };
}

/**
 * Returns the last N lineage records (or all if n is omitted).
 * Records are append-only and never mutated.
 *
 * @param {number} [n] — number of recent records to return
 * @returns {Array<object>}
 */
function getLineage(n) {
  if (typeof n === 'number' && n > 0) {
    return _lineage.slice(-n);
  }
  return [..._lineage];
}

/**
 * Returns total number of recorded lineage events.
 * @returns {number}
 */
function getSize() {
  return _lineage.length;
}

// ═══════════════════════════════════════════════════════════════════════════════
// State Materialization — computes current state from lineage entries
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Materialize the current global and domain states from a list of lineage entries.
 * This is a PURE function — it does not mutate _lineage.
 *
 * Logic:
 *   globalState  ← last entry where layer === 'constitutional', use its resultantState
 *   domains.{name} ← last entry where authority === '{name}-fsm' and layer === 'domain',
 *                    use its resultantState
 *
 * @param {Array<object>} entries — lineage entries to analyze
 * @returns {{ globalState: string, domains: { acquisition: string, publishing: string, scheduling: string }, lastEvent: object|null, entryCount: number }}
 */
function materializeState(entries) {
  if (!entries || entries.length === 0) {
    return {
      globalState: 'BOOTING',
      domains: { acquisition: 'IDLE', publishing: 'IDLE', scheduling: 'IDLE' },
      lastEvent: null,
      entryCount: 0,
    };
  }

  let globalState = 'BOOTING';
  const domains = { acquisition: 'IDLE', publishing: 'IDLE', scheduling: 'IDLE' };
  let lastEvent = null;

  for (const entry of entries) {
    if (!entry || typeof entry.resultantState !== 'string') continue;

    if (entry.layer === 'constitutional') {
      globalState = entry.resultantState;
      lastEvent = entry;
    } else if (entry.layer === 'domain' && entry.authority) {
      // authority format: '{name}-fsm' e.g. 'acquisition-fsm'
      const domainName = entry.authority.replace('-fsm', '');
      if (domains.hasOwnProperty(domainName)) {
        domains[domainName] = entry.resultantState;
        lastEvent = entry;
      }
    }
  }

  return { globalState, domains, lastEvent, entryCount: entries.length };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Reconciliation Epoch — snapshot marker for reconciliation cycles
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Create a reconciliation epoch marker in the lineage ledger.
 * Appends an epoch marker entry and persists it to Redis.
 * Called by the reconciliation engine at the start of each cycle.
 *
 * @returns {{ epochId: string, lineagePosition: number }}
 */
function createEpoch() {
  const epochId = crypto.randomUUID();
  const lineagePosition = _lineage.length;

  const epochEntry = {
    id: crypto.randomUUID(),
    ts: Date.now(),
    authority: 'reconciliation-engine',
    layer: 'constitutional',
    intent: 'RECONCILIATION_EPOCH',
    priorState: 'HEALTHY',
    resultantState: 'EPOCH_CREATED',
    legitimacy: null,
    meta: { epochId, lineagePosition },
  };
  _lineage.push(epochEntry);
  _persist(epochEntry);
  return { epochId, lineagePosition };
}

/**
 * Compute a deterministic SHA-256 constitutional hash from current lineage.
 * Hash includes: entry count, global state, domain projections, last event timestamp.
 *
 * @returns {string} hex-encoded SHA-256 hash
 */
function computeHash() {
  const materialized = materializeState(_lineage);
  const payload = JSON.stringify({
    count: _lineage.length,
    globalState: materialized.globalState,
    domains: materialized.domains,
    lastTs: materialized.lastEvent ? materialized.lastEvent.ts : null,
  });
  return crypto.createHash('sha256').update(payload).digest('hex');
}

/**
 * Returns the last N lineage entries for a specific domain.
 * Filters by authority matching '{domainName}-fsm'.
 *
 * @param {string} domainName — 'acquisition' | 'publishing' | 'scheduling'
 * @param {number} [n] — number of recent entries to return (default: all)
 * @returns {Array<object>}
 */
function getDomainLineage(domainName, n) {
  const authority = `${domainName}-fsm`;
  const filtered = _lineage.filter(e => e.authority === authority);
  if (typeof n === 'number' && n > 0) {
    return filtered.slice(-n);
  }
  return filtered;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Rehydration — load persisted lineage from Redis on boot
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Load all persisted lineage entries from Redis into the module-level _lineage[].
 * Called by constitutional-kernel on boot to reconstruct state from persistent memory.
 *
 * @returns {Promise<{ loaded: number, latestTs: number | null }>}
 * @throws if Redis is unavailable — fast fail ensures CK does not boot with stale state
 */
async function rehydrate() {
  const redis = _getRedis();
  if (!redis || typeof redis.lrange !== 'function') {
    // Redis not available — proceed with empty lineage (graceful fallback)
    console.warn('[lineage-ledger] Redis not available — starting with empty lineage');
    return { loaded: 0, latestTs: null };
  }

  try {
    const raw = await redis.lrange(REDIS_KEY, 0, -1);
    if (!raw || raw.length === 0) {
      return { loaded: 0, latestTs: null };
    }

    let loaded = 0;
    let latestTs = null;

    for (const item of raw) {
      try {
        const entry = typeof item === 'string' ? JSON.parse(item) : item;
        if (entry && entry.id && entry.ts) {
          _lineage.push(entry);
          loaded++;
          if (latestTs === null || entry.ts > latestTs) latestTs = entry.ts;
        }
      } catch (parseErr) {
        // Skip corrupt entries — log warning but continue
        console.warn('[lineage-ledger] Skipping corrupt lineage entry:', parseErr.message);
      }
    }

    console.log(`[lineage-ledger] Rehydrated ${loaded} entries from Redis`);
    return { loaded, latestTs };
  } catch (err) {
    console.error('[lineage-ledger] Rehydrate failed:', err.message);
    throw err; // fast fail — CK should not boot if lineage cannot be loaded
  }
}

module.exports = { record, getLineage, getSize, materializeState, rehydrate, createEpoch, computeHash, getDomainLineage };
