// substrates/metrics-substrate.js
// Governance substrate: raw telemetry aggregation and health signal computation.
//
// Owns: rolling window of execution outcomes, health signal computation.
// Does NOT own: governance classification, orchestration, retry logic.
//
// Raw telemetry flows:
//   executionBridge.executeWithRetry()
//     → metricsSubstrate.record(domain, status, latencyMs)
//
// Cadence polls:
//   cadence.every(90s)
//     → metricsSubstrate.getHealthSignals()
//         → governance.dispatch(WORKER_METRICS_REPORTED, { raw counts })
//             → HSM evaluates threshold deterministically
//
// Architecture invariant:
//   This substrate exposes RAW signals only. Policy classification
//   (what failure rate constitutes degraded) belongs to the governance kernel.
//   No governance semantics live here.

const { getRedisClient } = require('../config/redis');

// ── Rolling window config ───────────────────────────────────────────────────

const METRICS_WINDOW_MS = 60_000;  // 60-second rolling window
const MAX_ENTRIES = 1000;          // memory cap
const REDIS_KEY_PREFIX = 'governance:metrics:';
const REDIS_TTL_S = 300;           // 5min TTL — survive process restarts
const DEGRADED_THRESHOLD = 0.5;    // failure rate at or above this → DEGRADED

// ── In-memory state ─────────────────────────────────────────────────────────

const _entries = []; // [{ ts, domain, accountId, status, latencyMs }]
let _lastHealthState = 'HEALTHY'; // tracks the last emitted health state for transition detection

// ── Redis-backed crash-survival ──────────────────────────────────────────────

/**
 * Persist the current entries window to Redis for crash-survival.
 * Called after each record(), debounced to avoid excessive Redis writes.
 */
let _persistTimer = null;
function _persistToRedis() {
  if (_persistTimer) clearTimeout(_persistTimer);
  _persistTimer = setTimeout(() => {
    _persistTimer = null;
    const redis = getRedisClient();
    if (!redis || redis.status !== 'ready') return;

    const payload = JSON.stringify({
      entries: _entries.slice(-MAX_ENTRIES),
      persistedAt: Date.now(),
    });

    redis.set(`${REDIS_KEY_PREFIX}window`, payload, 'EX', REDIS_TTL_S).catch(err => {
      console.warn(`[metrics-substrate] Failed to persist window: ${err.message}`);
    });
  }, 500); // debounce 500ms
}

/**
 * Rehydrate entries from Redis on startup.
 * Reads the persisted window and merges into in-memory state.
 * Only keeps entries still within the rolling window.
 */
async function rehydrate() {
  const redis = getRedisClient();
  if (!redis || redis.status !== 'ready') return;

  try {
    const raw = await redis.get(`${REDIS_KEY_PREFIX}window`);
    if (!raw) return;

    const parsed = JSON.parse(raw);
    const cutoff = Date.now() - METRICS_WINDOW_MS;
    const staleThreshold = Date.now() - (parsed.persistedAt - Date.now()); // rough age adjustment
    const rehydrated = (parsed.entries || []).filter(e => e.ts >= cutoff);

    if (rehydrated.length > 0) {
      // Merge: add rehydrated entries that aren't already in memory
      const existing = new Set(_entries.map(e => `${e.ts}:${e.domain}:${e.accountId}:${e.status}`));
      for (const entry of rehydrated) {
        const key = `${entry.ts}:${entry.domain}:${entry.accountId}:${entry.status}`;
        if (!existing.has(key)) {
          _entries.push(entry);
        }
      }
      // Sort by ts ascending and cap
      _entries.sort((a, b) => a.ts - b.ts);
      while (_entries.length > MAX_ENTRIES) _entries.shift();
      console.log(`[metrics-substrate] Rehydrated ${rehydrated.length} entries from Redis`);
    }
  } catch (err) {
    console.warn(`[metrics-substrate] Rehydration failed: ${err.message}`);
  }
}

// ── Core API ─────────────────────────────────────────────────────────────────

/**
 * Record a single execution outcome into the rolling window.
 * Called by executionBridge after each executeWithRetry attempt.
 *
 * @param {string} domain - 'comments'|'messages'|'media'|'insights'|'ugc'|'publish:*'
 * @param {'completed'|'failed'} status
 * @param {number} latencyMs - execution time in ms
 * @param {string} [accountId] - business account UUID (optional, for per-account health)
 */
function record(domain, status, latencyMs, accountId) {
  _entries.push({ ts: Date.now(), domain, accountId: accountId || null, status, latencyMs });

  // Evict expired entries
  const cutoff = Date.now() - METRICS_WINDOW_MS;
  while (_entries.length > 0 && _entries[0].ts < cutoff) {
    _entries.shift();
  }

  // Cap memory
  if (_entries.length > MAX_ENTRIES) {
    _entries.splice(0, _entries.length - MAX_ENTRIES);
  }

  _persistToRedis();
}

/**
 * Returns raw aggregate health signals for the rolling window.
 * No policy classification — raw counts only.
 *
 * @returns {{ windowMs: number, total: number, completed: number, failed: number, failureRate: number }}
 */
function getHealthSignals() {
  const cutoff = Date.now() - METRICS_WINDOW_MS;
  const recent = _entries.filter(e => e.ts >= cutoff);
  const completed = recent.filter(e => e.status === 'completed').length;
  const failed = recent.filter(e => e.status === 'failed').length;
  const total = recent.length;

  const computedHealth = total > 0 && failed / total >= DEGRADED_THRESHOLD ? 'DEGRADED' : 'HEALTHY';

  // Emit observability transition if health state changed
  if (computedHealth !== _lastHealthState) {
    const previousState = _lastHealthState;
    _lastHealthState = computedHealth;
    try {
      const observability = require('../control-plane/observability/emitters/transition-emitter');
      observability.transition({
        domain: 'metrics',
        entity: 'health_signal',
        entityId: 'system',
        previousState,
        nextState: computedHealth,
        authority: 'metrics-substrate',
        raw: { failureRate: total > 0 ? failed / total : 0, total, failed, completed },
      });
    } catch (_) {}
  }

  return {
    windowMs: METRICS_WINDOW_MS,
    total,
    completed,
    failed,
    failureRate: total > 0 ? failed / total : 0,
  };
}

/**
 * Returns per-domain breakdown of health signals.
 * Used for domain-level health analysis and targeted degradation diagnosis.
 *
 * @returns {{ [domain: string]: { total: number, completed: number, failed: number, failureRate: number } }}
 */
function getDomainBreakdown() {
  const cutoff = Date.now() - METRICS_WINDOW_MS;
  const recent = _entries.filter(e => e.ts >= cutoff);

  const breakdown = {};
  for (const entry of recent) {
    const d = entry.domain || 'unknown';
    if (!breakdown[d]) {
      breakdown[d] = { total: 0, completed: 0, failed: 0, failureRate: 0 };
    }
    breakdown[d].total++;
    if (entry.status === 'completed') breakdown[d].completed++;
    else if (entry.status === 'failed') breakdown[d].failed++;
  }

  for (const d of Object.keys(breakdown)) {
    const b = breakdown[d];
    b.failureRate = b.total > 0 ? b.failed / b.total : 0;
  }

  return breakdown;
}

/**
 * Returns per-account breakdown of health signals.
 * Extension point for per-account circuit breaker governance.
 *
 * @param {string} accountId
 * @returns {{ total: number, completed: number, failed: number, failureRate: number } | null}
 */
function getAccountHealth(accountId) {
  if (!accountId) return null;
  const cutoff = Date.now() - METRICS_WINDOW_MS;
  const recent = _entries.filter(e => e.ts >= cutoff && e.accountId === accountId);
  const completed = recent.filter(e => e.status === 'completed').length;
  const failed = recent.filter(e => e.status === 'failed').length;
  const total = recent.length;
  return {
    total,
    completed,
    failed,
    failureRate: total > 0 ? failed / total : 0,
  };
}

/**
 * Resets all in-memory entries. Use after recovery or between tests.
 */
function reset() {
  _entries.length = 0;
  const redis = getRedisClient();
  if (redis && redis.status === 'ready') {
    redis.del(`${REDIS_KEY_PREFIX}window`).catch(() => {});
  }
}

/**
 * Rehydrates from Redis on startup. Call once at system boot.
 * @returns {Promise<void>}
 */
async function init() {
  await rehydrate();
}

module.exports = {
  record,
  getHealthSignals,
  getDomainBreakdown,
  getAccountHealth,
  reset,
  init,
  METRICS_WINDOW_MS,
};
