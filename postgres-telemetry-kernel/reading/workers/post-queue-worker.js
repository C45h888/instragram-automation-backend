// substrates/db/reading/workers/post-queue-worker.js
// Post Queue + Scheduled Posts Worker: governed reads for publishing domain.
//
// Owns: query routing + caching. All Supabase I/O delegated to bedrock.
// Does NOT own: governance policy (FSM), routing (CK), IG API calls, status mutations.
//
// Operationally bounded to: db.post-queue, db.scheduled-posts read domains.
// Dispatched by: substrates/db/reading/index.js

const bedrock = require('../../bedrock');

// ── Per-worker cache ────────────────────────────────────────────────────────
const _cache = new Map();

const CACHE_TTL_MS = {
  getPendingPostQueue:         60_000,
  getApprovedScheduledPosts:  120_000,
};

function _cacheKey(accountId, query) {
  return `${query}:${accountId}`;
}

function _getCached(accountId, query) {
  const key = _cacheKey(accountId, query);
  const entry = _cache.get(key);
  if (entry && Date.now() < entry.expiresAt) return entry.data;
  if (entry) _cache.delete(key);
  return null;
}

function _setCache(accountId, query, data) {
  const ttl = CACHE_TTL_MS[query] || 60_000;
  const key = _cacheKey(accountId, query);
  _cache.set(key, { data, expiresAt: Date.now() + ttl });
}

// ── Execute ──────────────────────────────────────────────────────────────────

async function execute(params, governance) {
  const { accountId, query } = params;
  const startTime = Date.now();

  // ── Cache hit ────────────────────────────────────────────────────────────
  const cached = _getCached(accountId, query);
  if (cached !== null) {
    _emitObserved(governance, accountId, query, Date.now() - startTime, null, true);
    return { success: true, data: cached, error: null, latencyMs: Date.now() - startTime, cached: true };
  }

  try {
    let data;

    if (query === 'getApprovedScheduledPosts') {
      const result = await bedrock.publishing.getScheduledPosts(accountId);
      if (!result.success) throw new Error(result.error);
      data = result.data || [];
    } else {
      // default: getPendingPostQueue
      const result = await bedrock.publishing.getPendingPublications(accountId);
      if (!result.success) throw new Error(result.error);
      data = result.data || [];
    }

    const elapsed = Date.now() - startTime;
    _setCache(accountId, query, data);
    _emitObserved(governance, accountId, query, elapsed);
    return { success: true, data, error: null, latencyMs: elapsed };

  } catch (err) {
    const elapsed = Date.now() - startTime;
    _emitObserved(governance, accountId, query, elapsed, err.message);
    return { success: false, data: null, error: err.message, latencyMs: elapsed };
  }
}

// ── Telemetry ────────────────────────────────────────────────────────────────

function _emitObserved(governance, accountId, query, latencyMs, error = null, cached = false) {
  if (!governance || typeof governance.dispatch !== 'function') return;
  try {
    governance.dispatch({
      type: 'DB_READ_OBSERVED',
      domain: query.startsWith('getPending') ? 'db.post-queue' : 'db.scheduled-posts',
      query,
      accountId,
      latencyMs,
      error,
      cached,
    });
  } catch (_) {}
}

// ── Cache management ─────────────────────────────────────────────────────────

function clearCache(accountId) {
  if (accountId) {
    for (const key of _cache.keys()) {
      if (key.endsWith(`:${accountId}`)) _cache.delete(key);
    }
  } else {
    _cache.clear();
  }
}

module.exports = { execute, clearCache };
