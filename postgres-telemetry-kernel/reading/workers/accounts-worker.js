// substrates/db/reading/workers/accounts-worker.js
// Accounts Worker: governed Supabase reads for the accounts domain.
//
// Owns: query routing + caching. All Supabase I/O delegated to bedrock.
// Does NOT own: governance policy (FSM), routing (CK), IG API calls, account mutations.
//
// Operationally bounded to: db.accounts read domain.
// Dispatched by: substrates/db/reading/index.js (via dispatchRead).

const bedrock = require('../../bedrock');

// ── Per-worker cache ────────────────────────────────────────────────────────
const _cache = new Map();
const crypto = require('crypto');

const CACHE_TTL_MS = {
  getActiveAccounts: 30_000,
  igIdToUserId:     30_000,
  igThreadIdToUuid: 30_000,
};

function _cacheKey(query, context = null) {
  if (context) return `${query}:${context}`;
  return `${query}:null`;
}

function _sortedHash(arr) {
  return crypto.createHash('sha256').update([...arr].sort().join(',')).digest('hex').slice(0, 16);
}

function _getCached(query, context = null) {
  const key = _cacheKey(query, context);
  const entry = _cache.get(key);
  if (entry && Date.now() < entry.expiresAt) return entry.data;
  if (entry) _cache.delete(key);
  return null;
}

function _setCache(query, data, context = null) {
  const ttl = CACHE_TTL_MS[query] || 30_000;
  const key = _cacheKey(query, context);
  _cache.set(key, { data, expiresAt: Date.now() + ttl });
}

function _getStale(query, context = null) {
  const entry = _cache.get(_cacheKey(query, context));
  return entry ? entry.data : null;
}

// ── Execute ──────────────────────────────────────────────────────────────────

async function execute(params, governance) {
  const { query, igIds, threadIds } = params;
  const startTime = Date.now();

  const VALID_QUERIES = ['getActiveAccounts', 'igIdToUserId', 'igThreadIdToUuid', 'getBusinessAccount'];
  if (!VALID_QUERIES.includes(query)) {
    const elapsed = Date.now() - startTime;
    _emitObserved(governance, query, elapsed, `unknown_query: ${query}`);
    return { success: false, data: null, error: `unknown_query: ${query}`, latencyMs: elapsed };
  }

  // ── igIdToUserId: batch-resolve instagram_business_id → user_id ──────────
  if (query === 'igIdToUserId') {
    const sorted = [...new Set(igIds || [])].sort();
    if (sorted.length === 0) {
      return { success: true, data: [], error: null, latencyMs: Date.now() - startTime, cached: false };
    }
    const hashCtx = _sortedHash(sorted);

    const cached = _getCached(query, hashCtx);
    if (cached !== null) {
      _emitObserved(governance, query, Date.now() - startTime, null, true);
      return { success: true, data: cached, error: null, latencyMs: Date.now() - startTime, cached: true };
    }

    try {
      const result = await bedrock.token.resolveBusinessAccountIds(sorted);
      if (!result.success) throw new Error(result.error);
      const data = result.data || [];
      const elapsed = Date.now() - startTime;
      _setCache(query, data, hashCtx);
      _emitObserved(governance, query, elapsed);
      return { success: true, data, error: null, latencyMs: elapsed };
    } catch (err) {
      const elapsed = Date.now() - startTime;
      _emitObserved(governance, query, elapsed, err.message);
      return { success: false, data: null, error: err.message, latencyMs: elapsed };
    }
  }

  // ── igThreadIdToUuid: batch-resolve instagram_thread_id → UUID ────────
  if (query === 'igThreadIdToUuid') {
    const sorted = [...new Set(threadIds || [])].sort();
    if (sorted.length === 0) {
      return { success: true, data: [], error: null, latencyMs: Date.now() - startTime, cached: false };
    }
    const hashCtx = _sortedHash(sorted);

    const cached = _getCached(query, hashCtx);
    if (cached !== null) {
      _emitObserved(governance, query, Date.now() - startTime, null, true);
      return { success: true, data: cached, error: null, latencyMs: Date.now() - startTime, cached: true };
    }

    try {
      const result = await bedrock.ugc.resolveThreadIds(sorted);
      if (!result.success) throw new Error(result.error);
      const data = result.data || [];
      const elapsed = Date.now() - startTime;
      _setCache(query, data, hashCtx);
      _emitObserved(governance, query, elapsed);
      return { success: true, data, error: null, latencyMs: elapsed };
    } catch (err) {
      const elapsed = Date.now() - startTime;
      _emitObserved(governance, query, elapsed, err.message);
      return { success: false, data: null, error: err.message, latencyMs: elapsed };
    }
  }

  // ── getBusinessAccount — single business account by UUID ─────────────────
  if (query === 'getBusinessAccount') {
    const { businessAccountId } = params;
    if (!businessAccountId) {
      return { success: false, data: null, error: 'businessAccountId required', latencyMs: Date.now() - startTime };
    }

    const baCached = _getCached('getBusinessAccount', businessAccountId);
    if (baCached !== null) {
      _emitObserved(governance, query, Date.now() - startTime, null, true);
      return { success: true, data: baCached, error: null, latencyMs: Date.now() - startTime, cached: true };
    }

    try {
      const result = await bedrock.token.getBusinessAccount(businessAccountId);
      if (!result.success) {
        const elapsed = Date.now() - startTime;
        if (result.error === 'business_account_not_found') return { success: false, data: null, error: 'business_account_not_found', latencyMs: elapsed };
        return { success: false, data: null, error: result.error, latencyMs: elapsed };
      }
      const elapsed = Date.now() - startTime;
      _setCache('getBusinessAccount', result.data, businessAccountId);
      _emitObserved(governance, query, elapsed);
      return { success: true, data: result.data, error: null, latencyMs: elapsed };
    } catch (err) {
      const elapsed = Date.now() - startTime;
      _emitObserved(governance, query, elapsed, err.message);
      return { success: false, data: null, error: err.message, latencyMs: elapsed };
    }
  }

  // ── getActiveAccounts ──────────────────────────────────────────────────
  const cached = _getCached(query);
  if (cached !== null) {
    _emitObserved(governance, query, Date.now() - startTime, null, true);
    return { success: true, data: cached, error: null, latencyMs: Date.now() - startTime, cached: true };
  }

  try {
    const result = await bedrock.token.getActiveBusinessAccounts();
    if (!result.success) throw new Error(result.error);
    const data = result.data || [];
    const elapsed = Date.now() - startTime;
    _setCache(query, data);
    _emitObserved(governance, query, elapsed);
    return { success: true, data, error: null, latencyMs: elapsed };
  } catch (err) {
    const elapsed = Date.now() - startTime;
    // Graceful degradation: return stale cache on error
    const stale = _getStale(query);
    if (stale !== null) {
      _emitObserved(governance, query, elapsed, err.message, false);
      return { success: true, data: stale, error: null, latencyMs: elapsed, stale: true };
    }
    _emitObserved(governance, query, elapsed, err.message);
    return { success: false, data: null, error: err.message, latencyMs: elapsed };
  }
}

// ── Telemetry ────────────────────────────────────────────────────────────────

function _emitObserved(governance, query, latencyMs, error = null, cached = false) {
  if (!governance || typeof governance.dispatch !== 'function') return;
  try {
    governance.dispatch({
      type: 'DB_READ_OBSERVED',
      domain: 'db.accounts',
      query,
      accountId: null,
      latencyMs,
      error,
      cached,
    });
  } catch (_) {}
}

// ── Cache management ─────────────────────────────────────────────────────────

function clearCache(accountId) {
  _cache.clear();
}

module.exports = { execute, clearCache };
