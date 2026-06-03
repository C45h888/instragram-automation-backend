// substrates/db/reading/workers/media-worker.js
// Media Worker: governed Supabase reads for media domain.
//
// Owns: getRecentMedia(), getMonitoredHashtags() with FSM-governed caching.
// Does NOT own: governance policy (FSM), routing (CK), IG API calls.
//
// Operationally bounded to: db.media read domain.
// Dispatched by: substrates/db/reading/index.js

const { getSupabaseAdmin } = require('../../../../config/supabase');

// ── Per-worker cache ────────────────────────────────────────────────────────
const _cache = new Map(); // key → { data, expiresAt }

const CACHE_TTL_MS = {
  getRecentMedia:       60_000,   // 1 min
  getMonitoredHashtags: 300_000,  // 5 min
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

/**
 * Execute a governed DB read for the media domain.
 *
 * @param {object} params     — { accountId, query: 'getRecentMedia'|'getMonitoredHashtags' }
 * @param {object} governance — CK module (for DB_READ_OBSERVED emission)
 * @returns {Promise<{success: boolean, data?, error?, latencyMs: number, cached?: boolean}>}
 */
async function execute(params, governance) {
  const { accountId, query } = params;
  const startTime = Date.now();

  // ── Cache hit ────────────────────────────────────────────────────────────
  const cached = _getCached(accountId, query);
  if (cached !== null) {
    _emitObserved(governance, accountId, query, Date.now() - startTime, null, true);
    return { success: true, data: cached, error: null, latencyMs: Date.now() - startTime, cached: true };
  }

  const supabase = getSupabaseAdmin();
  if (!supabase) {
    const elapsed = Date.now() - startTime;
    _emitObserved(governance, accountId, query, elapsed, 'supabase_unavailable');
    return { success: false, data: null, error: 'supabase_unavailable', latencyMs: elapsed };
  }

  try {
    let data;

    if (query === 'getMonitoredHashtags') {
      const result = await supabase
        .from('ugc_monitored_hashtags')
        .select('hashtag')
        .eq('business_account_id', accountId)
        .eq('is_active', true);
      if (result.error) throw result.error;
      data = (result.data || []).map(h => h.hashtag);
    } else {
      // default: getRecentMedia
      const result = await supabase
        .from('instagram_media')
        .select('instagram_media_id')
        .eq('business_account_id', accountId)
        .order('published_at', { ascending: false })
        .limit(10);
      if (result.error) throw result.error;
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
      domain: 'db.media',
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
