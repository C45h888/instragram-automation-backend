// substrates/rate-limiter/index.js
// Rate-limiter substrate: per-domain rate limit state tracker.
//
// Owns: tracking rate limit state per (domain, accountId) pair.
//        Auto-expiring entries, substrate-scoped queries.
// Does NOT own: enforcement policy, circuit breaker decisions,
//               IG API transport, orchestration.
//
// Pure mechanical state tracker. Zero policy interpretation.

const { getSubstrate, getAffectedDomains, getCooldown } = require('./domain-map');

// Internal state: `${domain}:${accountId}` → { until: number (epoch ms) }
const _entries = new Map();

/**
 * Record a rate limit for a domain. Also marks sibling domains in
 * the same substrate — they share the same IG app token rate limit bucket.
 *
 * @param {string} domain — e.g. 'comments', 'messages'
 * @param {string} accountId
 * @param {number|null} igCode — IG error code (4, 17, 32, 613) or null
 * @param {number|null} retryAfterSeconds — from Retry-After header or null
 * @returns {{ affectedDomains: string[], until: number }}
 */
function recordRateLimit(domain, accountId, igCode, retryAfterSeconds) {
  const cooldownSec = retryAfterSeconds || getCooldown(domain, igCode);
  const until = Date.now() + cooldownSec * 1000;
  const affectedDomains = getAffectedDomains(domain);

  for (const d of affectedDomains) {
    const key = `${d}:${accountId}`;
    const existing = _entries.get(key);
    // Only extend, never shorten — preserve the longest cooldown
    if (!existing || until > existing.until) {
      _entries.set(key, { until });
    }
  }

  return { affectedDomains, until };
}

/**
 * Check if a domain is currently rate-limited for an account.
 * Auto-expires stale entries.
 *
 * @param {string} domain
 * @param {string} accountId
 * @returns {{ limited: boolean, until: number|null, wasPreviouslyLimited: boolean }}
 */
function isRateLimited(domain, accountId) {
  const key = `${domain}:${accountId}`;
  const entry = _entries.get(key);
  if (!entry) return { limited: false, until: null, wasPreviouslyLimited: false };

  const now = Date.now();
  if (entry.until <= now) {
    _entries.delete(key);
    return { limited: false, until: null, wasPreviouslyLimited: true };
  }

  return { limited: true, until: entry.until, wasPreviouslyLimited: false };
}

/**
 * Get rate limit state for an entire substrate.
 *
 * @param {string} substrate — 'engagement' | 'ugc' | 'content' | 'insights'
 * @returns {{ domains: { [domain]: { until: number } }, anyLimited: boolean }}
 */
function getSubstrateState(substrate) {
  const now = Date.now();
  const domains = {};
  let anyLimited = false;

  for (const [key, entry] of _entries) {
    const [domain, accountId] = key.split(':');
    if (getSubstrate(domain) !== substrate) continue;

    if (entry.until > now) {
      domains[domain] = { until: entry.until };
      anyLimited = true;
    } else {
      _entries.delete(key); // auto-expire
    }
  }

  return { domains, anyLimited };
}

/**
 * Clear rate limit for a specific domain + account.
 * Also clears sibling domains.
 *
 * @param {string} domain
 * @param {string} accountId
 */
function clearRateLimit(domain, accountId) {
  const affected = getAffectedDomains(domain);
  for (const d of affected) {
    _entries.delete(`${d}:${accountId}`);
  }
}

/**
 * Clear ALL rate limits for a substrate + account.
 * Called when circuit breaker clears — all per-domain limits are stale.
 *
 * @param {string} substrate
 * @param {string} accountId
 */
function clearSubstrate(substrate, accountId) {
  for (const [key] of _entries) {
    const [domain] = key.split(':');
    if (getSubstrate(domain) === substrate) {
      _entries.delete(key);
    }
  }
}

module.exports = {
  recordRateLimit,
  isRateLimited,
  getSubstrateState,
  clearRateLimit,
  clearSubstrate,
  // Re-export for convenience
  getSubstrate,
  getAffectedDomains,
};
