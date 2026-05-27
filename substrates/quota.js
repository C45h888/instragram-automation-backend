// substrates/quota.js
// Bounded substrate: Meta API quota tracking.
//
// Owns: parsing X-Business-Use-Case-Usage header, per-account quota state,
//        adaptive delay computation based on current pressure.
// Does NOT own: Instagram API calls, retry decisions, orchestration.

const QUOTA_USAGE_TTL_MS = 60 * 60 * 1000; // 1h — Meta call_count rolls over 1h window

// ── Module state ─────────────────────────────────────────────────────────────

const _quotaUsage = new Map(); // accountId → { pct: number, recordedAt: number }

// ── Header parsing ───────────────────────────────────────────────────────────

/**
 * Parses X-Business-Use-Case-Usage header → max call_count across all Instagram entries.
 * Returns null if header is absent or unparseable — callers treat null as "no data, assume healthy".
 * Per Meta docs: call_count = "percentage of allowed calls made by your app over a rolling one-hour period."
 */
function parseUsageHeader(headerValue) {
  if (!headerValue) return null;
  try {
    const parsed = typeof headerValue === 'string' ? JSON.parse(headerValue) : headerValue;
    let max = 0;
    for (const entries of Object.values(parsed)) {
      for (const entry of Array.isArray(entries) ? entries : [entries]) {
        if (entry.type === 'instagram' && typeof entry.call_count === 'number') {
          max = Math.max(max, entry.call_count);
        }
      }
    }
    return max;
  } catch {
    return null;
  }
}

// ── State tracking ───────────────────────────────────────────────────────────

/**
 * Stores latest quota reading for an account.
 * Called by domain workers after collecting transport results.
 * Raw data only — no governance semantic emission. Tier evaluation
 * and QUOTA_PRESSURE signal emission is performed by
 * the engagement-telemetry-interpreter.
 */
function updateQuotaUsage(accountId, usagePct) {
  _quotaUsage.set(accountId, { pct: usagePct, recordedAt: Date.now() });
}

/**
 * Returns the inter-account delay scaled to current Meta quota pressure.
 * Tiers (per Meta docs: 100% = throttled):
 *   call_count < 50%  → ~17% of maxMs  (healthy — green)
 *   call_count 50–79% → 50% of maxMs   (moderate pressure — yellow)
 *   call_count ≥ 80%  → maxMs          (high pressure — red, full delay)
 * Defaults to green tier when no quota data exists, or when reading is
 * older than QUOTA_USAGE_TTL_MS.
 */
function getAdaptiveDelay(accountId, maxMs) {
  const entry = _quotaUsage.get(accountId);
  const pct = (entry && Date.now() - entry.recordedAt < QUOTA_USAGE_TTL_MS) ? entry.pct : 0;
  if (pct >= 80) return maxMs;
  if (pct >= 50) return Math.round(maxMs * 0.5);
  return Math.round(maxMs * 0.17);
}

module.exports = {
  _quotaUsage,
  QUOTA_USAGE_TTL_MS,
  parseUsageHeader,
  updateQuotaUsage,
  getAdaptiveDelay,
};
