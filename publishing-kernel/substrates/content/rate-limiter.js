// substrates/publishing/content/rate-limiter.js
// Content publishing rate limiter: enforces IG Graph API content publishing limits.
//
// Owns: tracking POST calls to /media and /media_publish endpoints per account.
// Does NOT own: circuit breaker (engagement-fsm), retry policy, credential resolution.
//
// Default: 25 posts per 24h per business account (IG Graph API content publishing limit).
// Window resets after 24 hours from first tracked call.

const DEFAULT_WINDOW_MS = 24 * 60 * 60 * 1000; // 24 hours
const DEFAULT_MAX_CALLS = 25;

const _windows = new Map(); // accountId → { calls, windowStart }

function _getWindow(accountId) {
  const now = Date.now();
  let win = _windows.get(accountId);

  if (!win || now - win.windowStart > DEFAULT_WINDOW_MS) {
    win = { calls: 0, windowStart: now };
    _windows.set(accountId, win);
  }

  return win;
}

/**
 * Check whether the account is currently rate-limited for content publishing.
 * @param {string} accountId
 * @returns {{ limited: boolean, until?: number }}
 */
function isRateLimited(accountId) {
  const win = _getWindow(accountId);
  if (win.calls >= DEFAULT_MAX_CALLS) {
    return { limited: true, until: win.windowStart + DEFAULT_WINDOW_MS };
  }
  return { limited: false };
}

/**
 * Record a content publish attempt. Called by orchestrator before dispatching.
 * @param {string} accountId
 */
function recordCall(accountId) {
  const win = _getWindow(accountId);
  win.calls++;
}

/**
 * Force-reset the rate limiter for an account.
 * @param {string} accountId
 */
function reset(accountId) {
  _windows.delete(accountId);
}

module.exports = { isRateLimited, recordCall, reset };
