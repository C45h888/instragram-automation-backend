// substrates/publishing/engagement/rate-limiter.js
// Engagement publishing rate limiter: enforces IG Graph API messaging limits.
//
// Owns: tracking POST calls to /messages endpoint per account.
// Does NOT own: circuit breaker (engagement-fsm), retry policy, credential resolution.
//
// Instagram messaging rate limits are conversation-based, not global.
// This limiter tracks aggregate messaging call volume per account.

const DEFAULT_WINDOW_MS = 60 * 60 * 1000; // 1 hour
const DEFAULT_MAX_CALLS = 200; // conservative for messaging endpoints

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
 * Check whether the account is currently rate-limited for engagement publishing.
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
 * Record an engagement publish attempt.
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
