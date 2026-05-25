// substrates/sync-substrate.js
// Sync Substrate: intent polling cadence.
//
// Owns: Redis queue polling for acquisition intents, round-robin domain/account
//        rotation. Polls at 10s interval when kernel signals HEALTHY.IDLE
//        (controlled via START/STOP actions from governance).
// Does NOT own: state transitions, governance decisions, account discovery.
//
// Architecture invariant:
//   Signals UP   → emit via onIntent callback (no governance reference)
//   Authority DOWN → kernel controls polling via START/STOP actions
//   Account list DOWN → kernel sends UPDATE_ACCOUNT_LIST for polling targets
//
// Polling interval is 10s — driven internally by this substrate.
// Kernel sends START/STOP to control polling state.

const ACQUISITION_DOMAINS = ['comments', 'messages', 'ugc', 'insights', 'media'];
const ACQUISITION_PUBLISH_DOMAINS = ['publish:media', 'publish:ugc', 'publish:messaging'];
const POLL_INTERVAL_MS = 10_000;

let _redis = null;
let _onIntent = null; // (event) => void — no governance reference
let _running = false;
let _pollInterval = null;
let _lastPolledDomainIdx = 0;
let _accountIds = []; // received via UPDATE_ACCOUNT_LIST action from governance

function _allDomains() {
  return [...ACQUISITION_DOMAINS, ...ACQUISITION_PUBLISH_DOMAINS];
}

function _poll() {
  if (!_running || !_redis || !_onIntent) return;
  if (!_accountIds || _accountIds.length === 0) return;

  const allDomains = _allDomains();
  const domain = allDomains[_lastPolledDomainIdx % allDomains.length];
  _lastPolledDomainIdx = (_lastPolledDomainIdx + 1) % allDomains.length;

  const accountIdx = Math.floor(_lastPolledDomainIdx / allDomains.length) % _accountIds.length;
  const accountId = _accountIds[accountIdx];
  if (!accountId) return;

  const queueKey = `supervisor:acquisitions:${domain}:${accountId}`;

  _redis.lpop(queueKey).then(raw => {
    if (!raw) return;
    let intent;
    try { intent = JSON.parse(raw); } catch { return; }
    if (!intent || !intent.intent_id) return;

    // Emit upward via callback — no governance reference
    _onIntent({
      type: 'ACQUISITION_INTENT_RECEIVED',
      accountId,
      domain,
      intentId: intent.intent_id,
      params: intent.payload || intent.parameters || {},
    });
  }).catch(err => {
    console.error(`[sync-substrate] Poll error (${queueKey}):`, err.message);
  });
}

/**
 * Handle kernel actions. Called by orchestrator's onAction subscriber.
 * START_INTENT_DISCOVERY → begin polling at 10s interval
 * STOP_INTENT_DISCOVERY  → stop polling
 * UPDATE_ACCOUNT_LIST    → update account list for round-robin polling
 */
function onKernelSignal(action) {
  if (action.type === 'START_INTENT_DISCOVERY') {
    if (!_pollInterval) {
      _running = true;
      _pollInterval = setInterval(_poll, POLL_INTERVAL_MS);
      _pollInterval.unref();
    }
  } else if (action.type === 'STOP_INTENT_DISCOVERY') {
    _running = false;
    if (_pollInterval) {
      clearInterval(_pollInterval);
      _pollInterval = null;
    }
  } else if (action.type === 'UPDATE_ACCOUNT_LIST') {
    _accountIds = Array.isArray(action.accountIds) ? action.accountIds : [];
  }
}

/**
 * Start the sync substrate.
 * @param {object} redis — ioredis client instance
 * @param {Function} onIntent — signal callback: (event) => void, receives ACQUISITION_INTENT_RECEIVED
 */
function start(redis, onIntent) {
  if (!redis || typeof onIntent !== 'function') {
    console.error('[sync-substrate] start() requires redis client and onIntent callback');
    return;
  }
  _redis = redis;
  _onIntent = onIntent;
  console.log('[sync-substrate] Started — waiting for START_INTENT_DISCOVERY from kernel');
}

/**
 * Stop the sync substrate and clear polling interval.
 */
function stop() {
  _running = false;
  if (_pollInterval) {
    clearInterval(_pollInterval);
    _pollInterval = null;
  }
  console.log('[sync-substrate] Stopped');
}

/**
 * Returns whether the substrate is currently polling.
 * @returns {boolean}
 */
function isRunning() {
  return _running;
}

module.exports = {
  start,
  stop,
  onKernelSignal,
  isRunning,
};
