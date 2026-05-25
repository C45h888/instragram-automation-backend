// substrates/sync-substrate.js
// Sync Substrate: intent polling cadence.
//
// Owns: Redis queue polling for acquisition intents, round-robin domain/account
//        rotation. Polls at 10s interval when kernel is HEALTHY.IDLE
//        (signaled via START/STOP actions).
// Does NOT own: state transitions, governance decisions.
//
// Architecture invariant:
//   Signals UP → governance.dispatch(ACQUISITION_INTENT_RECEIVED)
//   Authority DOWN → kernel controls start/stop via actions
//   Orchestrator wires the action subscriber to this module.
//
// Polling interval is 10s — driven internally by this substrate.
// Kernel sends START/STOP to control polling state.

const ACQUISITION_DOMAINS = ['comments', 'messages', 'ugc', 'insights', 'media'];
const ACQUISITION_PUBLISH_DOMAINS = ['media', 'ugc', 'messaging'];
const POLL_INTERVAL_MS = 10_000;

let _redis = null;
let _governance = null;
let _running = false;
let _pollInterval = null;
let _lastPolledDomainIdx = 0;

function _allDomains() {
  return [...ACQUISITION_DOMAINS, ...ACQUISITION_PUBLISH_DOMAINS.map(d => `publish:${d}`)];
}

function _poll() {
  if (!_running || !_redis || !_governance) return;

  const accountIds = _governance.getAccountIds();
  if (!accountIds || accountIds.length === 0) return;

  const allDomains = _allDomains();
  const domain = allDomains[_lastPolledDomainIdx % allDomains.length];
  _lastPolledDomainIdx = (_lastPolledDomainIdx + 1) % allDomains.length;

  const accountIdx = Math.floor(_lastPolledDomainIdx / allDomains.length) % accountIds.length;
  const accountId = accountIds[accountIdx];
  if (!accountId) return;

  const queueKey = `supervisor:acquisitions:${domain}:${accountId}`;

  _redis.lpop(queueKey).then(raw => {
    if (!raw) return;
    let intent;
    try { intent = JSON.parse(raw); } catch { return; }
    if (!intent || !intent.intent_id) return;

    _governance.dispatch({
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
  }
}

/**
 * Start the sync substrate.
 * @param {object} redis — ioredis client instance
 * @param {object} governance — governance kernel module
 */
function start(redis, governance) {
  if (!redis || !governance) {
    console.error('[sync-substrate] start() requires redis client and governance module');
    return;
  }
  _redis = redis;
  _governance = governance;
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
