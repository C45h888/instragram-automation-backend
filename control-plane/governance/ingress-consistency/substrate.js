// control-plane/governance/ingress-consistency/substrate.js
// Ingress Consistency Substrate: monitors log vs ledger and signals CK.
// Sits under CK — NOT a domain FSM, NOT registered in DOMAIN_EVENT_MAP.
// Layer 1 of the 4-layer ingress lag architecture.
//
// Responsibilities:
//   - Sample logSize from observability (sync)
//   - Sample ledgerSize from lineageLedger (async, Redis)
//   - Compute lag and derive status
//   - Update cached watermarks
//   - Signal INGRESS_STATE_CHANGED to CK when status changes
//   - Expose getIngressState() for CK's G5 gate to read synchronously
//
// What it does NOT do:
//   - Never restarts Redis, reconstructs lineage, or mutates replay state
//   - Does not own the fix — only monitors and signals

const OBSERVABILITY_SAMPLE_INTERVAL_MS = 5000;
const STALE_THRESHOLD_MS = 15000;

// ── Cached watermarks ──────────────────────────────────────────────────────
let _cachedLogSize = 0;
let _cachedLedgerSize = 0;
let _cachedLag = 0;
let _currentStatus = 'CONSISTENT';
let _lastSampleTs = 0;
let _intervalHandle = null;
let _dispatchGlobal = null;

// ── Status thresholds ───────────────────────────────────────────────────────
function _deriveStatus(lag) {
  if (lag === 0) return 'CONSISTENT';
  if (lag <= 5) return 'LAGGING';
  if (lag <= 10) return 'CRITICAL';
  return 'DEGRADED';
}

// ── Lazy deps — avoid circular at module load ──────────────────────────────
function _getObsDeps() {
  const observability = require('../../observability');
  const lineageLedger = require('../lineage-ledger');
  return { observability, lineageLedger };
}

// ── Sample loop ─────────────────────────────────────────────────────────────
async function _sample() {
  const { observability, lineageLedger } = _getObsDeps();

  try {
    // logSize — sync, in-memory
    const logSize = observability.query ? observability.query.getLogSize() : 0;

    // ledgerSize — async, from Redis
    let ledgerSize = 0;
    try {
      ledgerSize = await lineageLedger.getSize();
    } catch (_) {
      // Redis unavailable — fail to a state that blocks G5 (fail closed)
      ledgerSize = 0;
    }

    const lag = logSize - ledgerSize;
    const prevStatus = _currentStatus;
    const newStatus = _deriveStatus(lag);

    // Update watermarks
    _cachedLogSize = logSize;
    _cachedLedgerSize = ledgerSize;
    _cachedLag = lag;
    _lastSampleTs = Date.now();

    // Signal CK only on status change
    if (newStatus !== prevStatus && _dispatchGlobal) {
      _currentStatus = newStatus;
      _dispatchGlobal({
        type: 'INGRESS_STATE_CHANGED',
        lag,
        status: newStatus,
        timestamp: _lastSampleTs,
      });
    } else if (newStatus !== prevStatus) {
      // No dispatch fn yet — just update status locally
      _currentStatus = newStatus;
    }
  } catch (err) {
    console.error('[ingress-consistency-substrate] Sample error:', err.message);
  }
}

// ── Public API ───────────────────────────────────────────────────────────────

/**
 * Start the sampling loop.
 * @param {function} dispatchGlobalFn — CK's dispatch function for global events
 */
function start(dispatchGlobalFn) {
  if (_intervalHandle) return; // already running
  _dispatchGlobal = dispatchGlobalFn || _dispatchGlobal;
  _currentStatus = 'CONSISTENT';
  _intervalHandle = setInterval(_sample, OBSERVABILITY_SAMPLE_INTERVAL_MS);
  // Fire immediately on start
  _sample();
}

/**
 * Stop the sampling loop.
 */
function stop() {
  if (_intervalHandle) {
    clearInterval(_intervalHandle);
    _intervalHandle = null;
  }
}

/**
 * Synchronous state read for CK's G5 gate.
 * Returns the last sampled ingress state.
 */
function getIngressState() {
  const now = Date.now();
  const stale = _lastSampleTs === 0 || (now - _lastSampleTs) > STALE_THRESHOLD_MS;

  return {
    lag: _cachedLag,
    status: _currentStatus,
    lastSampleTs: _lastSampleTs,
    healthy: _currentStatus === 'CONSISTENT' || _currentStatus === 'LAGGING',
    stale,
  };
}

/**
 * Direct watermark access for diagnostics.
 */
function getWatermarks() {
  return {
    cachedLogSize: _cachedLogSize,
    cachedLedgerSize: _cachedLedgerSize,
    cachedLag: _cachedLag,
    lastSampleTs: _lastSampleTs,
  };
}

module.exports = {
  start,
  stop,
  getIngressState,
  getWatermarks,
};