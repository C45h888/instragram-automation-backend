// control-plane/governance/ingress-consistency/substrate.js
// Ingress Consistency Substrate: monitors log vs ledger and signals CK.
// Sits under CK — NOT a domain FSM, NOT registered in DOMAIN_EVENT_MAP.
// Layer 1 of the 4-layer ingress lag architecture.
//
// Responsibilities:
//   - Sample logSize from observability (sync)
//   - Sample ledgerSize from lineageLedger (async, Redis)
//   - Sample transition-writer health (sync, via CK)
//   - Compute lag and derive status
//   - Update cached watermarks
//   - Signal INGRESS_STATE_CHANGED to CK when status changes
//   - Signal TRANSITION_WRITER_HEALTH_CHANGED to CK when writer health changes
//   - Expose getIngressState() for CK's G5 gate to read synchronously
//
// What it does NOT do:
//   - Never restarts Redis, reconstructs lineage, or mutates replay state
//   - Does not own the fix — only monitors and signals

const OBSERVABILITY_SAMPLE_INTERVAL_MS = 2000;   // reduced from 5000ms for faster stall detection
const STALE_THRESHOLD_MS = 6000;

// ── Cached watermarks ──────────────────────────────────────────────────────
let _cachedLogSize = 0;
let _cachedLedgerSize = 0;
let _cachedLag = 0;
let _currentStatus = 'CONSISTENT';
let _lastSampleTs = 0;
let _intervalHandle = null;
let _dispatchGlobal = null;

// ── Transition writer health tracking ──────────────────────────────────────
// Track previous health status to detect transitions
let _prevWriterStatus = null;

// ── Stall detector ─────────────────────────────────────────────────────────
// If log grows but ledger stays flat for N consecutive polls, trigger immediately
let _stallCount = 0;
const STALL_THRESHOLD = 3; // polls with no ledger growth despite log growth = stall

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
  const CK = require('../constitutional-kernel');
  return { observability, lineageLedger, CK };
}

// ── Sample loop ─────────────────────────────────────────────────────────────
async function _sample() {
  const { observability, lineageLedger, CK } = _getObsDeps();

  try {
    // ── Part 1: Lag computation ──────────────────────────────────────────────
    const logSize = observability.query ? observability.query.getLogSize() : 0;

    let ledgerSize = 0;
    try {
      ledgerSize = await lineageLedger.getSize();
    } catch (_) {
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

    // ── Part 2: Stall detection ─────────────────────────────────────────────
    // If log is growing but ledger is not, this is a stall — escalate immediately
    const ledgerGrowing = ledgerSize > _cachedLedgerSize && _cachedLedgerSize > 0;
    const logGrowing = logSize > _cachedLogSize && _cachedLogSize > 0;
    if (logGrowing && !ledgerGrowing) {
      _stallCount++;
      if (_stallCount >= STALL_THRESHOLD && _dispatchGlobal) {
        // Force status to CRITICAL or DEGRADED depending on lag
        const stallStatus = lag > 10 ? 'DEGRADED' : 'CRITICAL';
        _dispatchGlobal({
          type: 'INGRESS_STATE_CHANGED',
          lag,
          status: stallStatus,
          timestamp: _lastSampleTs,
          stall: true,
          stallCount: _stallCount,
        });
        _stallCount = 0; // reset after firing
        return;
      }
    } else {
      _stallCount = 0; // reset on any ledger growth
    }

    // ── Part 3: Signal CK on status change ────────────────────────────────────
    if (newStatus !== prevStatus && _dispatchGlobal) {
      _currentStatus = newStatus;
      _dispatchGlobal({
        type: 'INGRESS_STATE_CHANGED',
        lag,
        status: newStatus,
        timestamp: _lastSampleTs,
      });
    } else if (newStatus !== prevStatus) {
      _currentStatus = newStatus;
    }

    // ── Part 4: Transition-writer health check ────────────────────────────────
    let writerHealth = null;
    try {
      if (CK && typeof CK.getTransitionWriterHealth === 'function') {
        writerHealth = CK.getTransitionWriterHealth();
      }
    } catch (_) {
      writerHealth = null;
    }

    if (writerHealth && writerHealth.status !== _prevWriterStatus) {
      const prevStatus2 = _prevWriterStatus;
      _prevWriterStatus = writerHealth.status;

      // Signal health change to CK — CK routes to telemetry FSM
      if (_dispatchGlobal) {
        _dispatchGlobal({
          type: 'TRANSITION_WRITER_HEALTH_CHANGED',
          namespace: 'aggregate',
          health: writerHealth,
        });
      }
    } else if (writerHealth) {
      _prevWriterStatus = writerHealth.status;
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
  _prevWriterStatus = null;
  _stallCount = 0;
  _intervalHandle = setInterval(_sample, OBSERVABILITY_SAMPLE_INTERVAL_MS);
  _sample(); // fire immediately on start
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