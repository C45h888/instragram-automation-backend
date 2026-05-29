/**
 * Runtime Monitor Probe
 * ====================
 * Polls runtime state at regular intervals during a soak test.
 * Captures structural snapshots of the observability plane and ledger
 * to detect constitutional regressions without modifying runtime state.
 *
 * Can run standalone (Node.js process polling itself) or as a background
 * co-process alongside a soak test.
 *
 * Usage:
 *   const { startMonitor, stopMonitor, getSnapshots, getViolations } = require('./helpers/runtime-monitor');
 *   await startMonitor({ intervalMs: 30000 });
 *   // ... run soak test ...
 *   await stopMonitor();
 *   const report = getReport();
 */

const DEFAULT_INTERVAL_MS = 30_000;
const DEFAULT_SOAK_MS = 1_800_000; // 30 min

let _monitorInterval = null;
let _snapshots = [];
let _violations = [];
let _startedAt = null;
let _intervalMs = DEFAULT_INTERVAL_MS;
let _stopped = false;

/**
 * Start polling runtime state.
 * @param {{ intervalMs?: number, ledgerLookback?: number }} opts
 */
async function startMonitor({ intervalMs = DEFAULT_INTERVAL_MS, ledgerLookback = 500 } = {}) {
  _intervalMs = intervalMs;
  _snapshots = [];
  _violations = [];
  _startedAt = Date.now();
  _stopped = false;

  const observability = require('../../control-plane/observability/index.js');
  const lineageLedger = require('../../control-plane/governance/lineage-ledger.js');
  const { assertNoTimestampRegression, assertMonotonicCursors } = require('./constitutional-invariants');

  await observability.init();

  const poll = async () => {
    if (_stopped) return;

    const elapsed = Date.now() - _startedAt;
    const logSize = observability.query.getLogSize();
    const snapshot = observability.query.getFullSnapshot();
    const ledger = await lineageLedger.getLineage(ledgerLookback);
    const ledgerSize = ledger.length;

    let snapshot_ok = true;
    let violation_desc = null;

    // Check 1: Timestamp monotonicity
    let tsRegressions = 0;
    for (let i = 1; i < ledger.length; i++) {
      if (ledger[i].timestamp < ledger[i - 1].timestamp) {
        tsRegressions++;
      }
    }
    if (tsRegressions > 0) {
      snapshot_ok = false;
      violation_desc = `timestamp_regression:${tsRegressions}`;
    }

    // Check 2: No corruption markers
    const corrupted = ledger.filter(e => e.raw?.raw?.corrupted === true);
    if (corrupted.length > 0) {
      snapshot_ok = false;
      violation_desc = `corruption_markers:${corrupted.length}`;
    }

    // Check 3: Ledger growth monotonic
    const prevSize = _snapshots.length > 0 ? _snapshots[_snapshots.length - 1].ledgerSize : 0;
    if (ledgerSize < prevSize) {
      snapshot_ok = false;
      violation_desc = `ledger_shrink:${prevSize}→${ledgerSize}`;
    }

    // Check 4: Log size is non-negative
    if (logSize < 0) {
      snapshot_ok = false;
      violation_desc = `negative_log_size:${logSize}`;
    }

    _snapshots.push({
      elapsed_ms: elapsed,
      elapsed_s: Math.round(elapsed / 1000),
      logSize,
      ledgerSize,
      ledgerGrowDelta: ledgerSize - prevSize,
      transitionCount: snapshot.transitionCount,
      domainCount: Object.keys(snapshot.domains || {}).length,
      snapshot_ok,
      violation_desc,
    });

    if (!snapshot_ok) {
      _violations.push({ elapsed_ms: elapsed, reason: violation_desc, ledgerSize, logSize });
    }

    // Drain pending promises to avoid unhandled rejections
    await Promise.resolve();
  };

  // Immediate first snapshot
  await poll();

  _monitorInterval = setInterval(async () => {
    try {
      await poll();
    } catch (err) {
      _violations.push({ elapsed_ms: Date.now() - _startedAt, reason: `probe_error:${err.message}` });
    }
  }, _intervalMs);

  return { startedAt: _startedAt, intervalMs: _intervalMs };
}

/**
 * Stop the monitoring loop.
 */
async function stopMonitor() {
  _stopped = true;
  if (_monitorInterval) {
    clearInterval(_monitorInterval);
    _monitorInterval = null;
  }
  const observability = require('../../control-plane/observability/index.js');
  await observability.stop();
}

/**
 * Get all snapshots captured so far.
 * @returns {Array<object>}
 */
function getSnapshots() {
  return [..._snapshots];
}

/**
 * Get all violations detected.
 * @returns {Array<object>}
 */
function getViolations() {
  return [..._violations];
}

/**
 * Get a full monitoring report.
 * @returns {object}
 */
function getReport() {
  const duration_ms = Date.now() - _startedAt;
  return {
    duration_ms,
    duration_s: Math.round(duration_ms / 1000),
    intervalMs: _intervalMs,
    snapshots: getSnapshots(),
    violations: getViolations(),
    violationCount: _violations.length,
    snapshotCount: _snapshots.length,
    ok: _violations.length === 0,
    summary: {
      totalSnapshots: _snapshots.length,
      totalViolations: _violations.length,
      lastLedgerSize: _snapshots.length > 0 ? _snapshots[_snapshots.length - 1].ledgerSize : 0,
      maxLedgerSize: _snapshots.reduce((m, s) => Math.max(m, s.ledgerSize), 0),
      firstViolation: _violations.length > 0 ? _violations[0] : null,
    },
  };
}

/**
 * Reset monitor state (for re-use in multiple test cycles).
 */
function reset() {
  _snapshots = [];
  _violations = [];
  _startedAt = null;
  _stopped = false;
  if (_monitorInterval) {
    clearInterval(_monitorInterval);
    _monitorInterval = null;
  }
}

module.exports = {
  startMonitor,
  stopMonitor,
  getSnapshots,
  getViolations,
  getReport,
  reset,
  DEFAULT_INTERVAL_MS,
  DEFAULT_SOAK_MS,
};
