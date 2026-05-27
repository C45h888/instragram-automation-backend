// control-plane/telemetry-workers/runtime-projection-worker.js
// Runtime Projection Worker: synthesizes runtimeState, executionPressure,
// retryPressure, cadenceHealth.
//
// Owns: semantic synthesis of runtime execution signals from metrics substrate.
// Does NOT own: governance decisions, lineage, FSM semantics.
//
// Projection Type: RUNTIME_PROJECTION
// Source: metricsSubstrate health signals + domain breakdown
//
// Determinism contract:
//   same healthSignals + same domainBreakdown + same version
//   = ALWAYS same runtimeState, executionPressure, retryPressure, cadenceHealth

const { BaseProjectionWorker } = require('./base-projection-worker');
const metricsSubstrate = require('../../substrates/metrics-substrate');

const PROJECTION_TYPE = 'RUNTIME_PROJECTION';
const POLL_INTERVAL_MS = 30_000;

class RuntimeProjectionWorker extends BaseProjectionWorker {
  constructor() {
    super({ pollIntervalMs: POLL_INTERVAL_MS, workerName: 'runtime-projection-worker' });
    this._retryPressureHistory = [];
  }

  get _projectType() {
    return PROJECTION_TYPE;
  }

  get _domain() {
    return 'runtime';
  }

  /**
   * Fetch raw telemetry from metrics substrate.
   * @returns {Promise<object>}
   */
  async _getSnapshotSource() {
    const healthSignals = metricsSubstrate.getHealthSignals();
    const domainBreakdown = metricsSubstrate.getDomainBreakdown();

    return {
      healthSignals,
      domainBreakdown,
      tickCount: this._tickCount,
      windowOpenedAt: Date.now() - POLL_INTERVAL_MS,
      entryCount: healthSignals.total,
      noiseGate: healthSignals.total < 5,
    };
  }

  /**
   * Synthesize runtimeState, executionPressure, retryPressure, cadenceHealth.
   * Deterministic: uses only signals passed as argument.
   *
   * @param {object} projectionState — previous cached projection
   * @param {object} signals — from _getSnapshotSource()
   * @returns {object} projectionPayload
   */
  _synthesize(projectionState, signals) {
    const { healthSignals, domainBreakdown } = signals;

    const { total, failed, failureRate } = healthSignals;

    // Runtime state derived from failure rate thresholds
    const runtimeState = this._deriveRuntimeState(failureRate, total);

    // Execution pressure: 0-1, composite of failure rate and volume
    const executionPressure = Math.min(1.0, (failureRate * 0.7) + (total > 100 ? 0.3 : total / 333));

    // Retry pressure: how many retries relative to successful completions
    const retryPressure = this._deriveRetryPressure(failed, total, domainBreakdown);

    // Cadence health: quality of cadence between polling intervals
    const cadenceHealth = this._deriveCadenceHealth(domainBreakdown, projectionState);

    return {
      runtimeState,
      executionPressure,
      retryPressure,
      cadenceHealth,
      totalSamples: total,
      failedSamples: failed,
      failureRate,
    };
  }

  _deriveRuntimeState(failureRate, total) {
    if (total === 0) return 'IDLE';
    if (failureRate >= 0.6) return 'CRITICAL';
    if (failureRate >= 0.4) return 'DEGRADED';
    if (failureRate >= 0.2) return 'ELEVATED';
    return 'HEALTHY';
  }

  _deriveRetryPressure(failed, total, domainBreakdown) {
    if (total === 0) return 0;
    // Retry pressure inferred from failed ratio + oscillation in domain breakdown
    let oscillation = 0;
    if (domainBreakdown) {
      const rates = Object.values(domainBreakdown).map(d => d.failureRate);
      const above = rates.filter(r => r >= 0.3).length;
      oscillation = rates.length > 0 ? above / rates.length : 0;
    }
    const basePressure = failed / Math.max(1, total);
    this._retryPressureHistory.push(basePressure);
    if (this._retryPressureHistory.length > 10) this._retryPressureHistory.splice(0, 1);
    return Math.min(1.0, basePressure + oscillation * 0.3);
  }

  _deriveCadenceHealth(domainBreakdown, projectionState) {
    if (!domainBreakdown) return 1.0;
    // Cadence health: how stable are domain failure rates across polling windows
    const rates = Object.values(domainBreakdown).map(d => d.failureRate);
    if (rates.length === 0) return 1.0;
    const prev = projectionState.recentFailureRates || [];
    const allRates = [...prev, ...rates];
    if (allRates.length < 4) return 1.0;

    // Standard deviation of failure rates across all observations
    const mean = allRates.reduce((s, r) => s + r, 0) / allRates.length;
    const variance = allRates.reduce((s, r) => s + Math.pow(r - mean, 2), 0) / allRates.length;
    const stdDev = Math.sqrt(variance);
    return Math.max(0, 1.0 - stdDev * 4);
  }

  _computeConfidence(signals) {
    if (signals.noiseGate) return 0.0;
    if (signals.healthSignals.total < 5) return 0.0;
    if (signals.healthSignals.total < 20) return 0.5;
    return 1.0;
  }

  _computeIntegrityScore(signals) {
    const { healthSignals } = signals;
    if (!healthSignals) return 0.0;
    // Integrity: failure rate should not be NaN or out of bounds
    const fr = healthSignals.failureRate;
    if (isNaN(fr) || fr < 0 || fr > 1) return 0.0;
    return 1.0;
  }
}

module.exports = RuntimeProjectionWorker;
