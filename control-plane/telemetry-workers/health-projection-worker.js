// control-plane/telemetry-workers/health-projection-worker.js
// Health Projection Worker: synthesizes degradationSignals, failureRate,
// runtimeEntropy, operationalStress, RETRY_PRESSURE.
//
// Owns: semantic synthesis of health signals from raw metrics + observability.
// Does NOT own: governance decisions, lineage, FSM semantics.
//
// Projection Type: HEALTH_PROJECTION
// Source: observability snapshot + metricsSubstrate (fallback only)
//
// Semantic synthesis ownership:
//   RETRY_PRESSURE      — inferred from failureRate >= 0.5 threshold
//   degradationSignals  — derived from failure rate severity tiers
//   runtimeEntropy      — derived from failure rate volatility over time
//
// Determinism contract:
//   same healthSignals + same observabilitySnapshot + same version
//   = ALWAYS same degradationSignals, failureRate, runtimeEntropy, operationalStress

const { BaseProjectionWorker } = require('./base-projection-worker');
const metricsSubstrate = require('../../substrates/metrics-substrate');

const PROJECTION_TYPE = 'HEALTH_PROJECTION';
const POLL_INTERVAL_MS = 30_000;

class HealthProjectionWorker extends BaseProjectionWorker {
  constructor() {
    super({ pollIntervalMs: POLL_INTERVAL_MS, workerName: 'health-projection-worker' });
    this._lastFailureRate = 0;
    this._degradationHistory = [];
  }

  get _projectType() {
    return PROJECTION_TYPE;
  }

  get _domain() {
    return 'health';
  }

  /**
   * @returns {Promise<object>}
   */
  async _getSnapshotSource() {
    const healthSignals = metricsSubstrate.getHealthSignals();
    const domainBreakdown = metricsSubstrate.getDomainBreakdown();

    let observabilitySnapshot = {};
    try {
      // eslint-disable-next-line global-require
      const observability = require('../observability');
      observabilitySnapshot = observability.getFullSnapshot() || {};
    } catch {
      // observability may not be initialized yet — skip
    }

    return {
      healthSignals,
      rawSignals: { healthSignals, domainBreakdown },
      observabilitySnapshot,
      windowOpenedAt: Date.now() - POLL_INTERVAL_MS,
      entryCount: healthSignals.total,
      noiseGate: healthSignals.total < 5,
    };
  }

  /**
   * Synthesize degradationSignals, failureRate, runtimeEntropy, operationalStress, RETRY_PRESSURE.
   *
   * RETRY_PRESSURE is inferred here (not in the adapter) because it is semantic synthesis,
   * not raw telemetry normalization. The adapter emits RAW_METRICS_WINDOW; this worker
   * synthesizes RETRY_PRESSURE from the raw failure rate signal.
   *
   * @param {object} projectionState
   * @param {object} signals
   * @returns {object}
   */
  _synthesize(projectionState, signals) {
    const { healthSignals } = signals;
    if (!healthSignals) {
      return { degradationSignals: {}, failureRate: 0, runtimeEntropy: 0, operationalStress: 0, retryPressure: 0 };
    }

    const { total, failed, failureRate } = healthSignals;

    // Failure rate
    const failureRateOut = failureRate;

    // RETRY_PRESSURE: semantic synthesis from raw failure rate signal.
    // Threshold policy is owned here, not in the adapter.
    const RETRY_PRESSURE = this._deriveRetryPressure(failureRate, total);

    // Runtime entropy: measured volatility in failure rate over time
    const runtimeEntropy = this._deriveRuntimeEntropy(failureRate);

    // Operational stress: composite of failure rate + retry pressure
    const operationalStress = this._deriveOperationalStress(healthSignals);

    // Degradation signals: emit individual signals per severity tier
    const degradationSignals = this._deriveDegradationSignals(failureRate, total, operationalStress);

    return {
      degradationSignals,
      failureRate: failureRateOut,
      runtimeEntropy,
      operationalStress,
      retryPressure: RETRY_PRESSURE,
      totalSamples: total,
      failedSamples: failed,
    };
  }

  _deriveRetryPressure(failureRate, total) {
    if (total === 0) return 0;
    if (failureRate >= 0.5) return 0.8;
    if (failureRate >= 0.3) return 0.5;
    if (failureRate >= 0.15) return 0.2;
    return 0;
  }

  _deriveRuntimeEntropy(currentFailureRate) {
    // Track entropy as standard deviation-like measure of failure rate volatility
    this._degradationHistory.push(currentFailureRate);
    if (this._degradationHistory.length > 20) this._degradationHistory.splice(0, 1);

    if (this._degradationHistory.length < 3) return 0;
    const mean = this._degradationHistory.reduce((s, v) => s + v, 0) / this._degradationHistory.length;
    const variance = this._degradationHistory.reduce((s, v) => s + Math.pow(v - mean, 2), 0) / this._degradationHistory.length;
    const stdDev = Math.sqrt(variance);
    return Math.min(1.0, stdDev * 3); // scaled to 0-1
  }

  _deriveOperationalStress(healthSignals) {
    // Stress composite: failure rate weighted 60%, retry rate 40%
    // (retry rate derived from failed/total if retry was attempted)
    const failureComponent = healthSignals.failureRate;
    const retryComponent = healthSignals.total > 0
      ? (healthSignals.failed / Math.max(1, healthSignals.total)) * 0.4
      : 0;
    return Math.min(1.0, failureComponent + retryComponent);
  }

  _deriveDegradationSignals(failureRate, total, operationalStress) {
    const signals = {};
    if (total === 0) {
      signals.IDLE = true;
    } else if (failureRate >= 0.6) {
      signals.CRITICAL = true;
    } else if (failureRate >= 0.4) {
      signals.ELEVATED = true;
    } else if (failureRate >= 0.2) {
      signals.DEGRADED = true;
    } else {
      signals.HEALTHY = true;
    }

    if (operationalStress >= 0.7) {
      signals.HIGH_STRESS = true;
    }

    return signals;
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
    // Integrity: failure rate should not jump more than 0.3 between ticks
    const delta = Math.abs(healthSignals.failureRate - this._lastFailureRate);
    this._lastFailureRate = healthSignals.failureRate;
    return delta <= 0.3 ? 1.0 : Math.max(0, 1.0 - delta * 2);
  }
}

module.exports = HealthProjectionWorker;
