const { BaseProjectionWorker } = require('./base-projection-worker');
const healthInput = require('../projection-layers/inputs/health-input');
const healthSynthesis = require('../projection-layers/synthesis/health-projection');

const PROJECTION_TYPE = 'HEALTH_PROJECTION';
const POLL_INTERVAL_MS = 30000;

class HealthProjectionWorker extends BaseProjectionWorker {
  constructor() {
    super({ pollIntervalMs: POLL_INTERVAL_MS, workerName: 'health-projection-worker' });
    this._failureHistory = [];
    this._lastFailureRate = 0;
  }

  get _projectType() {
    return PROJECTION_TYPE;
  }

  get _domain() {
    return 'health';
  }

  async _getNormalizedInputWindow() {
    return healthInput.getNormalizedInputWindow({
      pollIntervalMs: POLL_INTERVAL_MS,
      tickCount: this._tickCount,
      failureHistory: this._failureHistory,
      lastFailureRate: this._lastFailureRate,
    });
  }

  _runSynthesis(projectionState, normalizedWindow) {
    const payload = healthSynthesis.synthesize(projectionState, normalizedWindow);
    if (normalizedWindow.healthSignals) {
      this._failureHistory.push(normalizedWindow.healthSignals.failureRate);
      if (this._failureHistory.length > 20) this._failureHistory.splice(0, this._failureHistory.length - 20);
      this._lastFailureRate = normalizedWindow.healthSignals.failureRate;
    }
    return payload;
  }

  _computeConfidence(signals) {
    return healthSynthesis.computeConfidence(signals);
  }

  _computeIntegrityScore(signals) {
    return healthSynthesis.computeIntegrityScore(signals);
  }
}

module.exports = HealthProjectionWorker;
