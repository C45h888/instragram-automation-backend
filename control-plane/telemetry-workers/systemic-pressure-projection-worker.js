const { BaseProjectionWorker } = require('./base-projection-worker');
const systemicInput = require('../projection-layers/inputs/systemic-input');
const systemicSynthesis = require('../projection-layers/synthesis/systemic-pressure-projection');

const PROJECTION_TYPE = 'SYSTEMIC_PRESSURE_PROJECTION';
const POLL_INTERVAL_MS = 30000;

class SystemicPressureProjectionWorker extends BaseProjectionWorker {
  constructor() {
    super({ pollIntervalMs: POLL_INTERVAL_MS, workerName: 'systemic-pressure-projection-worker' });
    this._previousConvergenceConfidence = 1.0;
  }

  get _projectType() {
    return PROJECTION_TYPE;
  }

  get _domain() {
    return 'systemic';
  }

  async _getNormalizedInputWindow() {
    return systemicInput.getNormalizedInputWindow({
      pollIntervalMs: POLL_INTERVAL_MS,
      tickCount: this._tickCount,
      previousConvergenceConfidence: this._previousConvergenceConfidence,
    });
  }

  _runSynthesis(projectionState, normalizedWindow) {
    const payload = systemicSynthesis.synthesize(projectionState, normalizedWindow);
    this._previousConvergenceConfidence = payload.convergenceConfidence;
    return payload;
  }

  _computeConfidence(signals) {
    return systemicSynthesis.computeConfidence(signals);
  }

  _computeIntegrityScore(signals) {
    return systemicSynthesis.computeIntegrityScore(signals);
  }
}

module.exports = SystemicPressureProjectionWorker;
