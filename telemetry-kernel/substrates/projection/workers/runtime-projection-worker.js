const { BaseProjectionWorker } = require('./base-projection-worker');
const runtimeInput = require('../inputs/runtime-input');
const runtimeSynthesis = require('../synthesis/runtime-projection');

const PROJECTION_TYPE = 'RUNTIME_PROJECTION';
const POLL_INTERVAL_MS = 30000;

class RuntimeProjectionWorker extends BaseProjectionWorker {
  constructor() {
    super({ pollIntervalMs: POLL_INTERVAL_MS, workerName: 'runtime-projection-worker' });
  }

  get _projectType() {
    return PROJECTION_TYPE;
  }

  get _domain() {
    return 'runtime';
  }

  async _getNormalizedInputWindow() {
    return runtimeInput.getNormalizedInputWindow({ pollIntervalMs: POLL_INTERVAL_MS, tickCount: this._tickCount });
  }

  _runSynthesis(projectionState, normalizedWindow) {
    return runtimeSynthesis.synthesize(projectionState, normalizedWindow);
  }

  _computeConfidence(signals) {
    return runtimeSynthesis.computeConfidence(signals);
  }

  _computeIntegrityScore(signals) {
    return runtimeSynthesis.computeIntegrityScore(signals);
  }
}

module.exports = RuntimeProjectionWorker;
