const { BaseProjectionWorker } = require('./base-projection-worker');
const integrityInput = require('../projection-layers/inputs/integrity-input');
const integritySynthesis = require('../projection-layers/synthesis/integrity-projection');

const PROJECTION_TYPE = 'INTEGRITY_PROJECTION';
const POLL_INTERVAL_MS = 30000;

class IntegrityProjectionWorker extends BaseProjectionWorker {
  constructor() {
    super({ pollIntervalMs: POLL_INTERVAL_MS, workerName: 'integrity-projection-worker' });
  }

  get _projectType() {
    return PROJECTION_TYPE;
  }

  get _domain() {
    return 'integrity';
  }

  async _getNormalizedInputWindow() {
    return integrityInput.getNormalizedInputWindow({ pollIntervalMs: POLL_INTERVAL_MS, tickCount: this._tickCount });
  }

  _runSynthesis(projectionState, normalizedWindow) {
    return integritySynthesis.synthesize(projectionState, normalizedWindow);
  }

  _computeConfidence(signals) {
    return integritySynthesis.computeConfidence(signals);
  }

  _computeIntegrityScore(signals) {
    return integritySynthesis.computeIntegrityScore(signals);
  }
}

module.exports = IntegrityProjectionWorker;
