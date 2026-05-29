const { BaseProjectionWorker } = require('./base-projection-worker');
const authorityInput = require('../projection-layers/inputs/authority-input');
const authoritySynthesis = require('../projection-layers/synthesis/authority-projection');

const PROJECTION_TYPE = 'AUTHORITY_PROJECTION';
const POLL_INTERVAL_MS = 30000;

class AuthorityProjectionWorker extends BaseProjectionWorker {
  constructor() {
    super({ pollIntervalMs: POLL_INTERVAL_MS, workerName: 'authority-projection-worker' });
  }

  get _projectType() {
    return PROJECTION_TYPE;
  }

  get _domain() {
    return 'authority';
  }

  async _getNormalizedInputWindow() {
    return authorityInput.getNormalizedInputWindow({ pollIntervalMs: POLL_INTERVAL_MS, tickCount: this._tickCount });
  }

  _runSynthesis(projectionState, normalizedWindow) {
    return authoritySynthesis.synthesize(projectionState, normalizedWindow);
  }

  _computeConfidence(signals) {
    return authoritySynthesis.computeConfidence(signals);
  }

  _computeIntegrityScore(signals) {
    return authoritySynthesis.computeIntegrityScore(signals);
  }
}

module.exports = AuthorityProjectionWorker;
