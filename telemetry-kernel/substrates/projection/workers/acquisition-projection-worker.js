const { BaseProjectionWorker } = require('./base-projection-worker');
const acquisitionInput = require('../inputs/acquisition-input');
const acquisitionSynthesis = require('../synthesis/acquisition-projection');
const PROJECTION_TYPE = 'ACQUISITION_PROJECTION';
const POLL_INTERVAL_MS = 30_000;
class AcquisitionProjectionWorker extends BaseProjectionWorker {
  constructor() { super({ pollIntervalMs: POLL_INTERVAL_MS, workerName: 'acquisition-projection-worker' }); }
  get _projectType() { return PROJECTION_TYPE; }
  get _domain() { return 'acquisition'; }
  async _getNormalizedInputWindow() { return acquisitionInput.getNormalizedInputWindow({ pollIntervalMs: POLL_INTERVAL_MS, tickCount: this._tickCount }); }
  _runSynthesis(state, signals) { return acquisitionSynthesis.synthesize(state, signals); }
  _computeConfidence(signals) { return acquisitionSynthesis.computeConfidence(signals); }
  _computeIntegrityScore(signals) { return acquisitionSynthesis.computeIntegrityScore(signals); }
}
module.exports = AcquisitionProjectionWorker;
