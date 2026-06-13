const { BaseProjectionWorker } = require('./base-projection-worker');
const publishingInput = require('../inputs/publishing-input');
const publishingSynthesis = require('../synthesis/publishing-projection');
const PROJECTION_TYPE = 'PUBLISHING_PROJECTION';
const POLL_INTERVAL_MS = 30_000;
class PublishingProjectionWorker extends BaseProjectionWorker {
  constructor() { super({ pollIntervalMs: POLL_INTERVAL_MS, workerName: 'publishing-projection-worker' }); }
  get _projectType() { return PROJECTION_TYPE; }
  get _domain() { return 'publishing'; }
  async _getNormalizedInputWindow() { return publishingInput.getNormalizedInputWindow({ pollIntervalMs: POLL_INTERVAL_MS, tickCount: this._tickCount }); }
  _runSynthesis(state, signals) { return publishingSynthesis.synthesize(state, signals); }
  _computeConfidence(signals) { return publishingSynthesis.computeConfidence(signals); }
  _computeIntegrityScore(signals) { return publishingSynthesis.computeIntegrityScore(signals); }
}
module.exports = PublishingProjectionWorker;
