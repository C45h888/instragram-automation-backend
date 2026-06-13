const { BaseProjectionWorker } = require('./base-projection-worker');
const dedupInput = require('../inputs/dedup-input');
const dedupSynthesis = require('../synthesis/dedup-projection');
const PROJECTION_TYPE = 'DEDUP_PROJECTION';
const POLL_INTERVAL_MS = 30_000;
class DedupProjectionWorker extends BaseProjectionWorker {
  constructor() { super({ pollIntervalMs: POLL_INTERVAL_MS, workerName: 'dedup-projection-worker' }); }
  get _projectType() { return PROJECTION_TYPE; }
  get _domain() { return 'dedup'; }
  async _getNormalizedInputWindow() { return dedupInput.getNormalizedInputWindow({ pollIntervalMs: POLL_INTERVAL_MS, tickCount: this._tickCount }); }
  _runSynthesis(state, signals) { return dedupSynthesis.synthesize(state, signals); }
  _computeConfidence(signals) { return dedupSynthesis.computeConfidence(signals); }
  _computeIntegrityScore(signals) { return dedupSynthesis.computeIntegrityScore(signals); }
}
module.exports = DedupProjectionWorker;
