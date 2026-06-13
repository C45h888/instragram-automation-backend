// telemetry-kernel/substrates/projection/workers/scheduling-projection-worker.js
const { BaseProjectionWorker } = require('./base-projection-worker');
const schedulingInput = require('../inputs/scheduling-input');
const schedulingSynthesis = require('../synthesis/scheduling-projection');

const PROJECTION_TYPE = 'SCHEDULING_PROJECTION';
const POLL_INTERVAL_MS = 30_000;

class SchedulingProjectionWorker extends BaseProjectionWorker {
  constructor() { super({ pollIntervalMs: POLL_INTERVAL_MS, workerName: 'scheduling-projection-worker' }); }
  get _projectType() { return PROJECTION_TYPE; }
  get _domain() { return 'scheduling'; }
  async _getNormalizedInputWindow() { return schedulingInput.getNormalizedInputWindow({ pollIntervalMs: POLL_INTERVAL_MS, tickCount: this._tickCount }); }
  _runSynthesis(state, signals) { return schedulingSynthesis.synthesize(state, signals); }
  _computeConfidence(signals) { return schedulingSynthesis.computeConfidence(signals); }
  _computeIntegrityScore(signals) { return schedulingSynthesis.computeIntegrityScore(signals); }
}

module.exports = SchedulingProjectionWorker;
