// telemetry-kernel/substrates/projection/workers/capability-projection-worker.js
// Capability Projection Worker: 6th bounded projection worker.
// Mirrors the contract of the 5 existing projection workers.

const { BaseProjectionWorker } = require('./base-projection-worker');
const capabilityInput = require('../inputs/capability-input');
const capabilitySynthesis = require('../synthesis/capability-projection');

const PROJECTION_TYPE = 'CAPABILITY_PROJECTION';
const POLL_INTERVAL_MS = 30_000;

class CapabilityProjectionWorker extends BaseProjectionWorker {
  constructor() {
    super({ pollIntervalMs: POLL_INTERVAL_MS, workerName: 'capability-projection-worker' });
  }

  get _projectType() {
    return PROJECTION_TYPE;
  }

  get _domain() {
    return 'capability';
  }

  async _getNormalizedInputWindow() {
    return capabilityInput.getNormalizedInputWindow({
      pollIntervalMs: POLL_INTERVAL_MS,
      tickCount: this._tickCount,
    });
  }

  _runSynthesis(projectionState, normalizedWindow) {
    return capabilitySynthesis.synthesize(projectionState, normalizedWindow);
  }

  _computeConfidence(signals) {
    return capabilitySynthesis.computeConfidence(signals);
  }

  _computeIntegrityScore(signals) {
    return capabilitySynthesis.computeIntegrityScore(signals);
  }
}

module.exports = CapabilityProjectionWorker;
