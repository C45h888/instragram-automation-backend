// telemetry-kernel/substrates/projection/workers/persist-telemetry-projection-worker.js
// Persist-Telemetry Projection Worker: 7th bounded projection worker.
// Mirrors the contract of the 6 existing projection workers.

const { BaseProjectionWorker } = require('./base-projection-worker');
const persistTelemetryInput = require('../inputs/persist-telemetry-input');
const persistTelemetrySynthesis = require('../synthesis/persist-telemetry-projection');

const PROJECTION_TYPE = 'PERSIST_TELEMETRY_PROJECTION';
const POLL_INTERVAL_MS = 30_000;

class PersistTelemetryProjectionWorker extends BaseProjectionWorker {
  constructor() {
    super({ pollIntervalMs: POLL_INTERVAL_MS, workerName: 'persist-telemetry-projection-worker' });
  }

  get _projectType() {
    return PROJECTION_TYPE;
  }

  get _domain() {
    return 'persist-telemetry';
  }

  async _getNormalizedInputWindow() {
    return persistTelemetryInput.getNormalizedInputWindow({
      pollIntervalMs: POLL_INTERVAL_MS,
      tickCount: this._tickCount,
    });
  }

  _runSynthesis(projectionState, normalizedWindow) {
    return persistTelemetrySynthesis.synthesize(projectionState, normalizedWindow);
  }

  _computeConfidence(signals) {
    return persistTelemetrySynthesis.computeConfidence(signals);
  }

  _computeIntegrityScore(signals) {
    return persistTelemetrySynthesis.computeIntegrityScore(signals);
  }
}

module.exports = PersistTelemetryProjectionWorker;
