// telemetry-kernel/substrates/projection/workers/reconciliation-projection-worker.js
// Reconciliation Projection Worker: 8th bounded projection worker.
// Mirrors the contract of the 7 existing projection workers.

const { BaseProjectionWorker } = require('./base-projection-worker');
const reconciliationInput = require('../inputs/reconciliation-input');
const reconciliationSynthesis = require('../synthesis/reconciliation-projection');

const PROJECTION_TYPE = 'RECONCILIATION_PROJECTION';
const POLL_INTERVAL_MS = 30_000;

class ReconciliationProjectionWorker extends BaseProjectionWorker {
  constructor() {
    super({ pollIntervalMs: POLL_INTERVAL_MS, workerName: 'reconciliation-projection-worker' });
  }

  get _projectType() {
    return PROJECTION_TYPE;
  }

  get _domain() {
    return 'reconciliation';
  }

  async _getNormalizedInputWindow() {
    return reconciliationInput.getNormalizedInputWindow({
      pollIntervalMs: POLL_INTERVAL_MS,
      tickCount: this._tickCount,
    });
  }

  _runSynthesis(projectionState, normalizedWindow) {
    return reconciliationSynthesis.synthesize(projectionState, normalizedWindow);
  }

  _computeConfidence(signals) {
    return reconciliationSynthesis.computeConfidence(signals);
  }

  _computeIntegrityScore(signals) {
    return reconciliationSynthesis.computeIntegrityScore(signals);
  }
}

module.exports = ReconciliationProjectionWorker;
