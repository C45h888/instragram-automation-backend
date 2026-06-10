// retry-cadence-kernel/substrates/ig-recovery-substrate.js
// IG Recovery Substrate — façade that dispatches IG-specific
// recommendations from the IG reliability substrate to the
// appropriate operationally bounded worker.
//
// CONSTITUTIONAL CONTRACT (Phase 4):
//   Owns: routing the canonical IG analysis (from
//          ig-reliability-substrate.analyzeFailure) to the
//          operationally bounded worker that executes the
//          recovery action. The substrate is the dispatcher;
//          the workers are the executors.
//
//   Does NOT own:
//     - Interpretation (ig-reliability-substrate)
//     - Authorization (engagement-fsm IG_FAILURE_OBSERVED handler)
//     - State mutation (engagement-fsm _decidedIgFailures Map)
//     - Persistence (postgres-telemetry-kernel writers)
//
// USAGE:
//   The engagement-fsm's {REC}_AUTHORIZED transitions call
//   substrate.execute({ ...event, type: '{REC}_AUTHORIZED' }, governance).
//   The substrate dispatches to the worker keyed on the
//   recommendation type. Mirrors the persistence path's
//   retry-execution-substrate.execute() shape.
//
// WORKER REGISTRY:
//   The substrate holds a Map of recommendation type → worker
//   path. Workers are registered via registerWorker(recommendation,
//   workerPath). The orchastrator wires the workers at boot.

const path = require('path');

const _workerRegistry = new Map();

function registerWorker(recommendation, workerPath) {
  _workerRegistry.set(recommendation, workerPath);
}

function _resolveWorkerPath(recommendation) {
  return _workerRegistry.get(recommendation) || null;
}

/**
 * Execute the recovery action for an IG-specific recommendation.
 *
 * @param {object} event — { type: 'REFRESH_TOKEN_AUTHORIZED', analysis, ... }
 * @param {object} governance — CK reference
 * @returns {Promise<{ success, workerName, durationMs, error }>}
 */
async function execute(event, governance) {
  const startTime = Date.now();
  const type = event?.type || '';
  // Map {REC}_AUTHORIZED → {REC}
  const recommendation = type.replace(/_AUTHORIZED$/, '');

  // For REQUEUE_OPERATION, the worker is keyed on the IG domain
  // (publish:post, publish:story, publish:comment, publish:message)
  // so each surface has its own retry executor. Phase 4.
  let workerKey = recommendation;
  if (recommendation === 'REQUEUE_OPERATION') {
    const domain = event?.domain || event?.params?.domain || null;
    if (domain && domain.startsWith('publish:')) {
      workerKey = `REQUEUE_OPERATION_${domain.replace(/:/g, '_').toUpperCase()}`;
    }
  }

  const workerPath = _resolveWorkerPath(workerKey);
  if (!workerPath) {
    return {
      success: false,
      workerName: null,
      durationMs: Date.now() - startTime,
      error: `No IG recovery worker registered for recommendation: ${recommendation}`,
    };
  }

  let worker;
  try {
    worker = require(workerPath);
  } catch (err) {
    return {
      success: false,
      workerName: workerPath,
      durationMs: Date.now() - startTime,
      error: `Failed to load IG recovery worker: ${err.message}`,
    };
  }

  if (!worker || typeof worker.execute !== 'function') {
    return {
      success: false,
      workerName: workerPath,
      durationMs: Date.now() - startTime,
      error: `IG recovery worker does not export execute(): ${workerPath}`,
    };
  }

  try {
    const result = await worker.execute(event, governance);
    return {
      success: result?.success !== false,
      workerName: result?.workerName || path.basename(workerPath, '.js'),
      durationMs: result?.durationMs ?? (Date.now() - startTime),
      error: result?.error || null,
      data: result?.data || null,
    };
  } catch (err) {
    return {
      success: false,
      workerName: path.basename(workerPath, '.js'),
      durationMs: Date.now() - startTime,
      error: err.message || String(err),
    };
  }
}

module.exports = {
  execute,
  registerWorker,
  _resolveWorkerPath,
  _workerRegistry,
};
