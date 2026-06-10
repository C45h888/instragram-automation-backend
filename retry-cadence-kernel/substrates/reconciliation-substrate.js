// retry-cadence-kernel/substrates/reconciliation-substrate.js
// Reconciliation Substrate — bounded state reconciliation and conflict resolution.
//
// CONSTITUTIONAL CONTRACT:
//   Owns: state comparison logic, divergence detection, conflict resolution
//         strategy selection, dispatching to the appropriate worker.
//
//   Does NOT own: classification (persistence-failure-substrate),
//                 recommendation selection (FSM),
//                 reconciliation cycle execution (reconciliation-kernel).
//
// Workers beneath:
//   state-reconciliation-worker — full state comparison and divergence detection
//   conflict-resolution-worker  — unique constraint / FK violation resolution
//
// Flow:
//   FSM → RECONCILE_STATE_AUTHORIZED → reconciliation-substrate.execute()
//     → substrate selects worker based on analysis.subtype:
//       - unique_constraint_violation → conflict-resolution-worker
//       - foreign_key_violation       → conflict-resolution-worker
//       - other                       → state-reconciliation-worker
//     → worker executes
//     → emits RECONCILIATION_COMPLETE or RECONCILIATION_FAILED

const stateReconciliationWorker = require('../workers/state-reconciliation-worker');
const conflictResolutionWorker = require('../workers/conflict-resolution-worker');

// Subtypes that route to the conflict-resolution worker
const CONFLICT_SUBTYPES = new Set([
  'unique_constraint_violation',
  'foreign_key_violation',
  'http_409',
  'transaction_rollback',
]);

async function execute(event, governance) {
  const startTime = Date.now();
  const { domain, accountId, intentId, table, analysis } = event;

  if (!governance || (typeof governance.dispatchGlobal !== 'function' && typeof governance.dispatch !== 'function')) {
    return { success: false, error: 'governance required', durationMs: Date.now() - startTime };
  }

  const subtype = analysis?.subtype || 'unknown';
  const isConflict = CONFLICT_SUBTYPES.has(subtype);

  const result = isConflict
    ? await conflictResolutionWorker.execute({
        domain, accountId, intentId, table, analysis,
      }, governance)
    : await stateReconciliationWorker.execute({
        domain, accountId, intentId, table, analysis,
      }, governance);

  const durationMs = Date.now() - startTime;

  (governance?.dispatchGlobal || governance?.dispatch)({
    type: result.success ? 'RECONCILIATION_COMPLETE' : 'RECONCILIATION_FAILED',
    domain: domain || 'persist-telemetry',
    accountId: accountId || '*',
    intentId,
    table,
    route: isConflict ? 'conflict_resolution' : 'state_reconciliation',
    workerName: isConflict ? 'conflict-resolution-worker' : 'state-reconciliation-worker',
    success: result.success,
    error: result.error,
    resolution: result.resolution || null,
    durationMs,
  });

  // If reconciliation succeeded and the original failure was a conflict,
  // the write effectively landed. Re-injecting would be wrong — mark resolved.
  if (result.success && isConflict && result.resolution === 'already_persisted') {
    (governance?.dispatchGlobal || governance?.dispatch)({
      type: 'DB_WRITE_COMPLETE',
      domain: domain || 'persist-telemetry',
      accountId: accountId || '*',
      intentId,
      table,
      count: 1,
      error: null,
      status: 'resolved_through_reconciliation',
    });
  }

  return { ...result, durationMs };
}

module.exports = { execute };
