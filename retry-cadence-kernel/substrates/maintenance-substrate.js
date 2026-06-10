// retry-cadence-kernel/substrates/maintenance-substrate.js
// Maintenance Substrate — bounded schema repair and cache maintenance.
//
// CONSTITUTIONAL CONTRACT:
//   Owns: schema validation, migration integrity checks, cache invalidation,
//         cache rebuild triggering, structural recovery routing.
//
//   Does NOT own: classification (persistence-failure-substrate),
//                 recommendation selection (FSM),
//                 actual schema migration (schema-recovery-worker),
//                 actual cache operations (cache-repair-worker).
//
// Workers beneath:
//   schema-recovery-worker — validates schema integrity, runs migration checks
//   cache-repair-worker    — invalidates and rebuilds stale/corrupt caches
//
// This substrate handles TWO recommendation types:
//   REPAIR_SCHEMA → schema-recovery-worker
//   REBUILD_CACHE → cache-repair-worker
//
// Flow:
//   FSM → REPAIR_SCHEMA_AUTHORIZED  → maintenance-substrate.execute() → schema worker
//   FSM → REBUILD_CACHE_AUTHORIZED  → maintenance-substrate.execute() → cache worker

const schemaRecoveryWorker = require('../workers/schema-recovery-worker');
const cacheRepairWorker = require('../workers/cache-repair-worker');

async function execute(event, governance) {
  const startTime = Date.now();
  const { type, domain, accountId, intentId, analysis } = event;

  if (!governance || (typeof governance.dispatchGlobal !== 'function' && typeof governance.dispatch !== 'function')) {
    return { success: false, error: 'governance required', durationMs: Date.now() - startTime };
  }

  // Route based on the action type — which recommendation triggered this
  const isSchemaRepair = type === 'REPAIR_SCHEMA_AUTHORIZED';
  const worker = isSchemaRepair ? schemaRecoveryWorker : cacheRepairWorker;
  const workerName = isSchemaRepair ? 'schema-recovery-worker' : 'cache-repair-worker';

  const result = await worker.execute({
    domain, accountId, intentId, analysis,
  }, governance);

  const durationMs = Date.now() - startTime;

  if (isSchemaRepair) {
    (governance?.dispatchGlobal || governance?.dispatch)({
      type: result.success ? 'SCHEMA_VALIDATED' : 'SCHEMA_MISMATCH',
      domain: domain || 'persist-telemetry',
      accountId: accountId || '*',
      intentId,
      workerName,
      success: result.success,
      error: result.error,
      durationMs,
    });
  } else {
    (governance?.dispatchGlobal || governance?.dispatch)({
      type: result.success ? 'CACHE_REPAIRED' : 'CACHE_REPAIR_FAILED',
      domain: domain || 'persist-telemetry',
      accountId: accountId || '*',
      intentId,
      workerName,
      success: result.success,
      error: result.error,
      durationMs,
    });
  }

  return { ...result, durationMs };
}

module.exports = { execute };
