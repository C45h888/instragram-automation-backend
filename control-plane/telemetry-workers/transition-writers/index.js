// control-plane/telemetry-workers/transition-writers/index.js
// Transition Writers: unified export for 5 bounded domain writers.
//
// Each writer is a dumb mechanical append pipe — it receives FSM-coordinated
// SEMANTIC_PROJECTION_TRANSITION entries (coordinatedBy === 'telemetry-coordination-fsm')
// and writes them to the canonical ledger. It does not interpret, serialize, or
// write to namespace projection state.
//
// Architecture:
//   Phase 1: projection workers emit PROJECTION_INTENT to observability
//            → written to domain-bounded partition: lineage:transitionLog:domain:{namespace}
//   Phase 2: FSM reactive coordination → _emitTransition() → SEMANTIC_PROJECTION_TRANSITION
//            → written to global + domain-bounded transition log
//            → onWrite fires → 5 transition-writers each receive the event
//              → filter: coordinatedBy === 'telemetry-coordination-fsm'? YES
//              → filter: domain === <this-writer's-namespace>? MATCH
//                → recordWorkerEntry() → lineage:ledger:entries
//                → CK.dispatch(PROJECTION_PERSISTED)
//   Phase 3: CK async validates → PROJECTION_ACCEPTED → namespace-projection-interpreter

const runtimeWriter = require('./runtime-transition-writer');
const integrityWriter = require('./integrity-transition-writer');
const authorityWriter = require('./authority-transition-writer');
const healthWriter = require('./health-transition-writer');
const systemicWriter = require('./systemic-transition-writer');

const writers = {
  runtime: runtimeWriter,
  integrity: integrityWriter,
  authority: authorityWriter,
  health: healthWriter,
  systemic: systemicWriter,
};

/**
 * Start all 5 transition writers.
 * Writers subscribe to observability.onWrite() and filter for FSM-coordinated output.
 * Must be called AFTER observability.init() and BEFORE constitutional.startLoop().
 */
function startAll() {
  for (const writer of Object.values(writers)) {
    writer.start();
  }
  console.log('[transition-writers] All 5 writers started — event-driven, bounded by namespace');
}

/**
 * Stop all 5 transition writers gracefully.
 * Unsubscribes from observability.onWrite() hook.
 */
function stopAll() {
  for (const writer of Object.values(writers)) {
    writer.stop();
  }
  console.log('[transition-writers] All 5 writers stopped');
}

/**
 * Return health signals for all writers.
 * @returns {object} per-namespace health status
 */
function getAllHealth() {
  const result = {};
  for (const [name, writer] of Object.entries(writers)) {
    result[name] = writer.getHealth();
  }
  return result;
}

module.exports = {
  writers,
  startAll,
  stopAll,
  getAllHealth,
};