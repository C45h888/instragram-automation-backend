// control-plane/telemetry-workers/transition-writers/integrity-transition-writer.js
// Integrity Transition Writer: bounded append for integrity namespace FSM output.
//
// Domain: integrity
// Filter: raw.entryType === 'SEMANTIC_PROJECTION_TRANSITION' AND domain === 'integrity'
// Write: lineage:ledger:entries (canonical ledger)
// Dispatch: CK.dispatch(PROJECTION_PERSISTED)

const { createTransitionWriter } = require('./base-transition-writer');

const writer = createTransitionWriter('integrity');

module.exports = {
  start: writer.start,
  stop: writer.stop,
  getHealth: writer.getHealth,
};