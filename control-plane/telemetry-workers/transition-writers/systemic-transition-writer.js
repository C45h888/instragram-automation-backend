// control-plane/telemetry-workers/transition-writers/systemic-transition-writer.js
// Systemic Transition Writer: bounded append for systemic namespace FSM output.
//
// Domain: systemic
// Filter: coordinatedBy === 'telemetry-coordination-fsm' AND domain === 'systemic'
// Write: lineage:ledger:entries (canonical ledger)
// Dispatch: CK.dispatch(PROJECTION_PERSISTED)

const { createTransitionWriter } = require('./base-transition-writer');

const writer = createTransitionWriter('systemic');

module.exports = {
  start: writer.start,
  stop: writer.stop,
  getHealth: writer.getHealth,
};