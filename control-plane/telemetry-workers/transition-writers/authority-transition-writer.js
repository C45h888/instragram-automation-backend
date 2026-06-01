// control-plane/telemetry-workers/transition-writers/authority-transition-writer.js
// Authority Transition Writer: bounded append for authority namespace FSM output.
//
// Domain: authority
// Filter: coordinatedBy === 'telemetry-coordination-fsm' AND domain === 'authority'
// Write: lineage:ledger:entries (canonical ledger)
// Dispatch: CK.dispatch(PROJECTION_PERSISTED)

const { createTransitionWriter } = require('./base-transition-writer');

const writer = createTransitionWriter('authority');

module.exports = {
  start: writer.start,
  stop: writer.stop,
  getHealth: writer.getHealth,
};