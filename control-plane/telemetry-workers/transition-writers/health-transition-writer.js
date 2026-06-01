// control-plane/telemetry-workers/transition-writers/health-transition-writer.js
// Health Transition Writer: bounded append for health namespace FSM output.
//
// Domain: health
// Filter: coordinatedBy === 'telemetry-coordination-fsm' AND domain === 'health'
// Write: lineage:ledger:entries (canonical ledger)
// Dispatch: CK.dispatch(PROJECTION_PERSISTED)

const { createTransitionWriter } = require('./base-transition-writer');

const writer = createTransitionWriter('health');

module.exports = {
  start: writer.start,
  stop: writer.stop,
  getHealth: writer.getHealth,
};