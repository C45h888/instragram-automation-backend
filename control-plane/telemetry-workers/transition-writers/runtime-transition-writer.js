// control-plane/telemetry-workers/transition-writers/runtime-transition-writer.js
// Runtime Transition Writer: bounded append for runtime namespace FSM output.
//
// Domain: runtime
// Filter: raw.entryType === 'SEMANTIC_PROJECTION_TRANSITION' AND domain === 'runtime'
// Write: lineage:ledger:entries (canonical ledger)
// Dispatch: CK.dispatch(PROJECTION_PERSISTED)
//
// See base-transition-writer.js for the contract.
// This file is auto-generated — the base factory creates identical writers per namespace.

const { createTransitionWriter } = require('./base-transition-writer');

const writer = createTransitionWriter('runtime');

module.exports = {
  start: writer.start,
  stop: writer.stop,
  getHealth: writer.getHealth,
};