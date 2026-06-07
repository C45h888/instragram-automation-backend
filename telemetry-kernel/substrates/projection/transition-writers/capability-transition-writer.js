// telemetry-kernel/substrates/projection/transition-writers/capability-transition-writer.js
// Capability Transition Writer: bounded append for the capability namespace.
// Mirrors the contract of the 5 existing transition writers (4-line file).
//
// Domain: capability
// Filter: raw.entryType === 'SEMANTIC_PROJECTION_TRANSITION' AND domain === 'capability'
// Write: lineage:ledger:entries (canonical ledger)
// Dispatch: CK.dispatch(PROJECTION_PERSISTED)

const { createTransitionWriter } = require('./base-transition-writer');

const writer = createTransitionWriter('capability');

module.exports = {
  start: writer.start,
  stop: writer.stop,
  getHealth: writer.getHealth,
  awaitPendingWrite: writer.awaitPendingWrite,
};
