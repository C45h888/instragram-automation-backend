// telemetry-kernel/substrates/projection/transition-writers/reconciliation-transition-writer.js
// Reconciliation Transition Writer: bounded append for the reconciliation namespace.

const { createTransitionWriter } = require('./base-transition-writer');

const writer = createTransitionWriter('reconciliation');

module.exports = {
  start: writer.start,
  stop: writer.stop,
  getHealth: writer.getHealth,
  awaitPendingWrite: writer.awaitPendingWrite,
};
