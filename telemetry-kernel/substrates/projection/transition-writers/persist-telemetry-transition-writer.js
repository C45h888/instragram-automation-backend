// telemetry-kernel/substrates/projection/transition-writers/persist-telemetry-transition-writer.js
// Persist-Telemetry Transition Writer: bounded append for the persist-telemetry namespace.
// Mirrors the contract of the 6 existing transition writers (4-line file).

const { createTransitionWriter } = require('./base-transition-writer');

const writer = createTransitionWriter('persist-telemetry');

module.exports = {
  start: writer.start,
  stop: writer.stop,
  getHealth: writer.getHealth,
  awaitPendingWrite: writer.awaitPendingWrite,
};
