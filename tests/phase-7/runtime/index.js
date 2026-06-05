/**
 * Phase 7 runtime validation framework — public surface.
 * Imports and re-exports the framework pieces.
 */

const { Phase7RuntimeSimulator } = require('./runtime-simulator.js');
const { GraphSimulator, FAILURE_MODES } = require('./graph-simulator.js');
const { EventRecorder } = require('./event-recorder.js');
const { StateInspector } = require('./state-inspector.js');
const { MutationTracker } = require('./mutation-tracker.js');
const { WorkerTracer } = require('./worker-tracer.js');
const { GovernanceObserver } = require('./governance-observer.js');
const { CadenceAccelerator, DEFAULT_TIERS } = require('./cadence-accelerator.js');

module.exports = {
  Phase7RuntimeSimulator,
  GraphSimulator,
  FAILURE_MODES,
  EventRecorder,
  StateInspector,
  MutationTracker,
  WorkerTracer,
  GovernanceObserver,
  CadenceAccelerator,
  DEFAULT_TIERS,
};
