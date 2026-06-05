// telemetry-kernel/index.js
// Kernel root façade. Wires FSM to constitutional kernel, exposes public surface.
//
// External consumers import telemetry substrate from here.
// Orchestrator wires FSM to CK via registerDomain(telemetryKernel.fsm).
//
// Pattern:
//   const telemetryKernel = require('./telemetry-kernel');
//   constitutional.registerDomain(telemetryKernel.fsm);
//   const telemetrySubstrate = telemetryKernel.substrate;

const fsm = require('./fsm');
const substrate = require('./substrates/projection');
const ingressLagWorker = require('./substrates/ingress-lag-worker');

// Canonical lifecycle — alias old call sites (startAll/stopAll) to substrate lifecycle.
// Old call shape: telemetryWorkers.startAll() / telemetryWorkers.stopAll()
// New canonical shape: substrate.startProjections() / substrate.stopProjections()
const startAll = (pollIntervalMs) => substrate.startProjections(pollIntervalMs);
const stopAll = () => substrate.stopProjections();

module.exports = {
  fsm, // telemetry coordination FSM (canonical — was control-plane/governance/domains/telemetry-coordination-fsm.js)
  substrate, // projection substrate (canonical — was control-plane/telemetry-workers + control-plane/projection-layers)
  ingressLagWorker, // ingress lag observation worker (stays in control-plane for now, consumed by substrate)
  // Lifecycle — old call shape preserved, delegates to canonical substrate
  startAll,
  stopAll,
  // Canonical start/stop for kernel-internal callers
  startProjections: substrate.startProjections,
  stopProjections: substrate.stopProjections,
  getProjectionHealth: substrate.getProjectionHealth,
  getProjections: substrate.getProjections,
};
