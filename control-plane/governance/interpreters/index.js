// control-plane/governance/interpreters/index.js
// Interpreters: unified export for bounded telemetry consumers.
//
// Architecture:
//   - FSM interpreters: bounded by domain namespace (FSM only sees its jurisdiction)
//   - HSM interpreters: full observability pass-through (HSM sees everything)
//   - Recon interpreters: full observability pass-through (Recon sees everything)
//   - Central ledger: unchanged (single source of truth)
//
// View filtering at read time, not write time:
//   - Single lineage:ledger:entries (no data fragmentation)
//   - Interpreters apply namespace filtering based on consumer type

const fsmLineageInterpreter = require('./fsm-lineage-interpreter');
const hsmLineageInterpreter = require('./hsm-lineage-interpreter');
const reconLineageInterpreter = require('./recon-lineage-interpreter');
const fsmTelemetryInterpreter = require('./fsm-telemetry-interpreter');
const hsmTelemetryInterpreter = require('./hsm-telemetry-interpreter');

module.exports = {
  // Lineage Interpreters
  fsmLineage: fsmLineageInterpreter,
  hsmLineage: hsmLineageInterpreter,
  reconLineage: reconLineageInterpreter,

  // Telemetry Interpreters
  fsmTelemetry: fsmTelemetryInterpreter,
  hsmTelemetry: hsmTelemetryInterpreter,

  // Convenience exports
  getFSMLineage: (domain, n) => fsmLineageInterpreter.getFSMLineage(domain, n),
  getHSMLineage: (n) => hsmLineageInterpreter.getHSMLineage(n),
  getReconLineage: (n) => reconLineageInterpreter.getReconLineage(n),
  getFSMTelemetry: (domain) => fsmTelemetryInterpreter.getFSMTelemetry(domain),
  getHSMTelemetry: () => hsmTelemetryInterpreter.getHSMTelemetry(),
};
