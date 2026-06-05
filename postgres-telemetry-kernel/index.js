// postgres-telemetry-kernel/index.js
// Kernel root façade. Wires FSM to constitutional kernel, exposes public surface.
//
// Reading-substrate is NOT part of this kernel.
// CK owns reading-substrate and injects it into the FSM via setReadingSubstrate().
// The FSM receives it as an injected reference, not as a direct import.
//
// External consumers import the FSM from here:
//   const postgresTelemetryFsm = require('./postgres-telemetry-kernel').fsm;
//   constitutional.registerDomain(postgresTelemetryFsm);
//
// CK wires reading-substrate at registerDomain():
//   if (fsm.name === 'persist-telemetry') {
//     readingSubstrate.init({ governance: ck, fsm });
//     fsm.setReadingSubstrate(readingSubstrate);
//   }

const fsm = require('./fsm');
const writers = require('./writers');

module.exports = {
  fsm,
  writers,
};