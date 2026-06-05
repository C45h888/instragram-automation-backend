// dedup-kernel/index.js
// Kernel root façade. Wires FSM to constitutional kernel, exposes public surface.
//
// External consumers import dedup substrate from here.
// Orchestrator wires FSM to CK via registerDomain(dedupKernel.fsm).
//
// Pattern:
//   const dedupKernel = require('./dedup-kernel');
//   constitutional.registerDomain(dedupKernel.fsm);
//   const dedupSubstrate = dedupKernel.substrate;

const fsm = require('./fsm');
const substrate = require('./substrates/dedup');
const conversationRepair = require('./substrates/repair/conversation-repair');

module.exports = {
  fsm,
  substrate,
  conversationRepair,
};