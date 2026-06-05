// graph-capability-kernel/index.js
// Kernel root façade. Wires FSM to constitutional kernel, exposes public surface.
// Migrated from substrates/graph-capability/wiring.js pattern.
//
// Architecture:
//   server.js → graphCapabilityKernel.install({ck}) → binds FSM → starts substrate
//   External consumers import vault and graph-capability modules from here.
//
// Pattern:
//   const gck = require('./graph-capability-kernel');
//   gck.vault.pat.exchange({ ... });
//   gck.verdictGate.requireCapability(...);

const wiring = require('./substrates/graph-capability/wiring');
const fsm = require('./fsm');

// Re-export the public surface from the kernel substrates
const vault = require('./substrates/vault');
const verdictGate = require('./substrates/graph-capability/verdict-gate');
const triggerBridge = require('./substrates/graph-capability/trigger-bridge');

let _installed = false;
let _started = false;

/**
 * Install the graph-capability kernel into the runtime.
 * Wires FSM to constitutional kernel and starts the capability plane.
 *
 * @param {{ ck: object }} params
 * @returns {{ fsm: object, ctx: object, started: boolean }}
 */
function install({ ck } = {}) {
  if (_installed) {
    return { fsm, started: _started };
  }

  const result = wiring.install({ ck });
  _installed = true;
  _started = result.started;

  return { fsm, started: _started };
}

function uninstall() {
  wiring.uninstall();
  _installed = false;
  _started = false;
}

function isInstalled() {
  return _installed;
}

module.exports = {
  install,
  uninstall,
  isInstalled,
  // Public surface: vault (pat/uat/scope operations)
  vault,
  // Public surface: graph-capability read/write
  verdictGate,
  triggerBridge,
  // Direct FSM access for CK registration
  fsm,
};