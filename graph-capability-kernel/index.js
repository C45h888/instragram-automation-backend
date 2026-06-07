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
const healthWiring = require('./substrates/health-substrate/wiring');
const fsm = require('./fsm');

// Re-export the public surface from the kernel substrates
const vault = require('./substrates/vault');
const verdictGate = require('./substrates/graph-capability/verdict-gate');
const triggerBridge = require('./substrates/graph-capability/trigger-bridge');
const health = require('./substrates/health-substrate');

let _installed = false;
let _started = false;

/**
 * Install the graph-capability kernel into the runtime.
 * Wires FSM to constitutional kernel, starts the graph-capability substrate,
 * then starts the health substrate.
 *
 * @param {{ ck: object }} params
 * @returns {{ fsm: object, ctx: object, started: boolean, healthStarted: boolean }}
 */
function install({ ck } = {}) {
  if (_installed) {
    return { fsm, started: _started, healthStarted: health.isStarted ? health.isStarted() : false };
  }

  const result = wiring.install({ ck });
  _installed = true;
  _started = result.started;

  // Layer 4.2: start the health substrate. Its wiring.install() is a no-op
  // guard unless the caller wants explicit guard semantics. Starting the
  // substrate makes it accept run calls; signal-dispatch binding is shared
  // with vault (set by wiring.install above).
  const healthResult = healthWiring.install();
  if (health.start) health.start();

  return { fsm, started: _started, healthStarted: healthResult.started };
}

function uninstall() {
  // Layer 4.2: stop health before tearing down the constitutional binding.
  if (health.stop) health.stop();
  if (healthWiring.isInstalled()) healthWiring.uninstall();
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
  // Public surface: health substrate (token-health / uat-refresh orchestration)
  // Migrated from services/sync/token-health.js — see
  // graph-capability-kernel/substrates/health-substrate/
  health,
  // Direct FSM access for CK registration
  fsm,
};