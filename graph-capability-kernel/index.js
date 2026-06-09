// graph-capability-kernel/index.js
// Kernel root façade. Wires FSM to constitutional kernel, exposes public surface.
// Migrated from substrates/graph-capability/wiring.js pattern.
//
// Architecture (Phase D — constitutional contract, no wiring.js):
//   server.js → gck.install({ck}) → builds dispatch ctx → fsm.setDispatchCtx(ctx)
//     → fsm.setGovernance(ck) → registers membranes (health-substrate, etc.)
//     → fsm.setMembrane('health', {substrate}) → starts the graph-capability
//     substrate → binds signal-dispatch to fsm + ctx
//
//   The FSM is the constitutional ingress for substrate emissions.
//   The CK provides the action fabric (subscribeAction) which the substrate
//   subscribes to via substrate.start(ck). The substrate is a delegated
//   executor orchestrated by the FSM — it never talks to the CK directly
//   for emissions, and the CK never calls the substrate directly.
//
// Pattern:
//   const gck = require('./graph-capability-kernel');
//   gck.install({ ck });
//   gck.vault.pat.exchange({ ... });
//   gck.fsm.requireCapability(...);

const wiring = require('./substrates/graph-capability/wiring');
const fsm = require('./fsm');
const signalDispatch = require('./substrates/vault/signal-dispatch');
const healthSubstrate = require('./substrates/health-substrate');

// Re-export the public surface from the kernel substrates
const vault = require('./substrates/vault');
const health = require('./substrates/health-substrate');

let _installed = false;
let _started = false;
let _lastBoundCk = null;

// ── Dispatch ctx — shared with the FSM and the signal-dispatch module ──────
// The ctx is the same shape the CK passes to fsm.dispatch today:
//   { validate, dispatchGlobal, getGlobalState, sanityCheck }.
//
//   validate        → asks CK to validate a domain transition
//   dispatchGlobal  → forwards an event to the CK for cross-domain routing
//   getGlobalState  → reads CK's global state
//   sanityCheck     → universal gate (fail-open by default)
//
// Built once at install time. The signal-dispatch module captures it at
// bindFsm(fsm, ctx) so substrate emissions route to the FSM with the
// correct ctx (so the FSM can ctx.dispatchGlobal back to the CK).
function _buildCtx(ck) {
  return {
    validate: (from, to, event) => {
      if (ck && typeof ck.validateDomainTransition === 'function') {
        return ck.validateDomainTransition('graph-capability', from, to, event);
      }
      return { allowed: true };
    },
    dispatchGlobal: (event) => {
      if (ck && typeof ck.dispatch === 'function') {
        return ck.dispatch(event);
      }
      return { allowed: false, reason: 'CK not available' };
    },
    getGlobalState: () => {
      if (ck && typeof ck.getState === 'function') {
        return ck.getState();
      }
      return 'UNKNOWN';
    },
    sanityCheck: async () => ({ allowed: true }),
  };
}

/**
 * Install the graph-capability kernel into the runtime.
 * Wires FSM to constitutional kernel, registers membranes (delegated
 * executors) with the FSM, starts the graph-capability substrate, and
 * binds signal-dispatch to the FSM.
 *
 * Idempotent. If a different CK is passed while already installed, the
 * binding is updated.
 *
 * @param {{ ck: object }} params
 * @returns {{ fsm: object, started: boolean, healthStarted: boolean }}
 */
function install({ ck } = {}) {
  if (_installed) {
    // Re-install with the same CK — no-op
    if (ck === _lastBoundCk) {
      return { fsm, started: _started, healthStarted: health.isStarted ? health.isStarted() : false };
    }
    // CK changed — rebuild ctx and rebind signal-dispatch to the new FSM-binding
    const ctx = _buildCtx(ck);
    fsm.setDispatchCtx(ctx);
    fsm.setGovernance(ck);
    signalDispatch.bindFsm(fsm, ctx);
    _lastBoundCk = ck;
    return { fsm, started: _started, healthStarted: health.isStarted ? health.isStarted() : false };
  }

  // 1. Build the dispatch ctx — passed to fsm.dispatch for substrate emissions
  const ctx = _buildCtx(ck);

  // 2. Wire the FSM to the constitutional kernel and ctx
  fsm.setDispatchCtx(ctx);
  fsm.setGovernance(ck);

  // 3. Bind signal-dispatch to FSM + ctx (FSM is the constitutional ingress)
  signalDispatch.bindFsm(fsm, ctx);

  // 4. Register delegated executor membranes with the FSM. The FSM is the
  //    orchestrator — it will call substrate.start(ck) on the first
  //    CAPABILITY_BOOTSTRAP transition. The CK never calls substrates directly.
  fsm.setMembrane('health', { substrate: healthSubstrate });

  // 5. Start the graph-capability substrate (binding only, no workers of its own)
  const result = wiring.install({ ck });
  _started = result.started;
  _installed = true;
  _lastBoundCk = ck;

  // 6. (Removed) health-substrate is not "started" until the FSM wires it
  //    during the first CAPABILITY_BOOTSTRAP transition. The membrane's
  //    isStarted() returns false until that happens. This is constitutional
  //    correctness: the substrate does not exist as a first-class citizen
  //    until the FSM orchestrates it. The CK (via gck.install) does not
  //    start substrates.

  return { fsm, started: _started, healthStarted: health.isStarted ? health.isStarted() : false };
}

function uninstall() {
  // Stop the graph-capability substrate first
  wiring.uninstall();
  // Release the FSM binding
  signalDispatch.bindFsm(null, null);
  fsm.setDispatchCtx(null);
  fsm.setGovernance(null);
  // Reset all membrane wire state so the next install re-wires them
  fsm.resetMembrane();
  _installed = false;
  _started = false;
  _lastBoundCk = null;
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
  // Public surface: health substrate
  health,
  // Direct FSM access for CK registration
  fsm,
};
