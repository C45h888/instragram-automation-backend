// substrates/graph-capability/wiring.js
// Boot-time wiring. Runs once on server boot to make the substrate operationally live.
//
// Architecture:
//   server.js → wiring.install({ck}) → resolves FSM → builds CK context → substrate.bindFsm() → substrate.start()
//
// Idempotent: calling install() multiple times is safe. The substrate.start() guards against double-start.
// The FSM reference is resolved from CK's domain registry if available, else via direct require.
//
// Contract:
//   install({ck}) → { fsm, ctx, started }
//     fsm     — the graph-capability-fsm instance
//     ctx     — the CK context object passed to FSM.dispatch
//     started — boolean, whether substrate.start() succeeded

const substrate = require('./index');
const fsm = require('../../control-plane/governance/domains/graph-capability-fsm');

let _installed = false;
let _fsmRef = null;
let _ctxRef = null;

/**
 * Resolve the FSM reference. Prefer CK's domain registry; fall back to direct require.
 * @param {object} ck — constitutional kernel
 * @returns {object} the FSM instance
 */
function _resolveFsm(ck) {
  // Try CK's domain registry first (preferred — keeps FSM as a registered domain)
  if (ck && ck._domains && typeof ck._domains.get === 'function') {
    const registered = ck._domains.get('graph-capability');
    if (registered) return registered;
  }
  // Fallback: direct require (the FSM is a singleton with module-level state)
  return fsm;
}

/**
 * Build the CK context object that the FSM consumes in its dispatch() call.
 * @param {object} ck — constitutional kernel
 * @returns {{ validate: Function, dispatchGlobal: Function, getGlobalState: Function }}
 */
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
  };
}

/**
 * Install the graph-capability substrate into the runtime.
 * Binds the FSM to the substrate, starts the worker cadence loops.
 *
 * @param {{ ck: object }} params
 * @returns {{ fsm: object, ctx: object, started: boolean }}
 */
function install({ ck } = {}) {
  if (_installed) {
    return { fsm: _fsmRef, ctx: _ctxRef, started: substrate.isStarted() };
  }

  _fsmRef = _resolveFsm(ck);
  _ctxRef = _buildCtx(ck);

  substrate.bindFsm(_fsmRef, _ctxRef);
  substrate.start({ fsm: _fsmRef, ctx: _ctxRef });

  _installed = true;
  console.log('[graph-capability] Wiring installed — substrate live');
  return { fsm: _fsmRef, ctx: _ctxRef, started: true };
}

/**
 * Tear down the wiring. Stops substrate, unbinds FSM.
 * Primarily for test cleanup.
 */
function uninstall() {
  if (!_installed) return;
  substrate.stop();
  _installed = false;
  _fsmRef = null;
  _ctxRef = null;
  console.log('[graph-capability] Wiring uninstalled — substrate stopped');
}

function isInstalled() {
  return _installed;
}

module.exports = {
  install,
  uninstall,
  isInstalled,
};
