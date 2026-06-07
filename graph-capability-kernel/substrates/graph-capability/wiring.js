// graph-capability-kernel/substrates/graph-capability/wiring.js
// Boot-time wiring. Runs once on server boot to make the substrate operationally live.
// Migrated from substrates/graph-capability/wiring.js
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
const fsm = require('../../fsm');
const signalDispatch = require('../vault/signal-dispatch');

let _installed = false;
let _fsmRef = null;
let _ctxRef = null;
let _lastBoundCk = null;

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
 * Idempotent on the same CK. If a different CK is passed while already
 * installed, the binding is updated (the substrate FSM, context, and
 * signal-dispatch are all rebound to the new CK).
 *
 * @param {{ ck: object }} params
 * @returns {{ fsm: object, ctx: object, started: boolean }}
 */
function install({ ck } = {}) {
  if (_installed) {
    // Already installed. If the new CK is the same object, return the
    // cached references (no work to do). If the CK changed, rebind.
    const isSameCk = (ck === _lastBoundCk);
    if (isSameCk) {
      return { fsm: _fsmRef, ctx: _ctxRef, started: substrate.isStarted() };
    }
    // CK changed — rebind signal-dispatch and the substrate context.
    // The FSM reference is the same singleton (it's a domain, not per-CK).
    // The CK context is rebuilt against the new CK.
    _ctxRef = _buildCtx(ck);
    signalDispatch.bindCk(ck);
    substrate.bindFsm(_fsmRef, _ctxRef);
    _lastBoundCk = ck;
    console.log('[graph-capability] Wiring re-installed with new CK — context rebound');
    return { fsm: _fsmRef, ctx: _ctxRef, started: substrate.isStarted() };
  }

  _fsmRef = _resolveFsm(ck);
  _ctxRef = _buildCtx(ck);
  _lastBoundCk = ck;

  // Layer 1.2: bind CK into signal-dispatch so every vault success path
  // reaches the constitutional ingress. This closes GAP-3.
  signalDispatch.bindCk(ck);

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
  // Layer 1: release the CK binding so uninstall is fully reversible.
  signalDispatch.bindCk(null);
  _installed = false;
  _fsmRef = null;
  _ctxRef = null;
  _lastBoundCk = null;
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