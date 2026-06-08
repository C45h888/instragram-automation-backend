// graph-capability-kernel/substrates/graph-capability/index.js
// Graph Capability substrate façade. PURE BINDING — no workers, no cadence, no I/O.
//
// Binds the FSM into the runtime. Called by wiring.install() at boot.

// ── FSM binding ──────────────────────────────────────────────────────────────

let _fsm = null;
let _ckContext = null;
let _started = false;

function bindFsm(fsm, ctx) {
  _fsm = fsm;
  _ckContext = ctx;
}

/**
 * Dispatch an event to the bound FSM. Pure pass-through.
 */
function _dispatchToFsm(event) {
  if (!_fsm || typeof _fsm.dispatch !== 'function') return;
  try { _fsm.dispatch(event, _ckContext); }
  catch (err) { console.warn(`[graph-capability] FSM dispatch failed for ${event.type}:`, err.message); }
}

function start(bindings = {}) {
  if (_started) return;
  if (bindings.fsm) bindFsm(bindings.fsm, bindings.ctx);
  _started = true;
}

function stop() {
  if (!_started) return;
  _started = false;
  _fsm = null;
  _ckContext = null;
}

function isStarted() { return _started; }

module.exports = { start, stop, bindFsm, isStarted, _dispatchToFsm };