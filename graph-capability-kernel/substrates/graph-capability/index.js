// graph-capability-kernel/substrates/graph-capability/index.js
// Graph Capability substrate façade. PURE BINDING — no workers, no cadence, no I/O.
// Migrated from substrates/graph-capability/index.js
//
// Constitutional rule (enforced):
//   Graph-capability = pure governance plane.
//     - FSM (in graph-capability-kernel/fsm.js)
//     - Verdict-gate (read surface, graph-capability-kernel/substrates/graph-capability/verdict-gate.js)
//     - Trigger-bridge (event ingress, graph-capability-kernel/substrates/graph-capability/trigger-bridge.js)
//     - Wiring (boot-time install, graph-capability-kernel/substrates/graph-capability/wiring.js)
//     - Observations normalizer (pure function, graph-capability-kernel/substrates/graph-capability/observations.js)
//     - This façade (binds FSM to substrate index, exposes start/stop/isStarted)
//
//   No workers. No setInterval. No aggregation loop. No state.
//   The canonical workers live in graph-capability-kernel/substrates/vault/<domain>/workers/
//   They are event-driven. When a vault worker call succeeds, the substrate façade
//   emits a trigger → trigger-bridge → ck → FSM. The FSM transitions.
//   This façade does not own that flow — it just binds the FSM into the runtime.

const observations = require('./observations');

// ── FSM binding ──────────────────────────────────────────────────────────────

let _fsm = null;
let _ckContext = null;
let _started = false;

function bindFsm(fsm, ctx) {
  _fsm = fsm;
  _ckContext = ctx;
}

/**
 * Dispatch an event to the bound FSM. Pure pass-through. No state, no aggregation.
 * @param {object} event
 */
function _dispatchToFsm(event) {
  if (!_fsm || typeof _fsm.dispatch !== 'function') {
    return;
  }
  try {
    _fsm.dispatch(event, _ckContext);
  } catch (err) {
    console.warn(`[graph-capability] FSM dispatch failed for ${event.type}:`, err.message);
  }
}

// ── Public lifecycle ─────────────────────────────────────────────────────────

/**
 * Bind the FSM into the substrate index. No timers registered. No workers started.
 * The graph-capability plane is reactive: it only does work in response to events
 * emitted by the canonical vault workers.
 *
 * @param {{ fsm: object, ctx: object }} bindings
 */
function start(bindings = {}) {
  if (_started) {
    console.log('[graph-capability] Substrate already started');
    return;
  }
  if (bindings.fsm) bindFsm(bindings.fsm, bindings.ctx);
  _started = true;
  console.log('[graph-capability] Substrate bound — no workers, no cadence (vault owns observation)');
}

function stop() {
  if (!_started) return;
  _started = false;
  _fsm = null;
  _ckContext = null;
  console.log('[graph-capability] Substrate unbound');
}

function isStarted() {
  return _started;
}

module.exports = {
  start,
  stop,
  bindFsm,
  isStarted,
  observations,
  // Internal — exposed only for tests
  _dispatchToFsm,
};