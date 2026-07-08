// control-plane/governance/webview-fsm.js
// WebView Reactive Membrane — Domain FSM.
//
// Pass 7 (S5 "Cross-Repo Bridge — Consumer Side"). The webview-fsm
// domain is the sole authority for transitions arriving from the
// WebView kernel via the lineage:webview:transitions stream.
//
// This file is Pass 7.1 — the mechanical stub. It registers the
// domain with the Constitutional Kernel, accepts every
// WEBVIEW_TRANSITION_REQUESTED dispatch, and emits a single
// {allowed: true} decision. Pass 7.2 (webview-stream.js + probe +
// guard) takes over the semantic surface in-place by replacing
// the dispatch() body and growing exports — registerDomain does
// NOT need to change.
//
// Contract (per constitutional-kernel.js registerDomain, line
// ~1187): name, dispatch(event, ctx), getState(), exportState(),
// getHealth(), init(state) is optional.
//
// Architectural contract:
//   - dispatch() returns { allowed, actions, lineageId } and is
//     the ONLY place where the WebView-origin decision is made.
//   - getState() returns the domain state machine's local
//     current state name (string).
//   - exportState() returns a JSON-serialisable snapshot.
//   - getHealth() returns health signals consumed by the CK's
//     degradation detection.
//   - The FSM never writes to the lineage ledger directly;
//     ctx.recordLineage is the only legitimate write path.

'use strict';

const FSM_NAME = 'webview-fsm';

// Local probe + decision + receipt modules (Pass 7.2 — semantic).
// Lazy-required: ensure the module load order tolerates a partial
// rollback that removes webview-stream.js / webview-receipt.js
// without breaking the FSM contract.
let _probe = null;
let _decision = null;
function _getProbe() {
  if (!_probe) {
    try { _probe = require('./probes/webview-transition.probe'); }
    catch (_) { _probe = { probeWebviewTransition: () => ({ ok: false, error: 'probe unavailable', data: null, queriedAtEpochMs: Date.now() }) }; }
  }
  return _probe;
}
function _getDecision() {
  if (!_decision) {
    try { _decision = require('./webview-decision'); }
    catch (_) { _decision = { computeDecision: (r) => ({ decision: 'REJECTED', reason: 'decision module unavailable' }) }; }
  }
  return _decision;
}

// WebView FSM internal state — the FSM itself is stateless at the
// pass-7.1 stub level. A real per-transition observation is added
// in 7.2; until then, every dispatch returns immediately.
let _lastDispatchedAtEpochMs = null;
let _lastDispatchCount = 0;
let _totalDispatches = 0;
let _currentState = 'IDLE';

const STATES = Object.freeze(['IDLE', 'OBSERVING', 'DEGRADED']);
const ALLOWED_EVENTS = Object.freeze([
  'WEBVIEW_TRANSITION_REQUESTED',
]);

/**
 * Optional rehydration entry — called by CK.registerDomain when the
 * FSM boot path has a stored state from the lineage. The stub
 * accepts the state literally; 7.2 will forward-port a richer
 * rehydrate path that reads from lineage:webview:read-results.
 *
 * @param {string} state — domain state name to restore
 */
function init(state) {
  if (typeof state === 'string' && STATES.includes(state)) {
    _currentState = state;
  }
}

/**
 * Domain dispatch — Pass 7.2 probe-gated guard.
 *
 * The dispatch evaluates every WEBVIEW_TRANSITION_REQUESTED against
 * the local probe (probeWebviewTransition) which validates the
 * producer-side XADD payload shape + the (from, event, to) rules
 * legality per the forward-ported rules table. The FSM guard
 * returns the probe verdict directly: legal = accept, illegal =
 * reject with a reason. The lineageId is the probe's
 * ruleFingerprint so receipts can correlate with the guard
 * decision trail.
 *
 * @param {object} event — { type: 'WEBVIEW_TRANSITION_REQUESTED',
 *                            payload: { transition, streamId? } }
 * @param {object} ctx  — CK dispatch ctx (lineage write handle)
 * @returns {Promise<{ allowed: boolean, actions: object[],
 *                    lineageId: string|null, reason?: string }>}
 */
async function dispatch(event, ctx) {
  _totalDispatches += 1;
  _lastDispatchedAtEpochMs = Date.now();
  _lastDispatchCount += 1;

  // Contract guard — same as Pass 7.1.
  if (!event || typeof event !== 'object' || typeof event.type !== 'string') {
    return { allowed: false, actions: [], lineageId: null,
             reason: 'webview-fsm.dispatch: missing event.type' };
  }
  if (!ALLOWED_EVENTS.includes(event.type)) {
    return { allowed: false, actions: [], lineageId: null,
             reason: `webview-fsm.dispatch: unknown event ${event.type}` };
  }

  // Pull the parsed transition out of the pump-built payload.
  const transition = event.payload && event.payload.transition;
  if (!transition || typeof transition !== 'object') {
    return { allowed: false, actions: [], lineageId: null,
             reason: 'webview-fsm.dispatch: missing payload.transition' };
  }

  // Run the local probe — read-only oracle that authors the
  // decision data (I32 carry-over: probe is the read source).
  const probe = _getProbe();
  const probeResult = probe.probeWebviewTransition(transition);
  if (!probeResult || probeResult.ok !== true || !probeResult.data) {
    _currentState = 'DEGRADED'; // probe failure surfaces DEGRADED
    const reason = (probeResult && probeResult.error)
      ? probeResult.error
      : 'webview-fsm.dispatch: probe returned non-ok';
    return { allowed: false, actions: [], lineageId: null, reason };
  }

  // Probe is the legal-transitions oracle. Pass-through to dispatch.
  _currentState = 'OBSERVING';
  return {
    allowed: true,
    actions: [],
    lineageId: probeResult.data.ruleFingerprint,
  };
}

/**
 * Domain-local current state. The CK uses this for status surfaces
 * and degradation detection.
 *
 * @returns {string} — one of STATES
 */
function getState() {
  return _currentState;
}

/**
 * Snapshot the FSM's exportable state for observability/lineage.
 * Pure JSON — no functions, no circular refs.
 *
 * @returns {object}
 */
function exportState() {
  return {
    name: FSM_NAME,
    currentState: _currentState,
    lastDispatchedAtEpochMs: _lastDispatchedAtEpochMs,
    lastDispatchCount: _lastDispatchCount,
    totalDispatches: _totalDispatches,
  };
}

/**
 * Health signals — consumed by the CK's `getDomainHealth` /
 * degradation detection. Reports the FSM's local state +
 * the rules-table hash (for D5 drift surfacing) + dispatch
 * counters. The FSM is "healthy" iff in OBSERVING; IDLE is
 * a healthy initial state; DEGRADED surfaces to the CK.
 *
 * @returns {{ healthy: boolean, signals: object[] }}
 */
function getHealth() {
  const healthy = _currentState !== 'DEGRADED';
  // Reach into the probe to surface the rules-table hash for drift.
  const probe = _getProbe();
  const rulesHash = (probe && typeof probe.EXPORTED_HASH === 'string')
    ? probe.EXPORTED_HASH : 'unavailable';
  return {
    healthy,
    signals: [
      { name: 'webview_fsm_state', value: _currentState, ok: healthy },
      { name: 'webview_fsm_rules_table_hash', value: rulesHash, ok: true },
      { name: 'webview_fsm_total_dispatches', value: _totalDispatches, ok: true },
      { name: 'webview_fsm_last_dispatched_at_epoch_ms',
        value: _lastDispatchedAtEpochMs, ok: true },
    ],
  };
}

/**
 * Lazy accessor used by the pump boot (webview-stream.js) to
 * verify the FSM is mounted before the XREAD loop starts
 * dispatching. Returns the singleton-bound dispatch function.
 */
function getDispatchHandle() {
  return { dispatch, getState, exportState, getHealth, init, name: FSM_NAME };
}

module.exports = {
  // ── CK contract surface (consumed by registerDomain) ──
  name: FSM_NAME,
  init,
  dispatch,
  getState,
  exportState,
  getHealth,
  // ── Pass 7.1.f — handle for 7.2 pump attachment ──
  getDispatchHandle,
  // ── Internal constants (exported for tests) ──
  _STATES: STATES,
  _ALLOWED_EVENTS: ALLOWED_EVENTS,
  _FSM_NAME: FSM_NAME,
};
