// telemetry-kernel/substrates/projection/inputs/capability-input.js
// Capability Projection Input: reads the graph-capability FSM transition
// log slice from observability, derives a window of signals for synthesis.
//
// The graph-capability FSM (graph-capability-kernel/fsm.js) already emits
// STATE_TRANSITION entries to observability with:
//   domain: 'graph-capability'
//   entity: 'fsm'
//   entityId: 'graph-capability-fsm'
//   previousState, nextState (FSM states: AUTHORIZED, UNAUTHORIZED, LIMITED, DEGRADED, UNKNOWN)
//   raw.intent (the event type that caused the transition)
//
// This input is read-only. It does NOT modify the FSM, the substrate, or
// observability state. It returns a window object that the synthesis
// stage consumes.

const REQUIRED_SCOPES = [
  'instagram_basic',
  'instagram_manage_comments',
  'instagram_manage_insights',
  'instagram_content_publish',
  'pages_show_list',
  'pages_read_engagement',
];

const DEFAULT_WINDOW_SIZE = 50;
const DEFAULT_POLL_INTERVAL_MS = 30_000;

async function getNormalizedInputWindow({
  pollIntervalMs = DEFAULT_POLL_INTERVAL_MS,
  tickCount = 0,
  windowSize = DEFAULT_WINDOW_SIZE,
} = {}) {
  const observability = readObservability();
  const now = Date.now();

  // Read the recent graph-capability FSM transition log.
  // We use getTransitionLog (entity-scoped sliding window) to get the
  // most recent N transitions for entity='fsm' in domain='graph-capability'.
  // Falls back to empty list if observability is unavailable.
  let transitions = [];
  if (observability && observability.query && typeof observability.query.getTransitionLog === 'function') {
    try {
      const log = observability.query.getTransitionLog('graph-capability', 'fsm', windowSize);
      transitions = Array.isArray(log) ? log : [];
    } catch (_) {
      transitions = [];
    }
  }

  // Derive scope coverage: best-effort from the most recent transition's
  // raw payload (if any scope-grant evidence is present). Most graph-capability
  // transitions do NOT carry scope information directly (scope is set by
  // vault substrates, not the FSM). Default to null so synthesis treats it
  // as unknown.
  const scopeCoverage = deriveScopeCoverage(transitions);

  return {
    transitions,
    scopeCoverage,
    now,
    tickCount,
    windowOpenedAt: now - pollIntervalMs,
    windowClosedAt: now,
    entryCount: transitions.length,
    // Noise gate: a small sample is not enough to make confident claims.
    noiseGate: transitions.length < 3,
    source: 'observability.transitionLog[graph-capability:fsm]',
  };
}

function readObservability() {
  try {
    // eslint-disable-next-line global-require
    return require('../../../../control-plane/observability');
  } catch (_) {
    return null;
  }
}

function deriveScopeCoverage(transitions) {
  if (transitions.length === 0) return null;
  // Walk backwards to find the most recent transition that carries scope
  // information in its raw payload. If none, return null (unknown).
  for (let i = transitions.length - 1; i >= 0; i--) {
    const t = transitions[i];
    const raw = t && t.raw ? t.raw : {};
    if (Array.isArray(raw.grantedScopes) && Array.isArray(raw.requiredScopes)) {
      const granted = raw.grantedScopes;
      const required = raw.requiredScopes;
      if (required.length === 0) return 1.0;
      const present = required.filter((s) => granted.includes(s)).length;
      return present / required.length;
    }
  }
  return null;
}

module.exports = {
  getNormalizedInputWindow,
  REQUIRED_SCOPES,
  DEFAULT_WINDOW_SIZE,
  DEFAULT_POLL_INTERVAL_MS,
};
