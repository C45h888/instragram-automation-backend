// telemetry-kernel/substrates/projection/synthesis/capability-projection.js
// Capability Projection Synthesis: pure function from raw graph-capability
// emissions → semantic capability projection payload.
//
// Reads signals collected by capability-input.js (transition log slice
// for domain='graph-capability', entity='fsm'). Synthesizes the
// capability state of the IG account/auth plane in a format consumable
// by the namespace-projection-interpreter.
//
// Deterministic: same signals + same version → same payload.
// Replay-safe. No external state reads. No I/O.

const PROJECTION_VERSION = '1.0.0';
const CAPABILITY_STATES = Object.freeze([
  'AUTHORIZED', 'UNAUTHORIZED', 'LIMITED', 'DEGRADED', 'UNKNOWN',
]);
const REQUIRED_SCOPES = [
  'instagram_basic',
  'instagram_manage_comments',
  'instagram_manage_insights',
  'instagram_content_publish',
  'pages_show_list',
  'pages_read_engagement',
];
const STALE_AFTER_MS = 300_000; // 5 min — capability projection considered stale after this
const HEALTHY_TRANSITION_VELOCITY = 2; // transitions/min — above this signals oscillation

function synthesize(_projectionState, signals) {
  const { transitions, scopeCoverage, now } = signals;
  const safeTransitions = Array.isArray(transitions) ? transitions : [];
  const current = currentStateFromTransitions(safeTransitions);
  const velocity = computeTransitionVelocity(safeTransitions, now);
  const stability = computeAuthorityStability(current, safeTransitions, scopeCoverage);
  const aging = computeAging(safeTransitions, now);
  const distribution = computeStateDistribution(safeTransitions);
  const oscillation = detectOscillation(safeTransitions, velocity);

  return {
    currentCapabilityState: current,
    capabilityAuthorityStability: stability,
    capabilityTransitionVelocity: velocity,
    capabilityAging: aging,
    capabilityOscillation: oscillation,
    scopeCoverage: typeof scopeCoverage === 'number' ? scopeCoverage : 0,
    stateDistribution: distribution,
    degradedStateCount: distribution.DEGRADED || 0,
    unauthorizedStateCount: distribution.UNAUTHORIZED || 0,
    authorizedStateCount: distribution.AUTHORIZED || 0,
    limitedStateCount: distribution.LIMITED || 0,
    totalTransitions: safeTransitions.length,
    isStale: aging > STALE_AFTER_MS,
  };
}

function currentStateFromTransitions(transitions) {
  if (transitions.length === 0) return 'UNKNOWN';
  const last = transitions[transitions.length - 1];
  const next = (last && last.nextState) || 'UNKNOWN';
  return CAPABILITY_STATES.includes(next) ? next : 'UNKNOWN';
}

function computeTransitionVelocity(transitions, now) {
  if (transitions.length < 2) return 0;
  const ts = (t) => {
    const candidate = t.timestamp || t.raw?.timestamp || (t.wallClockTimestamp || 0);
    return typeof candidate === 'number' && candidate > 0 ? candidate : now;
  };
  const earliest = ts(transitions[0]);
  const latest = ts(transitions[transitions.length - 1]);
  if (!earliest || !latest || latest <= earliest) return 0;
  const windowMs = latest - earliest;
  if (windowMs <= 0) return 0;
  return (transitions.length / windowMs) * 60_000; // transitions / minute
}

function computeAuthorityStability(current, transitions, scopeCoverage) {
  let stability = 1.0;
  if (current === 'UNAUTHORIZED') stability = 0.0;
  else if (current === 'DEGRADED') stability = 0.4;
  else if (current === 'LIMITED') stability = 0.6;
  else if (current === 'UNKNOWN') stability = 0.2;
  else if (current === 'AUTHORIZED') {
    stability = 0.95;
    if (typeof scopeCoverage === 'number' && scopeCoverage < 1.0) {
      stability = 0.95 * scopeCoverage;
    }
  }
  // Penalize for transitions ending in unhealthy states within the window
  const recent = transitions.slice(-5);
  for (const t of recent) {
    if (t.nextState === 'UNAUTHORIZED') stability = Math.min(stability, 0.3);
    else if (t.nextState === 'DEGRADED') stability = Math.min(stability, 0.5);
  }
  return Math.max(0, Math.min(1, stability));
}

function computeAging(transitions, now) {
  if (transitions.length === 0) return Number.POSITIVE_INFINITY;
  const last = transitions[transitions.length - 1];
  const lastTs = last.timestamp || last.raw?.timestamp || (last.wallClockTimestamp || 0);
  if (!lastTs) return Number.POSITIVE_INFINITY;
  return Math.max(0, now - lastTs);
}

function computeStateDistribution(transitions) {
  const out = { AUTHORIZED: 0, UNAUTHORIZED: 0, LIMITED: 0, DEGRADED: 0, UNKNOWN: 0 };
  for (const t of transitions) {
    if (CAPABILITY_STATES.includes(t.nextState)) {
      out[t.nextState] = (out[t.nextState] || 0) + 1;
    }
  }
  return out;
}

function detectOscillation(transitions, velocity) {
  if (transitions.length < 3) return false;
  if (velocity > HEALTHY_TRANSITION_VELOCITY) return true;
  // Count state reversals (e.g., AUTHORIZED → UNAUTHORIZED → AUTHORIZED)
  let reversals = 0;
  for (let i = 1; i < transitions.length; i++) {
    if (transitions[i].nextState !== transitions[i - 1].nextState) reversals++;
  }
  return reversals > transitions.length * 0.5;
}

function computeConfidence(signals) {
  if (!signals || signals.noiseGate) return 0.0;
  const total = signals.transitions ? signals.transitions.length : 0;
  if (total === 0) return 0.0;
  if (total < 3) return 0.3;
  if (total < 10) return 0.6;
  return 1.0;
}

function computeIntegrityScore(signals) {
  const total = signals && signals.transitions ? signals.transitions.length : 0;
  if (total === 0) return 0.0;
  return Math.min(1.0, total / 10);
}

module.exports = {
  synthesize,
  computeConfidence,
  computeIntegrityScore,
  PROJECTION_VERSION,
  REQUIRED_SCOPES,
  STALE_AFTER_MS,
};
