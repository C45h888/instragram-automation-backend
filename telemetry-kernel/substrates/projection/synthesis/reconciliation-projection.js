// telemetry-kernel/substrates/projection/synthesis/reconciliation-projection.js
// Reconciliation Projection Synthesis: pure function from raw reconciliation
// emissions → semantic reconciliation health projection payload.
//
// Reads signals collected by reconciliation-input.js (transition log slice
// for domain='reconciliation'). Synthesizes the reconciliation subsystem
// health: current FSM state, epoch counts, drift counters, escalation state.
//
// Deterministic: same signals + same version → same payload.
// Replay-safe. No external state reads. No I/O.

const PROJECTION_VERSION = '1.0.0';
const RECONCILIATION_FSM_STATES = Object.freeze([
  'IDLE', 'RECONCILING', 'CONVERGENT', 'DRIFTED',
]);
const STALE_AFTER_MS = 300_000;

function synthesize(_projectionState, signals) {
  const { transitions, now } = signals;
  const safeTransitions = Array.isArray(transitions) ? transitions : [];
  const currentState = deriveCurrentState(safeTransitions);
  const epochCount = deriveLastValue(safeTransitions, 'epochCount');
  const driftCounters = deriveLastDriftCounters(safeTransitions);
  const escalationSignaled = deriveLastValue(safeTransitions, 'escalationSignaled');
  const driftedEpochCount = driftCounters ? (driftCounters.substrate || 0) : 0;
  const replayDriftCount = driftCounters ? (driftCounters.replay || 0) : 0;
  const driftRate = epochCount > 0 ? driftedEpochCount / epochCount : 0;

  return {
    currentReconciliationState: currentState,
    epochCount: epochCount || 0,
    driftedEpochCount,
    replayDriftCount,
    escalationSignaled: escalationSignaled === true,
    driftRate: Math.round(driftRate * 1000) / 1000,
    totalTransitions: safeTransitions.length,
    isStale: computeAging(safeTransitions, now) > STALE_AFTER_MS,
  };
}

function deriveCurrentState(transitions) {
  if (transitions.length === 0) return 'IDLE';
  for (let i = transitions.length - 1; i >= 0; i--) {
    const t = transitions[i];
    if (t.entity === 'fsm') {
      const ns = t.nextState;
      if (RECONCILIATION_FSM_STATES.includes(ns)) return ns;
    }
  }
  return 'IDLE';
}

function deriveLastValue(transitions, key) {
  for (let i = transitions.length - 1; i >= 0; i--) {
    const raw = transitions[i].raw || {};
    if (raw[key] !== undefined && raw[key] !== null) return raw[key];
  }
  return null;
}

function deriveLastDriftCounters(transitions) {
  for (let i = transitions.length - 1; i >= 0; i--) {
    const raw = transitions[i].raw || {};
    if (raw.driftCounters && typeof raw.driftCounters === 'object') {
      return raw.driftCounters;
    }
  }
  return null;
}

function computeAging(transitions, now) {
  if (transitions.length === 0) return Number.POSITIVE_INFINITY;
  const last = transitions[transitions.length - 1];
  const lastTs = last.timestamp || (last.wallClockTimestamp || 0);
  if (!lastTs) return Number.POSITIVE_INFINITY;
  return Math.max(0, now - lastTs);
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
};
