// telemetry-kernel/substrates/projection/synthesis/persist-telemetry-projection.js
// Persist-Telemetry Projection Synthesis: pure function from raw
// persist-telemetry emissions → semantic DB health projection payload.
//
// Reads signals collected by persist-telemetry-input.js (transition log
// slice for domain='persist-telemetry'). Synthesizes the health of the
// postgres telemetry DB layer: write/read counts, failure rate, current
// FSM state, table hit distribution, severity breakdown.
//
// Deterministic: same signals + same version → same payload.
// Replay-safe. No external state reads. No I/O.

const PROJECTION_VERSION = '1.0.0';
const PERSIST_TELEMETRY_FSM_STATES = Object.freeze([
  'IDLE', 'WRITING', 'READING', 'ERROR', 'FAILED', 'RECOVERING',
]);

const INTENT_CATEGORIES = Object.freeze([
  'DB_WRITE_REQUESTED', 'DB_WRITE_COMPLETE',
  'DB_READ_REQUESTED', 'DB_READ_COMPLETE',
  'DB_PERSIST_FAILURE', 'DB_PERSIST_FAILURE_READ',
  'DB_WRITE_FAILED', 'DB_READ_FAILED',
]);

function synthesize(_projectionState, signals) {
  const { transitions, now } = signals;
  const safeTransitions = Array.isArray(transitions) ? transitions : [];
  const currentState = deriveCurrentState(safeTransitions);
  const counts = computeIntentCounts(safeTransitions);
  const tableDist = computeTableDistribution(safeTransitions);
  const severityBreakdown = computeSeverityBreakdown(safeTransitions);
  const lastInFlight = deriveLastInFlight(safeTransitions);
  const failureRate = computeFailureRate(counts);

  return {
    currentPersistTelemetryState: currentState,
    writeCount: counts.DB_WRITE_REQUESTED || 0,
    writeCompleteCount: counts.DB_WRITE_COMPLETE || 0,
    readCount: counts.DB_READ_REQUESTED || 0,
    readCompleteCount: counts.DB_READ_COMPLETE || 0,
    failureCount: (counts.DB_PERSIST_FAILURE || 0) + (counts.DB_PERSIST_FAILURE_READ || 0) + (counts.DB_WRITE_FAILED || 0) + (counts.DB_READ_FAILED || 0),
    failureRate,
    tableDistribution: tableDist,
    severityBreakdown,
    inFlight: lastInFlight,
    totalTransitions: safeTransitions.length,
    isStale: computeAging(safeTransitions, now) > 300_000,
  };
}

function deriveCurrentState(transitions) {
  if (transitions.length === 0) return 'UNKNOWN';
  // Walk backwards to find the last FSM state transition
  for (let i = transitions.length - 1; i >= 0; i--) {
    const t = transitions[i];
    if (t.entity === 'fsm') {
      const ns = t.nextState;
      if (PERSIST_TELEMETRY_FSM_STATES.includes(ns)) return ns;
    }
  }
  return 'UNKNOWN';
}

function computeIntentCounts(transitions) {
  const counts = {};
  for (const t of transitions) {
    const intent = (t.raw && t.raw.intent) || null;
    if (intent && INTENT_CATEGORIES.includes(intent)) {
      counts[intent] = (counts[intent] || 0) + 1;
    }
  }
  return counts;
}

function computeTableDistribution(transitions) {
  const dist = {};
  for (const t of transitions) {
    const table = (t.raw && t.raw.table) || null;
    if (table) {
      dist[table] = (dist[table] || 0) + 1;
    }
  }
  return dist;
}

function computeSeverityBreakdown(transitions) {
  const breakdown = { LOW: 0, MEDIUM: 0, HIGH: 0, CRITICAL: 0 };
  for (const t of transitions) {
    const severity = (t.raw && t.raw.severity) || null;
    if (severity && breakdown[severity] !== undefined) {
      breakdown[severity]++;
    }
  }
  return breakdown;
}

function computeFailureRate(counts) {
  const writes = (counts.DB_WRITE_REQUESTED || 0) + (counts.DB_WRITE_COMPLETE || 0);
  const reads = (counts.DB_READ_REQUESTED || 0) + (counts.DB_READ_COMPLETE || 0);
  const total = writes + reads;
  if (total === 0) return 0;
  const failures = (counts.DB_PERSIST_FAILURE || 0) + (counts.DB_PERSIST_FAILURE_READ || 0);
  return failures / total;
}

function deriveLastInFlight(transitions) {
  for (let i = transitions.length - 1; i >= 0; i--) {
    const inFlight = transitions[i].raw && transitions[i].raw.inFlight;
    if (inFlight !== null && inFlight !== undefined) return inFlight;
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
