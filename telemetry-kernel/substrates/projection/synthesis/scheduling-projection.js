// scheduling: transitions → { currentSchedulingState, cadenceContinuity, accountCount }

const PROJECTION_VERSION = '1.0.0';
const SCHEDULING_FSM_STATES = Object.freeze(['IDLE', 'SCANNING', 'DISPATCHING', 'COMPLETING', 'FAILED']);

function synthesize(_projectionState, signals) {
  const { transitions, now } = signals;
  const safeTransitions = Array.isArray(transitions) ? transitions : [];
  const currentState = deriveCurrentState(safeTransitions);
  const accountCount = deriveLastValue(safeTransitions, 'accountIds');
  const accountCountNum = Array.isArray(accountCount) ? accountCount.length : (typeof accountCount === 'number' ? accountCount : 0);

  return {
    currentSchedulingState: currentState,
    cadenceContinuity: computeCadenceContinuity(safeTransitions),
    accountCount: accountCountNum,
    totalTransitions: safeTransitions.length,
    isStale: computeAging(safeTransitions, now) > 300_000,
  };
}

function deriveCurrentState(transitions) {
  if (transitions.length === 0) return 'IDLE';
  for (let i = transitions.length - 1; i >= 0; i--) {
    const t = transitions[i];
    if (t.entity === 'fsm' && SCHEDULING_FSM_STATES.includes(t.nextState)) return t.nextState;
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

function computeCadenceContinuity(transitions) {
  if (transitions.length < 2) return 1.0;
  let reversals = 0;
  let starts = 0;
  for (const t of transitions) {
    const intent = (t.raw && t.raw.intent) || '';
    if (intent === 'CADENCE_TICK' || intent === 'WORKER_METRICS_REPORTED') starts++;
    const from = t.previousState;
    const to = t.nextState;
    if (from !== to) reversals++;
  }
  if (starts === 0) return 1.0;
  const ratio = reversals / (starts || 1);
  return Math.max(0, Math.min(1, 1 - ratio));
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

module.exports = { synthesize, computeConfidence, computeIntegrityScore, PROJECTION_VERSION };
