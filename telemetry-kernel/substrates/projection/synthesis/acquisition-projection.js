// synthesis/acquisition-projection.js
const PROJECTION_VERSION = '1.0.0';
const ACQUISITION_FSM_STATES = Object.freeze(['IDLE', 'ACQUIRING', 'STAGING']);

function synthesize(_projectionState, signals) {
  const { transitions, now } = signals;
  const safeTransitions = Array.isArray(transitions) ? transitions : [];
  const currentState = deriveCurrentState(safeTransitions);
  const intentCount = countEvents(safeTransitions, ['ACQUISITION_INTENT_RECEIVED']);
  const failCount = countEvents(safeTransitions, ['PARSING_FAILED', 'ACQUISITION_DEFER']);

  return {
    currentAcquisitionState: currentState,
    intentCount,
    failureCount: failCount,
    totalTransitions: safeTransitions.length,
    isStale: computeAging(safeTransitions, now) > 300_000,
  };
}

function deriveCurrentState(transitions) {
  if (transitions.length === 0) return 'IDLE';
  for (let i = transitions.length - 1; i >= 0; i--) {
    const t = transitions[i];
    if (t.entity === 'fsm' && ACQUISITION_FSM_STATES.includes(t.nextState)) return t.nextState;
    if (t.domain === 'acquisition' && t.nextState && ACQUISITION_FSM_STATES.includes(t.nextState)) return t.nextState;
  }
  return 'IDLE';
}

function countEvents(transitions, intents) {
  let count = 0;
  for (const t of transitions) {
    const raw = t.raw || {};
    const intent = raw.intent || '';
    if (intents.includes(intent)) count++;
  }
  return count;
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
  if (total === 0) return 0.0; if (total < 3) return 0.3; if (total < 10) return 0.6;
  return 1.0;
}

function computeIntegrityScore(signals) {
  const total = signals && signals.transitions ? signals.transitions.length : 0;
  if (total === 0) return 0.0;
  return Math.min(1.0, total / 10);
}

module.exports = { synthesize, computeConfidence, computeIntegrityScore, PROJECTION_VERSION };
