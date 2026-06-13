// telemetry-kernel/substrates/projection/inputs/reconciliation-input.js
// Reconciliation Projection Input: reads the reconciliation FSM transition
// log slice from observability, derives a window of signals for synthesis.
//
// Emission sources:
//   1. reconciliation-kernel/fsm.js — obs.transition({
//        domain: 'reconciliation', entity: 'fsm', previousState, nextState,
//        raw: { intent, epochCount, driftCounters, escalationSignaled }
//      })
//   2. reconciliation-kernel/fsm.js — substrate state transitions with same domain
//
// This input is read-only. It returns a window object that the synthesis
// stage consumes.

const DEFAULT_WINDOW_SIZE = 50;
const DEFAULT_POLL_INTERVAL_MS = 30_000;

async function getNormalizedInputWindow({
  pollIntervalMs = DEFAULT_POLL_INTERVAL_MS,
  tickCount = 0,
  windowSize = DEFAULT_WINDOW_SIZE,
} = {}) {
  const observability = readObservability();
  const now = Date.now();

  let transitions = [];
  if (observability && observability.query && typeof observability.query.getTransitionLog === 'function') {
    try {
      const log = observability.query.getTransitionLog('reconciliation', null, windowSize);
      transitions = Array.isArray(log) ? log : [];
    } catch (_) {
      transitions = [];
    }
  }

  return {
    transitions,
    now,
    tickCount,
    windowOpenedAt: now - pollIntervalMs,
    windowClosedAt: now,
    entryCount: transitions.length,
    noiseGate: transitions.length < 3,
    source: 'observability.transitionLog[reconciliation]',
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

module.exports = {
  getNormalizedInputWindow,
  DEFAULT_WINDOW_SIZE,
  DEFAULT_POLL_INTERVAL_MS,
};
