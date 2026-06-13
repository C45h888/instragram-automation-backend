// telemetry-kernel/substrates/projection/inputs/persist-telemetry-input.js
// Persist-Telemetry Projection Input: reads the postgres-telemetry FSM and
// DB_PERSIST_FAILURE transition log slice from observability, derives a
// window of signals for synthesis.
//
// Emission sources:
//   1. postgres-telemetry-kernel/fsm.js — observability.transition({
//        domain: 'persist-telemetry', entity: 'fsm', previousState, nextState,
//        raw: { intent, table, inFlight }
//      })
//   2. retry-cadence-kernel/fsm.js — DB_PERSIST_FAILURE / DB_PERSIST_FAILURE_READ
//      handlers emit with domain: domain || 'persist-telemetry', carrying
//      failure analysis (category, severity, recommendations)
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

  // Read ALL recent transitions for domain='persist-telemetry'.
  // Covers both FSM state changes and DB_PERSIST_FAILURE events.
  // Falls back to empty list if observability is unavailable.
  let transitions = [];
  if (observability && observability.query && typeof observability.query.getTransitionLog === 'function') {
    try {
      const log = observability.query.getTransitionLog('persist-telemetry', null, windowSize);
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
    source: 'observability.transitionLog[persist-telemetry]',
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
