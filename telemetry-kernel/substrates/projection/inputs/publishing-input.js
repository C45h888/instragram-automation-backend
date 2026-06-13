// inputs/publishing-input.js
const DEFAULT_WINDOW_SIZE = 50;
const DEFAULT_POLL_INTERVAL_MS = 30_000;
async function getNormalizedInputWindow({ pollIntervalMs = DEFAULT_POLL_INTERVAL_MS, tickCount = 0, windowSize = DEFAULT_WINDOW_SIZE } = {}) {
  const observability = readObservability();
  const now = Date.now();
  let transitions = [];
  if (observability && observability.query && typeof observability.query.getTransitionLog === 'function') {
    try { const log = observability.query.getTransitionLog('publishing', null, windowSize); transitions = Array.isArray(log) ? log : []; }
    catch (_) { transitions = []; }
  }
  return { transitions, now, tickCount, windowOpenedAt: now - pollIntervalMs, windowClosedAt: now, entryCount: transitions.length, noiseGate: transitions.length < 3, source: 'observability.transitionLog[publishing]' };
}
function readObservability() { try { return require('../../../../control-plane/observability'); } catch (_) { return null; } }
module.exports = { getNormalizedInputWindow, DEFAULT_WINDOW_SIZE, DEFAULT_POLL_INTERVAL_MS };
