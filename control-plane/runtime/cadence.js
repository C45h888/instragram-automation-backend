// control-plane/runtime/cadence.js
// Cadence: bounded periodic maintenance loop.
//
// Owns: running a background loop at a fixed interval, stop signalling.
// Does NOT own: worker pool refresh, operational checks — it just calls the callback.
//
// Contract:
//   cadence.every(intervalMs, callback)  → start background loop
//   cadence.stop()                        → stop loop

let _stopping = false;
let _loopPromise = null;
let _intervalMs = null;
let _lastTickAt = null; // timestamp of last tick — for reconciliation engine

function _sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Returns live runtime state. Deterministic, no side effects.
 * @returns {{ state: 'running'|'stopped', intervalMs: number|null }}
 */
function status() {
  return {
    state: _loopPromise ? 'running' : 'stopped',
    intervalMs: _intervalMs,
  };
}

/**
 * Returns the timestamp of the last cadence tick.
 * Used by the reconciliation engine for cadence gap detection.
 * @returns {number|null}
 */
function lastTick() {
  return _lastTickAt;
}

/**
 * Start a periodic background loop. Callback is awaited each cycle.
 * Idempotent — calling on an already-running loop logs a warning and is a no-op.
 *
 * @param {number} intervalMs — milliseconds between cycles, must be > 0
 * @param {Function} callback — async () => void, errors are caught and logged
 * @throws {Error} if intervalMs is not a positive number or callback is not a function
 */
function every(intervalMs, callback) {
  if (typeof intervalMs !== 'number' || intervalMs <= 0) {
    throw new Error(`[cadence] intervalMs must be > 0, got ${intervalMs}`);
  }
  if (typeof callback !== 'function') {
    throw new Error(`[cadence] callback must be a function, got ${typeof callback}`);
  }
  if (_loopPromise) {
    console.warn('[cadence] Loop already running — ignoring duplicate start');
    return;
  }
  _stopping = false;
  _intervalMs = intervalMs;
  console.log(`[cadence] Started — running every ${intervalMs}ms`);

  _loopPromise = (async () => {
    while (!_stopping) {
      await _sleep(intervalMs);
      if (_stopping) break;
      try {
        _lastTickAt = Date.now();
        // Observability: cadence tick transition
        _emitTransition('TICKING');
        await callback();
        // Observability: cadence return to idle after tick complete
        _emitTransition('IDLE');
      } catch (err) {
        console.error('[cadence] Cycle error:', err.message);
      }
    }
    _loopPromise = null;
    console.log('[cadence] Stopped');
  })();
}

/**
 * Stop the background loop. Returns once the current cycle completes.
 * Idempotent — calling on an already-stopped loop is a safe no-op.
 * Awaitable — resolves when loop has fully exited.
 */
async function stop() {
  // Observability: cadence stopped transition
  _emitTransition('STOPPED');
  _stopping = true;
  if (_loopPromise) {
    await _loopPromise;
  }
  _intervalMs = null;
}

/**
 * Emit observability transition for cadence state changes.
 */
function _emitTransition(nextState) {
  try {
    const observability = require('../observability/emitters/transition-emitter');
    const previousState = _loopPromise ? 'TICKING' : 'IDLE';
    observability.transition({
      domain: 'cadence',
      entity: 'cadence',
      entityId: 'cadence',
      previousState,
      nextState,
      authority: 'cadence-runtime',
      raw: { intervalMs: _intervalMs },
    });
  } catch (err) {
    console.warn('[cadence] Observability transition error:', err.message);
  }
}

module.exports = { every, stop, status, lastTick };
