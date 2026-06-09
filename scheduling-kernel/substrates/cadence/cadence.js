// scheduling-kernel/substrates/cadence/cadence.js
// Cadence: bounded periodic maintenance loop.
// Kernelized from: control-plane/runtime/cadence.js
//
// Owns: running a background loop at a fixed interval, stop signalling.
// Does NOT own: worker pool refresh, operational checks — it just calls the callback.
//
// Constitutional routing: all lifecycle events (start/stop/complete/failed) flow
// through CK.dispatch(). No direct observability writes. The callback (tick action)
// is caller-provided and expected to dispatch CADENCE_TICK through CK.
//
// Contract:
//   cadence.setGovernance(fn)           → wire CK dispatch function
//   cadence.every(intervalMs, callback) → start background loop
//   cadence.stop()                      → stop loop

let _stopping = false;
let _loopPromise = null;
let _intervalMs = null;
let _lastTickAt = null; // timestamp of last tick — for reconciliation engine

// ── Governance dispatch (set by CK at boot) ────────────────────────────
let _governanceDispatch = null;

function setGovernance(dispatchFn) {
  if (typeof dispatchFn === 'function') {
    _governanceDispatch = dispatchFn;
  }
}

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
 * Dispatch a lifecycle event through CK if wired.
 * Falls back to console log if governance not wired (e.g. tests).
 */
function _dispatchLifecycle(type, details = {}) {
  if (_governanceDispatch) {
    try {
      _governanceDispatch({ type, ...details });
    } catch (err) {
      console.warn(`[cadence] Governance dispatch failed for ${type}:`, err.message);
    }
  } else {
    console.log(`[cadence] ${type} (no governance wired)`);
  }
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
  _dispatchLifecycle('CADENCE_LOOP_STARTED', { intervalMs });

  _loopPromise = (async () => {
    while (!_stopping) {
      await _sleep(intervalMs);
      if (_stopping) break;
      try {
        _lastTickAt = Date.now();
        await callback();
        _dispatchLifecycle('CADENCE_CYCLE_COMPLETED', { tickAt: _lastTickAt });
      } catch (err) {
        console.error('[cadence] Cycle error:', err.message);
        _dispatchLifecycle('CADENCE_CYCLE_FAILED', {
          tickAt: _lastTickAt,
          error: err.message,
        });
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
  _dispatchLifecycle('CADENCE_LOOP_STOPPED', { intervalMs: _intervalMs });
  _stopping = true;
  if (_loopPromise) {
    await _loopPromise;
  }
  _intervalMs = null;
}

module.exports = { setGovernance, every, stop, status, lastTick };