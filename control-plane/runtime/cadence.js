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
        await callback();
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
  _stopping = true;
  if (_loopPromise) {
    await _loopPromise;
  }
  _intervalMs = null;
}

module.exports = { every, stop, status };
