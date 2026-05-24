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

function _sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Start a periodic background loop. Callback is awaited each cycle.
 * @param {number} intervalMs — milliseconds between cycles
 * @param {Function} callback — async () => void
 */
function every(intervalMs, callback) {
  if (_loopPromise) {
    console.warn('[cadence] Loop already running — ignoring duplicate start');
    return;
  }
  _stopping = false;
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
 */
async function stop() {
  _stopping = true;
  if (_loopPromise) {
    await _loopPromise;
  }
}

module.exports = { every, stop };
