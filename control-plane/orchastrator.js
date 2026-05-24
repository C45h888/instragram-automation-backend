// control-plane/lifecycle.js
// Orchestrator: deterministic governance kernel.
//
// Owns: wiring runtime modules together into a coherent governance loop.
// Does NOT own: mechanics of any individual module — delegates everything.
//
// Architecture:
//   signalIntake  ──► buffer.ingest()
//   buffer.onFlush ──► evaluator.evaluate() ──► emitter.emit() + emitter.emitMutation()
//
//   cadence.every(3min)
//     ├─► workerPool.refresh()  ──► buffer.destroy(removed)
//     └─► safety.runChecks()
//
// This module is the SINGLE place where modules are wired together.
// No module imports another module directly — all wiring lives here.

const signalIntake = require('./runtime/signal-intake');
const buffer = require('./runtime/buffer');
const cadence = require('./runtime/cadence');
const evaluator = require('./runtime/evaluation');
const emitter = require('./runtime/emission');
const workerPool = require('./runtime/lifecycle');
const safety = require('./runtime/operational-safety');

const REFRESH_INTERVAL_MS = 3 * 60 * 1000; // 3 min
const DEBOUNCE_MS = 500;

// ── Wiring ───────────────────────────────────────────────────────────────────

/**
 * Wire all runtime modules together. Called once on startup.
 */
function _wire() {
  // Buffer debounce window
  buffer.setDebounceMs(DEBOUNCE_MS);

  // Event → evaluate → emit pipeline
  buffer.onFlush(async (accountId, events) => {
    const result = evaluator.evaluate(accountId, events);

    // Apply mutations first (mark_failed rows)
    for (const mut of result.mutations) {
      await emitter.emitMutation(mut);
    }

    // Emit intents to Redis for workers to consume
    if (result.intents.length > 0) {
      await emitter.emit(result.intents);
    }
  });

  // Worker pool removes account → cleanup buffer
  workerPool.onRemove((accountId) => {
    buffer.destroy(accountId);
  });
}

// ── Public API ───────────────────────────────────────────────────────────────

async function startAllWorkers() {
  console.log('[lifecycle] Starting governance kernel...');
  _wire();

  // 1. Signal intake: subscribe to realtime → signal bus → buffer.ingest
  await signalIntake.start(null, buffer.ingest);

  // 2. Worker pool: discover accounts and spawn workers
  await workerPool.refresh();

  // 3. Cadence: 3-minute maintenance loop
  cadence.every(REFRESH_INTERVAL_MS, async () => {
    await workerPool.refresh();
    await safety.runChecks();
  });

  console.log(`[lifecycle] Governance kernel running — ${workerPool.size()} workers across all accounts`);
}

async function stopAllWorkers() {
  console.log('[lifecycle] Stopping governance kernel...');

  // Stop cadence loop first so no new operations are scheduled
  await cadence.stop();

  // Stop signal intake (realtime + signal bus)
  await signalIntake.stop();

  // Clear all buffered events and pending debounce timers
  buffer.destroyAll();

  // Stop all domain workers
  workerPool.stopAll();

  console.log('[lifecycle] Governance kernel stopped');
}

module.exports = { startAllWorkers, stopAllWorkers };
