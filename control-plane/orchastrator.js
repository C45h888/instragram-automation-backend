// control-plane/orchastrator.js
// Orchestrator: deterministic composition root.
//
// Owns: wiring runtime modules together, routing governance authority
//        downward and runtime signals upward.
// Does NOT own: mechanics of any individual module — delegates everything.
// Does NOT own: governance legality — delegates to governance-kernel.
//
// Architecture (invariant: signals ↑, authority ↓):
//
//   ┌─ governance loop (10s tick) ───────────────────────────────────────┐
//   │  watchdog: detects stale states → auto-transitions to DEGRADED/RECOVERY │
//   └────────────────────────────────────────────────────────────────────┘
//
//   signalIntake ──► buffer.ingest()  +  governance.dispatch(BUFFER_EVENT_INGESTED)
//   buffer.onFlush ──► governance.dispatch(BUFFER_FLUSH_READY)
//                          │
//                          ├── HSM transition  (BUFFERING → EVALUATING)
//                          └── onAction(EVALUATE) → executeEvaluationPipeline()
//                               ├── evaluator.evaluate()
//                               ├── emitter.emitMutation()
//                               └── emitter.emit()
//                                    └── governance.dispatch(EMISSION_COMPLETE)
//
//   cadence.every(3min)
//     ├─► workerPool.refresh()  ──► buffer.destroy(removed)
//     ├─► safety.runChecks()
//     └─► executionBridge.getMetrics()
//           ├── failureRate > 50%  → governance.dispatch(RETRY_PRESSURE_DETECTED)
//           └── healthy + degraded → governance.dispatch(PRESSURE_CLEARED)
//
// This module is the SINGLE place where modules are wired together.
// No module imports another module directly — all wiring lives here.

const governance = require('./governance/governance-kernel');
const executionBridge = require('./execution-bridge');
const signalIntake = require('./runtime/signal-intake');
const buffer = require('./runtime/buffer');
const cadence = require('./runtime/cadence');
const evaluator = require('./runtime/evaluation');
const emitter = require('./runtime/emission');
const workerPool = require('./runtime/lifecycle');
const safety = require('./runtime/operational-safety');

const REFRESH_INTERVAL_MS = 3 * 60 * 1000; // 3 min
const DEBOUNCE_MS = 500;
const GOVERNANCE_TICK_MS = 10_000; // 10s watchdog tick

// ── Helpers ──────────────────────────────────────────────────────────────────

function isDegraded() {
  const state = governance.getState();
  return state.startsWith('DEGRADED.');
}

// ── Wiring ───────────────────────────────────────────────────────────────────

/**
 * Execute the evaluation → mutation → emission pipeline for a single account.
 * Called by the governance action subscriber when a EVALUATE action is emitted.
 * After execution, reports completion back to governance.
 */
async function executeEvaluationPipeline(accountId, events) {
  try {
    const result = evaluator.evaluate(accountId, events);

    // Apply mutations first (mark_failed rows)
    for (const mut of result.mutations) {
      await emitter.emitMutation(mut);
    }

    // Emit intents to Redis for workers to consume
    if (result.intents.length > 0) {
      const emitResult = await emitter.emit(result.intents);
      if (emitResult.ok) {
        governance.dispatch({ type: 'EMISSION_COMPLETE', accountId, intentCount: result.intents.length });
      } else {
        governance.dispatch({ type: 'EMISSION_FAILED', accountId, reason: emitResult.error });
      }
    } else {
      governance.dispatch({ type: 'EVALUATION_EMPTY', accountId, intentCount: 0 });
    }
  } catch (err) {
    console.error(`[orchestrator] Evaluation pipeline error for ${accountId}:`, err.message);
    governance.dispatch({ type: 'EMISSION_FAILED', accountId, reason: err.message });
  }
}

/**
 * Wire all runtime modules together. Called once on startup.
 *
 * Architecture: runtime signals flow UPWARD (dispatch), governance authority
 * flows DOWNWARD (onAction). The orchestrator is the sole routing fabric.
 */
function _wire() {
  // Buffer debounce window
  buffer.setDebounceMs(DEBOUNCE_MS);

  // Governance action subscriber — routes governance intents to runtime modules.
  // The governance kernel emits WHAT should happen; the orchestrator executes HOW.
  governance.onAction((action) => {
    if (action.type === 'EVALUATE') {
      executeEvaluationPipeline(action.accountId, action.events);
    }
    if (action.type === 'LOG_DEGRADED') {
      console.warn(`[orchestrator] Runtime DEGRADED.${action.substate}: ${action.reason}`);
    }
    if (action.type === 'LOG_RECOVERY') {
      console.warn(`[orchestrator] Runtime RECOVERY.${action.substate}`);
    }
    if (action.type === 'LOG_HALT') {
      console.error(`[orchestrator] Runtime HALTED: ${action.reason}`);
    }
  });

  // Buffer flush → dispatch upward to governance kernel.
  // The kernel decides whether EVALUATING is legal given current runtime state.
  buffer.onFlush(async (accountId, events) => {
    governance.dispatch({ type: 'BUFFER_FLUSH_READY', accountId, events, eventCount: events.length });
  });

  // Worker pool removes account → cleanup buffer
  workerPool.onRemove((accountId) => {
    buffer.destroy(accountId);
  });
}

// ── Public API ───────────────────────────────────────────────────────────────

async function startAllWorkers() {
  console.log('[orchestrator] Starting governance kernel...');
  _wire();

  // 1. Signal intake: subscribe to realtime → signal bus.
  //    Each event is ingested into buffer AND dispatched upward to governance.
  await signalIntake.start(null, (event) => {
    buffer.ingest(event);
    governance.dispatch({ type: 'BUFFER_EVENT_INGESTED', accountId: event.accountId });
  });

  // 2. Worker pool: discover accounts and spawn workers
  await workerPool.refresh();

  // 3. Boot complete — governance transitions BOOTING → HEALTHY.IDLE
  governance.dispatch({ type: 'BOOT_COMPLETE' });

  // 4. Start governance watchdog loop (10s tick — detects stale states)
  governance.startLoop(GOVERNANCE_TICK_MS);

  // 5. Cadence: 3-minute maintenance loop
  cadence.every(REFRESH_INTERVAL_MS, async () => {
    await workerPool.refresh();
    await safety.runChecks();

    // Report worker execution health into governance.
    // If failure rate > 50% with at least 5 samples, escalate to degraded.
    const metrics = executionBridge.getMetrics();
    if (!metrics.isHealthy && metrics.total >= 5) {
      governance.dispatch({
        type: 'RETRY_PRESSURE_DETECTED',
        reason: `Worker failure rate ${Math.round(metrics.failureRate * 100)}% (${metrics.failed}/${metrics.total} in ${metrics.windowMs / 1000}s)`,
      });
    } else if (metrics.isHealthy && isDegraded()) {
      governance.dispatch({ type: 'PRESSURE_CLEARED' });
    }
  });

  console.log(`[orchestrator] Governance kernel running — ${workerPool.size()} workers across all accounts — state: ${governance.getState()}`);
}

async function stopAllWorkers() {
  console.log('[orchestrator] Stopping governance kernel...');

  // Stop governance watchdog loop first
  governance.stopLoop();

  // Stop cadence loop so no new operations are scheduled
  await cadence.stop();

  // Stop signal intake (realtime + signal bus)
  await signalIntake.stop();

  // Clear all buffered events and pending debounce timers
  buffer.destroyAll();

  // Stop all domain workers
  workerPool.stopAll();

  console.log('[orchestrator] Governance kernel stopped');
}

module.exports = { startAllWorkers, stopAllWorkers };
