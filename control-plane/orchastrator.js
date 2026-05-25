// control-plane/orchastrator.js
// Orchestrator: constitutional composition root.
//
// Owns: wiring 6 constitutional orchestrators to governance,
//        boot/shutdown sequencing.
// Does NOT own: domain semantics, execution intelligence, governance policy,
//               retry decisions, degradation logic, signal interpretation.
//
// Architecture (invariant: signals ↑, authority ↓):
//   Governance kernel → subscribeAction(type, handler)
//   6 constitutional orchestrators each subscribe to their action types
//   Each orchestrator is a THIN MEMBRANE — routes mechanically, never interprets
//
// Constitutional purity: this composition root is ~60 lines of wiring.
// All execution intelligence lives in governance + domain registry.
// All coordination lives in the 6 bounded orchestrators.
// This file only wires them together and sequences boot/shutdown.

const governance = require('./governance/governance-kernel');
const executionBridge = require('./execution-bridge');
const metricsSubstrate = require('../substrates/metrics-substrate');
const { getRedisClient } = require('../config/redis');
const buffer = require('./runtime/buffer');
const cadence = require('./runtime/cadence');
const lifecycle = require('./runtime/lifecycle');
const persistence = require('../substrates/persistence');
const syncSubstrate = require('../substrates/sync-substrate');

// ── 6 Constitutional orchestrators ───────────────────────────────────────────
const cadenceOrchestrator     = require('./orchestration/cadence-orchestrator');
const acquisitionOrchestrator = require('./orchestration/acquisition-orchestrator');
const emissionOrchestrator    = require('./orchestration/emission-orchestrator');
const signalOrchestrator      = require('./orchestration/signal-orchestrator');
const lifecycleOrchestrator   = require('./orchestration/lifecycle-orchestrator');
const degradationOrchestrator = require('./orchestration/degradation-orchestrator');

const REFRESH_INTERVAL_MS = 90 * 1000; // 90s cadence
const DEBOUNCE_MS = 500;
const GOVERNANCE_TICK_MS = 10_000; // 10s watchdog tick

// ── Wiring ───────────────────────────────────────────────────────────────────

/**
 * Wire all 6 constitutional orchestrators to the governance kernel.
 * Each orchestrator subscribes to its own action types via subscribeAction().
 * Called once on startup.
 */
function _wire() {
  buffer.setDebounceMs(DEBOUNCE_MS);

  // Wire execution bridge's governance reference for observation emission
  executionBridge.setGovernance(governance);

  // Wire each constitutional orchestrator
  cadenceOrchestrator.wire(governance);
  acquisitionOrchestrator.wire(governance);
  emissionOrchestrator.wire(governance);
  signalOrchestrator.wire(governance);
  lifecycleOrchestrator.wire(governance);
  degradationOrchestrator.wire(governance);
}

// ── Public API ───────────────────────────────────────────────────────────────

async function startAllWorkers() {
  console.log('[orchestrator] Starting governance kernel with 6 constitutional orchestrators...');
  _wire();

  // Metrics substrate: rehydrate from Redis for crash-survival
  await metricsSubstrate.init();

  // Signal intake: subscribe to realtime → signal bus
  await signalOrchestrator.start(governance);

  // Initial account discovery → dispatch to governance
  await lifecycle.refresh();
  const accounts = await persistence.getActiveAccounts();
  governance.dispatch({ type: 'LIFECYCLE_REFRESHED', accountIds: accounts.map(a => a.id) });

  // Boot complete → governance transitions BOOTING → HEALTHY.IDLE
  governance.dispatch({ type: 'BOOT_COMPLETE' });

  // Start governance watchdog loop (10s tick — stale state detection only)
  governance.startLoop(GOVERNANCE_TICK_MS);

  // Sync substrate starts polling when kernel is HEALTHY.IDLE
  const redis = getRedisClient();
  if (redis && redis.status === 'ready') {
    syncSubstrate.start(redis, governance);
  }

  // Cadence: 90-second maintenance loop → dumb signal only
  cadence.every(REFRESH_INTERVAL_MS, async () => {
    governance.dispatch({ type: 'CADENCE_TICK' });
  });

  console.log(`[orchestrator] Governance kernel running — ${accounts.length} account(s) — state: ${governance.getState()}`);
}

async function stopAllWorkers() {
  console.log('[orchestrator] Stopping governance kernel...');

  governance.stopLoop();
  syncSubstrate.stop();
  await cadence.stop();
  await signalOrchestrator.stop();
  buffer.destroyAll();
  lifecycle.stopAll();

  console.log('[orchestrator] Governance kernel stopped');
}

module.exports = { startAllWorkers, stopAllWorkers };
