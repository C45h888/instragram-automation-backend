// control-plane/orchastrator.js
// Orchestrator: constitutional composition root.
//
// Owns: wiring constitutional kernel + 3 domain FSMs + 6 membrane orchestrators,
//        boot/shutdown sequencing.
// Does NOT own: domain semantics, execution intelligence, governance policy,
//               retry decisions, degradation logic, signal interpretation.
//
// Architecture (invariant: signals ↑, authority ↓):
//   Constitutional kernel → subscribeAction(type, handler)
//   3 domain FSMs are registered with the constitutional kernel
//   6 membrane orchestrators each subscribe to their action types
//   Each orchestrator is a THIN MEMBRANE — routes mechanically, never interprets
//
// This is the SINGLE place where modules are wired together.
// No module imports another module directly — all wiring lives here.

const constitutional = require('./governance/constitutional-kernel');
const executionBridge = require('./execution-bridge');
const metricsSubstrate = require('../substrates/metrics-substrate');
const { getRedisClient } = require('../config/redis');
const buffer = require('./runtime/buffer');
const cadence = require('./runtime/cadence');
const lifecycle = require('./runtime/lifecycle');
const persistence = require('../substrates/persistence');
const syncSubstrate = require('../substrates/sync-substrate');

// ── 3 Domain FSMs ───────────────────────────────────────────────────────────
const acquisitionFsm = require('./governance/domains/acquisition-fsm');
const publishingFsm = require('./governance/domains/publishing-fsm');
const schedulingFsm = require('./governance/domains/scheduling-fsm');

// ── 6 Membrane orchestrators ─────────────────────────────────────────────────
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

function _wire() {
  buffer.setDebounceMs(DEBOUNCE_MS);

  // Register domain FSMs — must happen before wiring membranes
  constitutional.registerDomain(acquisitionFsm);
  constitutional.registerDomain(publishingFsm);
  constitutional.registerDomain(schedulingFsm);

  // Wire execution bridge's governance reference for observation emission
  executionBridge.setGovernance(constitutional);

  // Wire each membrane orchestrator
  cadenceOrchestrator.wire(constitutional);
  acquisitionOrchestrator.wire(constitutional, acquisitionFsm);
  emissionOrchestrator.wire(constitutional);
  signalOrchestrator.wire(constitutional);
  lifecycleOrchestrator.wire(constitutional);
  degradationOrchestrator.wire(constitutional);
}

// ── Public API ───────────────────────────────────────────────────────────────

async function startAllWorkers() {
  console.log('[orchestrator] Starting constitutional kernel with 3 domain FSMs...');
  _wire();

  await metricsSubstrate.init();

  await signalOrchestrator.start(constitutional);

  await lifecycle.refresh();
  const accounts = await persistence.getActiveAccounts();
  constitutional.dispatch({ type: 'LIFECYCLE_REFRESHED', accountIds: accounts.map(a => a.id) });

  constitutional.dispatch({ type: 'BOOT_COMPLETE' });

  constitutional.startLoop(GOVERNANCE_TICK_MS);

  const redis = getRedisClient();
  if (redis && redis.status === 'ready') {
    syncSubstrate.start(redis, (event) => {
      constitutional.dispatch(event);
    });
  }

  cadence.every(REFRESH_INTERVAL_MS, async () => {
    constitutional.dispatch({ type: 'CADENCE_TICK' });
  });

  const st = constitutional.status();
  console.log(`[orchestrator] Constitutional kernel running — ${accounts.length} account(s) — global: ${st.state} — domains: ${Object.keys(st.domains).join(', ')}`);
}

async function stopAllWorkers() {
  console.log('[orchestrator] Stopping constitutional kernel...');

  constitutional.stopLoop();
  syncSubstrate.stop();
  await cadence.stop();
  await signalOrchestrator.stop();
  buffer.destroyAll();
  lifecycle.stopAll();

  console.log('[orchestrator] Constitutional kernel stopped');
}

module.exports = { startAllWorkers, stopAllWorkers };
