// control-plane/orchastrator.js
// Orchestrator: constitutional composition root.
//
// Owns: wiring constitutional kernel + 6 domain FSMs + 6 membrane orchestrators,
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
const lineageWorker = require('./governance/lineage-worker');
const engagementTelemetryAdapter = require('./governance/interpreters/engagement-telemetry-adapter');

// ── 6 Domain FSMs ───────────────────────────────────────────────────────────
const acquisitionFsm = require('./governance/domains/acquisition-fsm');
const publishingFsm = require('./governance/domains/publishing-fsm');
const schedulingFsm = require('./governance/domains/scheduling-fsm');
const dedupFsm = require('./governance/domains/dedup-fsm');
const engagementFsm = require('./governance/domains/engagement-fsm');
const reconciliationFsm = require('./governance/domains/reconciliation-fsm');

// ── 6 Membrane orchestrators ─────────────────────────────────────────────────
const cadenceOrchestrator     = require('./orchestration/cadence-orchestrator');
const acquisitionOrchestrator = require('./orchestration/acquisition-orchestrator');
const emissionOrchestrator    = require('./orchestration/emission-orchestrator');
const signalOrchestrator      = require('./orchestration/signal-orchestrator');
const lifecycleOrchestrator   = require('./orchestration/lifecycle-orchestrator');
const degradationOrchestrator = require('./orchestration/degradation-orchestrator');

const REFRESH_INTERVAL_MS = 90 * 1000; // 90s cadence
const RECONCILIATION_INTERVAL_MS = 60 * 1000; // 60s reconciliation cadence — separate from maintenance
const DEBOUNCE_MS = 500;
const GOVERNANCE_TICK_MS = 10_000; // 10s watchdog tick

// ── Wiring ───────────────────────────────────────────────────────────────────

function _wire() {
  buffer.setDebounceMs(DEBOUNCE_MS);

  // Register domain FSMs — must happen before wiring membranes
  constitutional.registerDomain(acquisitionFsm);
  constitutional.registerDomain(publishingFsm);
  constitutional.registerDomain(schedulingFsm);
  constitutional.registerDomain(dedupFsm);
  constitutional.registerDomain(engagementFsm);
  constitutional.registerDomain(reconciliationFsm);

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
  console.log('[orchestrator] Starting constitutional kernel with 5 domain FSMs...');
  _wire();

  // Initialize the observability plane before any other subsystem starts
  const observability = require('./observability');
  await observability.init();

  // Start the bounded telemetry projection workers FIRST.
  // These produce SEMANTIC_PROJECTION_TRANSITION entries that the lineage
  // worker consumes. Workers must be running before lineage-worker starts.
  await telemetryWorkers.startAll();

  // Start the lineage worker — canonical runtime interpretation substrate.
  // Consumes from the observability plane and produces immutable lineage
  // entries for the reconciliation engine and governance.
  // MUST start after telemetry workers so projections are available.
  await lineageWorker.start(5000);

  // Start the engagement telemetry adapter — bounded raw telemetry normalizer.
  // Emits RAW_METRICS_WINDOW, RAW_QUOTA_WINDOW, RAW_RATE_LIMIT_WINDOW to observability.
  // All semantic synthesis (RETRY_PRESSURE, QUOTA_PRESSURE, etc.) is done by projection workers.
  await engagementTelemetryAdapter.start();

  // Rehydrate CK from the worker-populated ledger.
  // Prior entries from a previous process lifetime are now available.
  await constitutional.rehydrate();

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

  // ── Reconciliation tick — independent constitutional verification cadence ──
  cadence.every(RECONCILIATION_INTERVAL_MS, () => {
    constitutional.triggerReconciliation();
  });
  console.log(`[orchestrator] Reconciliation loop started — tick every ${RECONCILIATION_INTERVAL_MS / 1000}s`);

  const st = await constitutional.status();
  console.log(`[orchestrator] Constitutional kernel running — ${accounts.length} account(s) — global: ${st.state} — domains: ${Object.keys(st.domains).join(', ')}`);
}

async function stopAllWorkers() {
  console.log('[orchestrator] Stopping constitutional kernel...');

  await telemetryWorkers.stopAll();
  await lineageWorker.stop();
  constitutional.stopLoop();
  syncSubstrate.stop();
  await cadence.stop();
  await signalOrchestrator.stop();
  buffer.destroyAll();
  lifecycle.stopAll();

  // Shutdown observability plane — persist final snapshot
  const observability = require('./observability');
  await observability.stop();

  console.log('[orchestrator] Constitutional kernel stopped');
}

module.exports = { startAllWorkers, stopAllWorkers };
