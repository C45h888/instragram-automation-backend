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
const cadence = require('./runtime/cadence');
const lifecycle = require('./runtime/lifecycle');
const signalIntake = require('./runtime/signal-intake');
const syncSubstrate = require('../substrates/sync-substrate');
const engagementTelemetryAdapter = require('./governance/interpreters/engagement-telemetry-adapter');
const telemetryWorkers = require('./telemetry-workers');
const transitionWriters = require('./telemetry-workers/transition-writers');

const ingressSubstrate = require('./governance/ingress-consistency/substrate');
const namespaceProjectionInterpreter = require('./governance/interpreters/namespace-projection-interpreter');
const parsing = require('../acquisition-kernel/parsing');
const retryCadence = require('../retry-cadence-kernel/index');
const dbWriters = require('../substrates/db/writers');
const dbReaders = require('../substrates/db/readers');
const cognitionScanner = require('../substrates/db/cognition-scanner');
const orphanMessageRepair = require('../substrates/reconciliation/orphan-message-repair');

// ── 8 Domain FSMs ───────────────────────────────────────────────────────────
const acquisitionFsm = require('../acquisition-kernel/fsm');
const publishingFsm = require('../publishing-kernel/fsm');
const graphCapabilityFsm = require('../graph-capability-kernel/fsm');
const schedulingFsm = require('./governance/domains/scheduling-fsm');
const dedupFsm = require('../dedup-kernel/fsm');
const engagementFsm = require('../retry-cadence-kernel/fsm');
const reconciliationFsm = require('../reconciliation-kernel/fsm');
const telemetryCoordinationFsm = require('./governance/domains/telemetry-coordination-fsm');
const persistTelemetryFsm = require('./governance/domains/persist-telemetry-fsm');

// ── 6 Membrane orchestrators ─────────────────────────────────────────────────
const cadenceOrchestrator     = require('./orchestration/cadence-orchestrator');
const acquisitionOrchestrator = require('../acquisition-kernel/orchestrator');
const emissionOrchestrator    = require('../publishing-kernel/orchestrator');
const lifecycleOrchestrator   = require('./orchestration/lifecycle-orchestrator');
const degradationOrchestrator = require('./orchestration/degradation-orchestrator');

const REFRESH_INTERVAL_MS = 90 * 1000; // 90s cadence
const RECONCILIATION_INTERVAL_MS = 60 * 1000; // 60s reconciliation cadence — separate from maintenance
const GOVERNANCE_TICK_MS = 10_000; // 10s watchdog tick

// ── Wiring ───────────────────────────────────────────────────────────────────

function _wire() {
  // Register domain FSMs — must happen before wiring membranes
  constitutional.registerDomain(acquisitionFsm);
  constitutional.registerDomain(publishingFsm);
  constitutional.registerDomain(graphCapabilityFsm);
  constitutional.registerDomain(schedulingFsm);
  constitutional.registerDomain(dedupFsm);
  constitutional.registerDomain(engagementFsm);
  constitutional.registerDomain(reconciliationFsm);
  constitutional.registerDomain(telemetryCoordinationFsm);
  constitutional.registerDomain(persistTelemetryFsm);

  // Wire execution bridge's governance reference for observation emission
  executionBridge.setGovernance(constitutional);

  // Wire each membrane orchestrator
  cadenceOrchestrator.wire(constitutional);
  acquisitionOrchestrator.wire(constitutional, acquisitionFsm);
  emissionOrchestrator.wire(constitutional);
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
  // These produce PROJECTION_INTENT entries to domain-bounded transition log partitions.
  await telemetryWorkers.startAll();

  // Start the 5 transition writers — event-driven, bounded by namespace.
  // Each writer subscribes to observability.onWrite() and filters for:
  //   raw.entryType === 'SEMANTIC_PROJECTION_TRANSITION' AND domain === <namespace>
  // They append FSM-coordinated output to the canonical ledger and notify CK.
  // Writers read from the domain-bounded transition log partition (not the global log).
  transitionWriters.startAll();

  // Transition-writers are the sole write path.

  // Phase 3: Wire namespace projection interpreter to CK.
  // Subscribes to PROJECTION_ACCEPTED actions emitted by FSM after async validation.
  constitutional.subscribeAction('PROJECTION_ACCEPTED', (action) => {
    namespaceProjectionInterpreter.interpret(action);
  });

  // Start the engagement telemetry adapter — bounded raw telemetry normalizer.
  // Emits RAW_METRICS_WINDOW, RAW_QUOTA_WINDOW, RAW_RATE_LIMIT_WINDOW to observability.
  // All semantic synthesis (RETRY_PRESSURE, QUOTA_PRESSURE, etc.) is done by projection workers.
  await engagementTelemetryAdapter.start();

  // Wire parsing substrate to CK — workers emit PARSING_COMPLETE events on completion
  parsing.setGovernance(constitutional);

  // Wire retry-cadence substrate to CK — workers emit OBSERVATION + RETRY_EXHAUSTED events
  retryCadence.setGovernance(constitutional);

  // Wire DB writers substrate to CK — workers emit DB_WRITE_COMPLETE on Supabase upsert
  dbWriters.setGovernance(constitutional);

  // Wire DB readers substrate to CK — emit DB_READ_OBSERVED on every read
  dbReaders.setGovernance(constitutional);

  // Start orphan message repair — subscribes to DB_WRITE_COMPLETE on instagram_dm_messages
  // Runs async, idempotent, non-blocking.
  orphanMessageRepair.start(constitutional);

  // Rehydrate CK from the worker-populated ledger.
  // Prior entries from a previous process lifetime are now available.
  await constitutional.rehydrate();

  await metricsSubstrate.init();

  // Wire governance into runtime modules — governed reads require this
  lifecycle.setGovernance(constitutional);
  signalIntake.setGovernance(constitutional);

  await lifecycle.refresh();
  const result = await constitutional.governedRead('db.accounts', { query: 'getActiveAccounts' });
  const accounts = result.success ? result.data : [];

  // Start cognition scanner — sole deterministic trigger for publishing FSM
  await cognitionScanner.start(constitutional, accounts, publishingFsm);

  constitutional.dispatch({ type: 'LIFECYCLE_REFRESHED', accountIds: accounts.map(a => a.id) });

  constitutional.dispatch({ type: 'BOOT_COMPLETE' });

  constitutional.startLoop(GOVERNANCE_TICK_MS);

  // Start reactive coordination layer — FSM subscribes to observability onWrite
  // and triggers PROCESS_INTENTS on every PROJECTION_INTENT entry (event-driven).
  // ctx shape matches what dispatch() passes to domain FSMs:
  // { validate, dispatchGlobal, getGlobalState }.
  const ckCtx = {
    validate: (from, to, evt) => constitutional.validateDomainTransition('telemetry-coordination', from, to, evt),
    dispatchGlobal: (evt) => constitutional.dispatch(evt),
    getGlobalState: () => constitutional.getState(),
  };
  telemetryCoordinationFsm.start(ckCtx);

  // Start ingress consistency substrate — monitors log vs ledger lag, signals CK
  // Layer 1: observability plane, sits under CK, not a domain FSM
  ingressSubstrate.start(constitutional.dispatch);

  const redis = getRedisClient();
  if (redis && redis.status === 'ready') {
    syncSubstrate.start(redis, (event) => {
      constitutional.dispatch(event);
    });
  }

  cadence.every(REFRESH_INTERVAL_MS, async () => {
    constitutional.dispatch({ type: 'CADENCE_TICK' });
  });

  // ── Reconciliation is reactive — triggered by LOG_DEGRADED (T1), ESCALATION_SIGNAL (T2),
  //     death (T3), or manual (T5). No periodic cadence.
  //     CK.triggerReconciliation() is called by domain events, not by a timer.
  //     RECONCILIATION_INTERVAL_MS (60s) is deprecated.

  // ── Telemetry coordination: trigger-driven (Phase 3) ──
  // No more timer — CK validates asynchronously via FSM's PROJECTION_PERSISTED handler.
  // Telemetry coordination is purely trigger-driven via the onWrite reactive layer.
  console.log('[orchestrator] Telemetry coordination: trigger-driven via Phase 2 worker + CK async validation');

  const st = await constitutional.status();
  console.log(`[orchestrator] Constitutional kernel running — ${accounts.length} account(s) — global: ${st.state} — domains: ${Object.keys(st.domains).join(', ')}`);
}

async function stopAllWorkers() {
  console.log('[orchestrator] Stopping constitutional kernel...');

  await telemetryWorkers.stopAll();
  transitionWriters.stopAll();
  telemetryCoordinationFsm.stop();
  constitutional.stopLoop();
  syncSubstrate.stop();
  await cognitionScanner.stop();
  await cadence.stop();
  lifecycle.stopAll();

  // Shutdown observability plane — persist final snapshot
  const observability = require('./observability');
  await observability.stop();

  console.log('[orchestrator] Constitutional kernel stopped');
}

module.exports = { startAllWorkers, stopAllWorkers };
