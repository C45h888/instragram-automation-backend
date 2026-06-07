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
const substrateRegistry = require('../acquisition-kernel/substrate-registry');
const metricsSubstrate = require('../substrates/metrics-substrate');
const { getRedisClient } = require('../config/redis');
const cadence = require('../scheduling-kernel/substrates/cadence/cadence');
const lifecycle = require('../scheduling-kernel/substrates/cadence/lifecycle');
const signalIntake = require('./runtime/signal-intake');
const syncSubstrate = require('../substrates/sync-substrate');
const engagementTelemetryAdapter = require('./governance/interpreters/engagement-telemetry-adapter');
const telemetryKernel = require('../telemetry-kernel');
const telemetryWorkers = telemetryKernel; // kernel owns canonical startAll/stopAll
const transitionWriters = require('../telemetry-kernel/substrates/projection/transition-writers');

const ingressSubstrate = require('./governance/ingress-consistency/substrate');
const namespaceProjectionInterpreter = require('./governance/interpreters/namespace-projection-interpreter');
const parsing = require('../acquisition-kernel/substrates/parsing-substrate');
const retryCadence = require('../retry-cadence-kernel/index');
const dbWriters = require('../postgres-telemetry-kernel/writers');
const dbReaders = require('../postgres-telemetry-kernel/readers');
const cognitionScanner = require('../postgres-telemetry-kernel/cognition-scanner');
const orphanMessageRepair = require('../reconciliation-kernel/orphan-message-repair');
const dedupKernel = require('../dedup-kernel');
const dedupOrchestrator = require('../dedup-kernel/orchestrator');

// ── 8 Domain FSMs ───────────────────────────────────────────────────────────
const acquisitionFsm = require('../acquisition-kernel/fsm');
const publishingFsm = require('../publishing-kernel/fsm');
const graphCapabilityFsm = require('../graph-capability-kernel/fsm');
const schedulingFsm = require('../scheduling-kernel/fsm');
const dedupFsm = require('../dedup-kernel/fsm');
const engagementFsm = require('../retry-cadence-kernel/fsm');
const reconciliationFsm = require('../reconciliation-kernel/fsm');
const telemetryCoordinationFsm = telemetryKernel.fsm;
const persistTelemetryFsm = require('../postgres-telemetry-kernel/fsm');

// ── 6 Membrane orchestrators ─────────────────────────────────────────────────
const cadenceOrchestrator     = require('../scheduling-kernel/orchestrator');
const acquisitionOrchestrator = require('../acquisition-kernel/orchestrator');
const emissionOrchestrator    = require('../publishing-kernel/orchestrator');
const lifecycleOrchestrator   = require('./orchestration/lifecycle-orchestrator');
const degradationOrchestrator = require('./orchestration/degradation-orchestrator');

const REFRESH_INTERVAL_MS = 90 * 1000; // 90s cadence
const RECONCILIATION_INTERVAL_MS = 60 * 1000; // 60s reconciliation cadence — separate from maintenance
const GOVERNANCE_TICK_MS = 10_000; // 10s watchdog tick

// ── Wiring ───────────────────────────────────────────────────────────────────

function _wire() {
  // Register domain FSMs — must happen before wiring membranes.
  // The CK ctx.sanityCheck is the universal gate (Item a, this
  // turn) — every FSM gets the gate through its dispatch ctx.
  // No module-level sanity check wiring is needed.
  constitutional.registerDomain(acquisitionFsm);
  constitutional.registerDomain(publishingFsm);
  constitutional.registerDomain(graphCapabilityFsm);
  constitutional.registerDomain(schedulingFsm);
  constitutional.registerDomain(dedupFsm);
  constitutional.registerDomain(engagementFsm);
  constitutional.registerDomain(reconciliationFsm);
  constitutional.registerDomain(telemetryCoordinationFsm);
  constitutional.registerDomain(persistTelemetryFsm);

  // Wire governance refs to all domain FSMs.
  // Each FSM holds the governance ref so it can pass it to
  // workers via execution contexts. Workers receive governance
  // through the FSM — they never import it at module load.
  // Note: acquisitionFsm.setGovernance removed — acquisition-fsm emits
  // through observability plane directly; no governance ref needed.
  publishingFsm.setGovernance(constitutional);
  graphCapabilityFsm.setGovernance(constitutional);
  schedulingFsm.setGovernance(constitutional);
  dedupFsm.setGovernance(constitutional);
  engagementFsm.setGovernance(constitutional);
  reconciliationFsm.setGovernance(constitutional);
  telemetryCoordinationFsm.setGovernance(constitutional);
  persistTelemetryFsm.setGovernance(constitutional);

  // ── Worker registration — canonical FSM→worker bindings ──────────────
  // Each worker is registered with CK so the CTX gate (ctx.invokeWorker)
  // can validate ownership, contract, and sanity before invocation.
  // ── engagement-fsm — retry-cadence workers ────────────────────────────
  constitutional.registerWorker('engagement', 'engagement-retry',
    require('../retry-cadence-kernel/workers/engagement-retry-worker'));
  constitutional.registerWorker('engagement', 'content-retry',
    require('../retry-cadence-kernel/workers/content-retry-worker'));
  constitutional.registerWorker('engagement', 'ugc-retry',
    require('../retry-cadence-kernel/workers/ugc-retry-worker'));
  constitutional.registerWorker('engagement', 'insights-retry',
    require('../retry-cadence-kernel/workers/insights-retry-worker'));
  constitutional.registerWorker('engagement', 'publish-content-retry',
    require('../retry-cadence-kernel/workers/publish-content-retry-worker'));
  constitutional.registerWorker('engagement', 'publish-engagement-retry',
    require('../retry-cadence-kernel/workers/publish-engagement-retry-worker'));
  constitutional.registerWorker('engagement', 'classification',
    require('../retry-cadence-kernel/workers/classification-worker'));
  constitutional.registerWorker('engagement', 'dedup-retry',
    require('../retry-cadence-kernel/workers/dedup-retry-worker'));

  // ── acquisition-fsm — parsing workers ─────────────────────────────────
  constitutional.registerWorker('acquisition', 'comments-parser',
    require('../acquisition-kernel/substrates/parsing-substrate/workers/comments-parser'));
  constitutional.registerWorker('acquisition', 'messages-parser',
    require('../acquisition-kernel/substrates/parsing-substrate/workers/messages-parser'));
  constitutional.registerWorker('acquisition', 'content-parser',
    require('../acquisition-kernel/substrates/parsing-substrate/workers/content-parser'));
  constitutional.registerWorker('acquisition', 'ugc-parser',
    require('../acquisition-kernel/substrates/parsing-substrate/workers/ugc-parser'));
  constitutional.registerWorker('acquisition', 'insights-parser',
    require('../acquisition-kernel/substrates/parsing-substrate/workers/insights-parser'));

  // ── dedup-fsm — dedup workers (bound to substrates/dedup/index.js) ─
  constitutional.registerWorker('dedup', 'check-dedup',
    require('../dedup-kernel/substrates/dedup/workers/check-dedup-worker'));
  constitutional.registerWorker('dedup', 'mark-in-flight',
    require('../dedup-kernel/substrates/dedup/workers/mark-in-flight-worker'));
  constitutional.registerWorker('dedup', 'clear-tick',
    require('../dedup-kernel/substrates/dedup/workers/clear-tick-worker'));

  // Wire each membrane orchestrator
  cadenceOrchestrator.wire(constitutional);
  acquisitionOrchestrator.wire(constitutional, acquisitionFsm);
  emissionOrchestrator.wire(constitutional);
  lifecycleOrchestrator.wire(constitutional);
  degradationOrchestrator.wire(constitutional);
  dedupOrchestrator.wire(constitutional, dedupFsm);
}

// ── Public API ───────────────────────────────────────────────────────────────

async function startAllWorkers() {
  console.log('[orchestrator] Starting constitutional kernel with 5 domain FSMs...');
  _wire();

  // Boot-time validation: every domain in DOMAIN_REGISTRY has a binding
  // in every worker map (parsing, retry, classification). Catches
  // registry drift at boot, not at runtime. Throws on failure.
  substrateRegistry.validate();

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

  // Start conversation repair substrate — inside dedup-kernel, subscribes to
  // EXECUTE_CONVERSATION_REPAIR. Worker fetches from Graph API, upserts
  // through canonical writers, fixes orphaned message conversation_ids.
  dedupKernel.conversationRepair.start(constitutional);

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

  // Start acquisition-fsm timeout sweeper — force-closes intents that exceed
  // intentTimeoutMs. Sweeper runs as a setInterval, owned by the orchastrator
  // lifecycle. The FSM owns the policy (thresholds); the orchastrator owns
  // the lifecycle (start/stop). Stop in stopAllWorkers().
  acquisitionFsm.startTimeoutSweeper(30_000);

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
    validate: (from, to, evt) => constitutional.validateDomainTransition('telemetry-coordination-fsm', from, to, evt),
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
  // Stop acquisition-fsm timeout sweeper before cognition scanner shutdown —
  // sweeper is a setInterval and must be cleared deterministically to avoid
  // firing on a stopped FSM.
  acquisitionFsm.stopTimeoutSweeper();
  await cognitionScanner.stop();
  await cadence.stop();
  lifecycle.stopAll();

  // Shutdown observability plane — persist final snapshot
  const observability = require('./observability');
  await observability.stop();

  console.log('[orchestrator] Constitutional kernel stopped');
}

module.exports = { startAllWorkers, stopAllWorkers };
