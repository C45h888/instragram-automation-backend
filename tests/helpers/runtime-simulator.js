/**
 * RuntimeSimulator — Full Constitutional Runtime Bootstrap
 * =========================================================
 *
 * Boots the complete governance stack for Phase 5 integration testing:
 *   observability → CK domain registration → telemetry workers →
 *   lineage worker → CK rehydrate → CK startLoop
 *
 * Boot order (matches production orchestrator exactly):
 *   1. observability.init()
 *   2. Register all 7 domain FSMs with CK
 *   3. Start engagement telemetry adapter (raw bounded telemetry pre-processor)
 *   4. Start telemetry projection workers (5 workers)
 *   5. Start lineage worker (consumes observability → writes ledger)
 *   6. metricsSubstrate.init()
 *   7. CK.rehydrate()
 *   8. lifecycle.refresh() + LIFECYCLE_REFRESHED dispatch
 *   9. BOOT_COMPLETE dispatch (CK: BOOTING → HEALTHY)
 *   10. CK.startLoop()
 *
 * Architectural invariant:
 *   This simulator is a TEST SUBSTRATE. It never mutates constitutional state,
 *   never bypasses governance authority, and never reinterprets lineage meaning.
 *   All governance authority lives in the CK and domain FSMs.
 *
 * Usage:
 *   const sim = new RuntimeSimulator({ lineagePollMs: 400 });
 *   await sim.boot();
 *   const result = await sim.runEngineComparison();
 *   await sim.triggerReconciliationCycle();
 *   await sim.shutdown();
 */

const observability = require('../../control-plane/observability/index.js');
const CK = require('../../control-plane/governance/constitutional-kernel.js');
const telemetryWorkers = require('../../control-plane/telemetry-workers/index.js');
const lineageWorker = require('../../control-plane/governance/lineage-worker.js');
const lineageLedger = require('../../control-plane/governance/lineage-ledger.js');
const reconciliationEngine = require('../../control-plane/governance/reconciliation-engine.js');
const metricsSubstrate = require('../../substrates/metrics-substrate.js');
const lifecycle = require('../../control-plane/runtime/lifecycle.js');
const engagementTelemetryAdapter = require('../../control-plane/governance/interpreters/engagement-telemetry-adapter.js');

const acquisitionFsm = require('../../control-plane/governance/domains/acquisition-fsm.js');
const publishingFsm = require('../../control-plane/governance/domains/publishing-fsm.js');
const schedulingFsm = require('../../control-plane/governance/domains/scheduling-fsm.js');
const dedupFsm = require('../../control-plane/governance/domains/dedup-fsm.js');
const engagementFsm = require('../../control-plane/governance/domains/engagement-fsm.js');
const reconciliationFsm = require('../../control-plane/governance/domains/reconciliation-fsm.js');
const telemetryCoordinationFsm = require('../../control-plane/governance/domains/telemetry-coordination-fsm.js');

const ALL_DOMAIN_FSMS = [
  acquisitionFsm,
  publishingFsm,
  schedulingFsm,
  dedupFsm,
  engagementFsm,
  reconciliationFsm,
  telemetryCoordinationFsm,  // ← 7th domain (was missing)
];

const DOMAIN_NAMES = ALL_DOMAIN_FSMS.map((f) => f.name);

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

class RuntimeSimulator {
  /**
   * @param {object} opts
   * @param {number} [opts.tickIntervalMs=1000] — CK watchdog loop tick interval
   * @param {number} [opts.lineagePollMs=400] — lineage worker poll interval
   * @param {number} [opts.telemetryPollMs=50] — telemetry worker poll interval
   * @param {boolean} [opts.autoTick=true] — start CK watchdog loop on boot
   */
  constructor({
    tickIntervalMs = 1000,
    lineagePollMs = 400,
    telemetryPollMs = 50,
    autoTick = true,
  } = {}) {
    this._tickIntervalMs = tickIntervalMs;
    this._lineagePollMs = lineagePollMs;
    this._telemetryPollMs = telemetryPollMs;
    this._autoTick = autoTick;
    this._booted = false;
    this._lastReconResults = null;
    this._bootStartTime = null;
  }

  // ═════════════════════════════════════════════════════════════════════════
  // Bootstrap — matches production orchestrator boot order exactly
  // ═════════════════════════════════════════════════════════════════════════

  /**
   * Boot the complete constitutional runtime stack.
   *
   * Order matches production orchestrator.js startAllWorkers():
   *   observability.init()
   *   → register all 7 domain FSMs
   *   → engagementTelemetryAdapter.start()
   *   → telemetryWorkers.startAll()
   *   → lineageWorker.start()
   *   → metricsSubstrate.init()
   *   → CK.rehydrate()
   *   → lifecycle.refresh() + LIFECYCLE_REFRESHED dispatch
   *   → BOOT_COMPLETE dispatch
   *   → CK.startLoop()
   */
  async boot() {
    if (this._booted) {
      console.warn('[runtime-simulator] Already booted — ignoring duplicate boot');
      return;
    }

    this._bootStartTime = Date.now();

    // 1. Initialise the observability plane
    await observability.init();

    // 2. Register all 7 domain FSMs with the constitutional kernel
    for (const fsm of ALL_DOMAIN_FSMS) {
      CK.registerDomain(fsm);
    }

    // 3. Start engagement telemetry adapter — raw bounded telemetry pre-processor.
    //    Emits RAW_METRICS_WINDOW, RAW_QUOTA_WINDOW, RAW_RATE_LIMIT_WINDOW to observability.
    //    Must start before telemetry workers so workers can consume its emissions.
    await engagementTelemetryAdapter.start();

    // 4. Start telemetry projection workers (5 workers: runtime, integrity,
    //    authority, health, systemic). Must start BEFORE lineage worker so
    //    projections are available when lineage-worker starts consuming.
    await telemetryWorkers.startAll(this._telemetryPollMs);

    // 5. Start lineage worker — consumes from observability plane → writes ledger.
    //    MUST start after telemetry workers.
    await lineageWorker.start(this._lineagePollMs);

    // 6. Initialise metrics substrate
    await metricsSubstrate.init();

    // 7. Rehydrate CK from the lineage ledger (worker has started populating it)
    await CK.rehydrate();

    // 8. Refresh lifecycle and dispatch account state — matches production order
    await lifecycle.refresh();
    const accounts = await require('../../substrates/persistence.js').getActiveAccounts();
    CK.dispatch({
      type: 'LIFECYCLE_REFRESHED',
      accountIds: accounts.map((a) => a.id),
    });

    // 9. Signal boot complete — moves CK from BOOTING → HEALTHY
    //    (production orchestrator does this; test substrate must replicate it)
    CK.dispatch({ type: 'BOOT_COMPLETE' });

    // 10. Start the CK watchdog loop
    if (this._autoTick) {
      CK.startLoop(this._tickIntervalMs);
    }

    // Allow worker ingestion to catch up to initial boot state
    await sleep(300);

    this._booted = true;
    console.log(
      `[runtime-simulator] Boot complete — ${DOMAIN_NAMES.length} domains registered, workers started`
    );
  }

  /**
   * Gracefully shut down the runtime stack.
   * Order: CK loop → lineage worker → telemetry workers → observability.
   */
  async shutdown() {
    if (!this._booted) return;

    if (this._autoTick) {
      CK.stopLoop();
    }

    await lineageWorker.stop();
    await telemetryWorkers.stopAll();
    await engagementTelemetryAdapter.stop();
    await observability.stop();

    this._booted = false;
    console.log('[runtime-simulator] Shutdown complete');
  }

  // ═════════════════════════════════════════════════════════════════════════
  // Reconciliation Engine — Direct Comparison (Gap Testing)
  // ═════════════════════════════════════════════════════════════════════════

  /**
   * Run a direct reconciliation engine comparison.
   *
   * This bypasses the CK bridge and calls engine.compare() directly.
   * Substrate overrides allow targeted gap testing — e.g., forcing
   * an empty dedupSnapshot or a specific buffer state to trigger specific
   * drift signals.
   *
   * @param {object} [overrides]
   * @param {Map<string,object>} [overrides.fsms] — override domain FSM map
   * @param {object} [overrides.substrates] — override substrate query interface
   * @returns {Promise<{hash: string, observations: Array, worstSeverity: number}>}
   */
  async runEngineComparison(overrides = {}) {
    const substrates = overrides.substrates || this._buildSubstrates();
    const fsms = overrides.fsms || this._buildFsmMap();

    return reconciliationEngine.compare({
      fsms,
      substrates,
      lineageLedger,
    });
  }

  /**
   * Build the standard FSM map from CK's registered domains.
   * Used as default for runEngineComparison when no override is provided.
   * @returns {Map<string, object>}
   */
  _buildFsmMap() {
    const map = new Map();
    for (const fsm of ALL_DOMAIN_FSMS) {
      map.set(fsm.name, fsm);
    }
    return map;
  }

  /**
   * Build the substrate query interface matching CK._buildSubstrateQueries().
   * These are LIVE substrate calls — they hit real Redis via the substrate modules.
   * Override in gap tests to force specific signals.
   * @returns {object}
   */
  _buildSubstrates() {
    const dedupSubstrate = require('../../substrates/dedup-substrate');
    const retrySubstrate = require('../../substrates/retry');
    const cadence = require('../../control-plane/runtime/cadence');

    return {
      dedupIsInFlight: async (accountId, actionType, resourceId) => {
        return dedupSubstrate.isInFlight(accountId, actionType, resourceId);
      },
      retryInFlight: (accountId) => {
        return retrySubstrate.isAccountRateLimited
          ? retrySubstrate.isAccountRateLimited(accountId)
          : false;
      },
      bufferSnapshot: () => {
        const buffer = require('../../control-plane/runtime/buffer');
        try {
          return buffer.snapshot ? buffer.snapshot() : { size: 0, flushing: false };
        } catch {
          return { size: 0, flushing: false };
        }
      },
      metricsSignals: () => {
        const metricsSub = require('../../substrates/metrics-substrate');
        return metricsSub.getHealthSignals ? metricsSub.getHealthSignals() : {};
      },
      cadenceLastTick: () => {
        return cadence.lastTick ? cadence.lastTick() : null;
      },
      dedupSnapshot: () => {
        return typeof dedupSubstrate.getInflightSnapshot === 'function'
          ? dedupSubstrate.getInflightSnapshot()
          : { identityCount: 0, resourceCount: 0, sample: [] };
      },
    };
  }

  // ═════════════════════════════════════════════════════════════════════════
  // Reconciliation Cycle — Through CK Bridge (Integration Testing)
  // ═════════════════════════════════════════════════════════════════════════

  /**
   * Trigger a full reconciliation cycle through the CK.
   *
   * The CK owns the full cycle: dispatch tick → FSM emits CYCLE_STARTED action →
   * CK action subscriber fires async engine → CK dispatches results → FSM cycles
   * back to IDLE. This method awaits the full cycle via the promise that
   * the CK's action subscriber resolves when the cycle completes.
   *
   * @param {number} [waitMs=5000] — unused, kept for backward compat
   * @returns {Promise<object>} reconciliation results
   */
  async triggerReconciliationCycle(waitMs = 5000) {
    const startTime = Date.now();
    const reconFsm = ALL_DOMAIN_FSMS.find((f) => f.name === 'reconciliation');

    // CK.triggerReconciliation() is now truly async (fire-and-forget to the
    // subscriber, which resolves a promise when the full cycle completes).
    // await ensures we wait for FSM to return to IDLE before reading results.
    await CK.triggerReconciliation();

    // Cycle is complete — FSM should be back in IDLE
    const fsmEndState = reconFsm.getState();

    // Fetch results from ledger
    const recentEntries = await lineageLedger.getLineage(50);
    const reconEntries = recentEntries.filter((e) => e.domain === 'reconciliation');
    const lastResult = reconEntries[reconEntries.length - 1];

    this._lastReconResults = {
      fsmEndState,
      cycleEntries: reconEntries.slice(-5),
      lastResult,
      elapsedMs: Date.now() - startTime,
      timedOut: fsmEndState !== 'IDLE',
    };

    return this._lastReconResults;
  }

  // ═════════════════════════════════════════════════════════════════════════
  // Chaos Injection
  // ═════════════════════════════════════════════════════════════════════════

  /**
   * Kill all telemetry workers (simulate worker crash).
   * Workers are stopped gracefully via stopAll().
   */
  async killTelemetryWorkers() {
    await telemetryWorkers.stopAll();
  }

  /**
   * Restart telemetry workers after a kill.
   */
  async restartTelemetryWorkers() {
    await telemetryWorkers.startAll(this._telemetryPollMs);
  }

  /**
   * Kill the lineage worker (simulate lineage ingestion crash).
   */
  async killLineageWorker() {
    await lineageWorker.stop();
  }

  /**
   * Restart the lineage worker after a kill.
   */
  async restartLineageWorker() {
    await lineageWorker.start(this._lineagePollMs);
  }

  /**
   * Restart Redis — flush and reconnect.
   * WARNING: This destroys the canonical ledger. Only use in catastrophic
   * fault recovery tests where ledger loss is the scenario being tested.
   */
  async restartRedis() {
    const { getRedisClient } = require('../../config/redis');
    const redis = getRedisClient();
    await redis.flushall();
    // Allow connections to re-stabilize
    await sleep(500);
  }

  /**
   * Inject adversarial entries directly into the observability transition log.
   * These entries will be consumed by the lineage worker and written to the
   * canonical ledger, simulating adversarial substrate conditions.
   *
   * @param {Array<object>} entries — array of transition-like objects
   */
  injectChaosEntries(entries) {
    for (const entry of entries) {
      observability.transition({
        domain: entry.domain || 'governance',
        entity: entry.entity || 'fsm',
        entityId:
          entry.entityId ||
          `chaos-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
        previousState: entry.previousState || 'IDLE',
        nextState: entry.nextState || 'CHAOS_INJECTED',
        authority: entry.authority || 'chaos-injector',
        raw: entry.raw || { chaos: true, injectedAt: Date.now() },
      });
    }
  }

  // ═════════════════════════════════════════════════════════════════════════
  // State Queries
  // ═════════════════════════════════════════════════════════════════════════

  /**
   * Get the current CK global state.
   * @returns {string}
   */
  getCKState() {
    return CK.getState();
  }

  /**
   * Get the FSM state for a specific domain.
   * @param {string} domainName
   * @returns {string|null}
   */
  getDomainState(domainName) {
    const fsm = ALL_DOMAIN_FSMS.find((f) => f.name === domainName);
    return fsm && typeof fsm.getState === 'function' ? fsm.getState() : null;
  }

  /**
   * Get lineage entries from the canonical ledger.
   * @param {number} [n] — number of recent entries
   * @returns {Promise<Array<object>>}
   */
  async getLineage(n) {
    return lineageLedger.getLineage(n);
  }

  /**
   * Get the observability log size.
   * @returns {number}
   */
  getLogSize() {
    return observability.query.getLogSize();
  }

  /**
   * Get the last reconciliation cycle results.
   * @returns {object|null}
   */
  getLastReconciliationResults() {
    return this._lastReconResults;
  }

  /**
   * Get lineage worker projection snapshot.
   * @returns {object}
   */
  getProjections() {
    return lineageWorker.getProjections();
  }

  /**
   * Get the elapsed time since boot.
   * @returns {number} ms
   */
  getElapsedMs() {
    return this._bootStartTime ? Date.now() - this._bootStartTime : 0;
  }

  /**
   * Check if the simulator is booted.
   * @returns {boolean}
   */
  get isBooted() {
    return this._booted;
  }

  // ═════════════════════════════════════════════════════════════════════════
  // TEST ONLY — FSM getters for controlled test state injection
  // ═════════════════════════════════════════════════════════════════════════

  getDedupFsm() {
    return dedupFsm;
  }

  getEngagementFsm() {
    return engagementFsm;
  }

  getSchedulingFsm() {
    return schedulingFsm;
  }

  getAcquisitionFsm() {
    return acquisitionFsm;
  }

  getPublishingFsm() {
    return publishingFsm;
  }

  /**
   * Wait for the lineage worker to ingest entries from the observability log.
   * Polls until ledger size increases from the baseline.
   *
   * @param {number} [pollMs=500] - Poll interval in ms
   * @param {number} [timeoutMs=3000] - Timeout in ms
   */
  async waitForWorkerIngestion(pollMs = 500, timeoutMs = 3000) {
    const start = await lineageLedger.getSize();
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const current = await lineageLedger.getSize();
      if (current > start) return; // require NEW entries — not just same-as-start
      await new Promise((r) => setTimeout(r, pollMs));
    }
    // Graceful timeout — ledger may already be at steady state; do not throw
    console.warn(`[runtime-simulator] waitForWorkerIngestion timed out: ledger size unchanged at ${start}`);
  }
}

module.exports = { RuntimeSimulator, DOMAIN_NAMES };
