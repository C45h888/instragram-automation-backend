/**
 * Phase7RuntimeSimulator — Evolved execution platform (Phase 7 contract §4, §6, §7, §8, §15)
 * ═══════════════════════════════════════════════════════════════════════════════════════════
 *
 * The primary execution environment for all Phase 7 testing. Built on top
 * of the existing RuntimeSimulator (tests/helpers/runtime-simulator.js) with
 * ADDITIVE observation surface. The 17-step boot, existing public methods,
 * and Phases 1–6 contract are unchanged.
 *
 * New built-in observation surface (every test gets this for free):
 *   - event timeline
 *   - state inspector (pre/post snapshot + diff)
 *   - mutation tracker
 *   - worker tracer
 *   - governance observer
 *
 * On any failed assertion: report() dumps the full runtime timeline,
 * state diff, worker trace, and governance log to
 * tests/phase-7/reports/<run-id>/. No mystery failures.
 */

const path = require('path');
const { RuntimeSimulator } = require('../../helpers/runtime-simulator.js');
const { EventRecorder } = require('./event-recorder.js');
const { StateInspector } = require('./state-inspector.js');
const { MutationTracker } = require('./mutation-tracker.js');
const { WorkerTracer } = require('./worker-tracer.js');
const { GovernanceObserver } = require('./governance-observer.js');
const { CadenceAccelerator } = require('./cadence-accelerator.js');
const { GraphSimulator } = require('./graph-simulator.js');

const fs = require('fs');

const CK = require('../../../control-plane/governance/constitutional-kernel.js');
const lineageLedger = require('../../../control-plane/governance/lineage-ledger.js');
const graphCapabilityFsm = require('../../../graph-capability-kernel/fsm.js');

class Phase7RuntimeSimulator {
  /**
   * Per-process counter for graph-simulator port allocation. Kept for
   * diagnostics; the port is actually chosen by _findFreePortPair()
   * so we never collide with leftover bindings from prior test files.
   */
  static _instanceCount = 0;

  /**
   * Process-wide set of ports we know are in use. Stored on globalThis
   * so it survives vitest's per-file module isolation when possible
   * (singleFork + shared globalThis). When globalThis is isolated,
   * the set resets per file and the OS-probe handles the rest.
   */
  static get _allocatedPorts() {
    if (!globalThis.__phase7AllocatedPorts) {
      globalThis.__phase7AllocatedPorts = new Set();
    }
    return globalThis.__phase7AllocatedPorts;
  }

  /**
   * Find an unused (worker, worker+1) port pair. Strategy:
   *   1. Listen on port 0 to get an OS-assigned free port.
   *   2. Verify controlPort (= workerPort + 1) is also bindable.
   *   3. If either fails, retry up to 16 times with jittered backoff.
   * The OS-assigned port is the one we hand to the graph-simulator,
   * so collisions with leftover bindings from prior test files are
   * caught at listen() time (the graph-simulator would fail to bind
   * and the caller can retry the whole boot).
   */
  static async _findFreePortPair(maxAttempts = 16) {
    const net = require('net');
    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      const port = await new Promise((resolve, reject) => {
        const probe = net.createServer();
        probe.listen(0, '0.0.0.0', () => {
          const p = probe.address().port;
          probe.close(() => resolve(p));
        });
        probe.on('error', reject);
      });
      const controlPort = port + 1;
      // Verify control port is bindable. If not, retry.
      const controlFree = await new Promise((resolve) => {
        const cp = net.createServer();
        cp.listen(controlPort, '0.0.0.0', () => {
          cp.close(() => resolve(true));
        });
        cp.on('error', () => resolve(false));
      });
      if (!controlFree) {
        await new Promise((r) => setTimeout(r, 25 + Math.floor(Math.random() * 25)));
        continue;
      }
      return { workerPort: port, controlPort };
    }
    throw new Error(
      `Phase7RuntimeSimulator._findFreePortPair: could not find a free (worker, control) port pair in ${maxAttempts} attempts`
    );
  }

  /**
   * @param {object} [opts]
   * @param {string} [opts.runId] — defaults to timestamp
   * @param {object} [opts.bootOpts] — passed to underlying RuntimeSimulator
   * @param {boolean} [opts.startGraphSimulator=true] — start the canonical Graph
   * @param {boolean} [opts.autoReport=true] — dump report on failed assertion
   * @param {string} [opts.reportDir]
   */
  constructor({
    runId = null,
    bootOpts = {},
    startGraphSimulator = true,
    autoReport = true,
    reportDir = path.join(__dirname, '..', 'reports'),
  } = {}) {
    this._runId = runId || `run-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    this._bootOpts = bootOpts;
    this._autoReport = autoReport;
    this._reportDir = reportDir;
    this._booted = false;

    this._runtime = new RuntimeSimulator(bootOpts);

    // Cache constructor flag for lazy graph-simulator construction
    // in boot(). The flag controls whether a graph-simulator instance
    // is allocated at all — it is NOT a port-allocation detail.
    this._startGraphSimulator = startGraphSimulator;

    this._eventRecorder = new EventRecorder();
    this._stateInspector = new StateInspector({
      ck: CK,
      lineageLedger,
      capabilityFsm: graphCapabilityFsm,
    });
    this._mutationTracker = new MutationTracker();
    this._workerTracer = new WorkerTracer();
    this._governanceObserver = new GovernanceObserver();

    // Graph-simulator is created lazily in boot() so we can find a
    // free (worker, control) port pair before binding. See boot()
    // and _findFreePortPair() below.
    this._graphSimulator = startGraphSimulator ? null : null;

    this._assertionsRun = 0;
    this._assertionsFailed = 0;
    this._bootStartTime = null;
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Lifecycle
  // ═══════════════════════════════════════════════════════════════════════════

  async boot() {
    if (this._booted) return;
    this._bootStartTime = Date.now();

    // Start graph simulator first so workers have a target. The
    // port pair is allocated here (not in the constructor) because
    // _findFreePortPair() is async — it has to ask the OS for a
    // free port via a net.createServer probe.
    if (this._startGraphSimulator && !this._graphSimulator) {
      const { workerPort, controlPort } = await Phase7RuntimeSimulator._findFreePortPair();
      this._graphSimulator = new GraphSimulator({
        workerPort,
        controlPort,
      });
    }
    if (this._graphSimulator) {
      // Wait briefly for the OS to fully release any prior
      // graph-simulator's port. Without this, the kernel can
      // reassign a port that's still in TIME_WAIT to a new
      // listener, and the new listener's bind fails with
      // EADDRINUSE even though the OS reported the port as free
      // during the probe. 200ms is empirically enough on macOS.
      await new Promise((r) => setTimeout(r, 200));
      await this._graphSimulator.start();
    }

    // Boot the runtime (17 steps, unchanged)
    await this._runtime.boot();

    // Wire observation surface (additive)
    this._wireObservation();

    this._booted = true;
  }

  /**
   * Two-phase shutdown: (1) close the graph-simulator's HTTP
   * servers — this MUST complete (or the port stays bound and the
   * next test file's boot hits EADDRINUSE); (2) drain the runtime
   * projection workers, bounded by a 20s ceiling because the
   * underlying RuntimeSimulator.shutdown() can hang on the 30s
   * snapshot timer. Anything still alive after 20s gets reaped
   * when the vitest fork exits.
   */
  async shutdown() {
    if (!this._booted) return;
    // Phase 1: graph-simulator servers (port release)
    if (this._graphSimulator) {
      try {
        await this._graphSimulator.stop();
      } catch (_) { /* swallow — best-effort */ }
    }
    // Phase 2: runtime drain (projection workers, snapshot timers)
    const runtimeShutdown = this._runtime.shutdown();
    const hardTimeout = new Promise((resolve) => setTimeout(resolve, 20000));
    await Promise.race([runtimeShutdown, hardTimeout]);
    this._booted = false;
  }

  _wireObservation() {
    // Event recorder hooks into observability if available
    let observability;
    try {
      observability = require('../../../control-plane/observability/index.js');
      this._eventRecorder.attach(observability);
      this._workerTracer.attach({ observability });
    } catch (_) {
      // observability may not be available; recorders operate in passive mode
    }

    // Mutation tracker hooks into lineage ledger, CK, and substrate writes
    this._mutationTracker.attach({
      ck: CK,
      lineageLedger,
      capabilityFsm: graphCapabilityFsm,
      observability,
    });

    // Governance observer wraps CK methods
    this._governanceObserver.attach(CK);
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // New additive surface
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * Inject an event into the runtime via the observability plane.
   * Bypasses no authority — the event is governed normally.
   */
  injectEvent({ type, payload = {}, source = 'phase7-test', correlationId = null, ...rest }) {
    this._assertionsRun++;
    try {
      const observability = require('../../../control-plane/observability/index.js');
      observability.dispatch
        ? observability.dispatch({ type, payload, source, correlationId, ...rest })
        : observability.transition
        ? observability.transition({
            domain: (payload && payload.domain) || 'phase7',
            entity: (payload && payload.entity) || 'injected',
            entityId: (payload && payload.entityId) || correlationId || `inj-${Date.now()}`,
            previousState: null,
            nextState: type,
            authority: source,
            raw: payload,
          })
        : null;

      this._eventRecorder.record({
        type,
        source,
        destination: 'observability',
        payload,
        correlationId,
      });
    } catch (e) {
      this._assertionsFailed++;
      if (this._autoReport) this.report();
      throw e;
    }
  }

  /** Run n accelerated cadence ticks. */
  async tick(n = 1) {
    const accel = new CadenceAccelerator({
      tier: 'short',
      tickCount: n,
      tickIntervalMs: 0,
      dispatch: async (evt) => {
        // FIX (2026-06-14): route through observability first so the
        // EventRecorder captures the event. Then call CK.dispatch so
        // governance also processes it. Previously tick() bypassed the
        // observability plane entirely, causing the RecorderObserver to
        // miss tick-injected events.
        let observability;
        try {
          observability = require('../../../control-plane/observability/index.js');
          if (observability && typeof observability.transition === 'function') {
            observability.transition({
              domain: 'scheduling',
              entity: 'cadence',
              entityId: `tick-${Date.now()}`,
              previousState: null,
              nextState: evt.type,
              authority: 'cadence-accelerator',
              raw: evt,
            });
          }
        } catch (_) {
          // observability may not be available; fall through to direct CK dispatch
        }
        CK.dispatch(evt);
      },
    });
    return accel.run();
  }

  /** Full state snapshot. */
  async snapshot() {
    return this._stateInspector.snapshot();
  }

  /** Diff two snapshots. */
  diff(before, after) {
    return this._stateInspector.diff(before, after);
  }

  /** Causal event chain. */
  timeline() {
    return this._eventRecorder.timeline();
  }

  /** Worker execution record. */
  workerTrace(name) {
    return this._workerTracer.records(name);
  }

  /** Governance decision log. */
  governanceLog() {
    return this._governanceObserver.decisions();
  }

  /** Mutation history. */
  mutations() {
    return this._mutationTracker.peek();
  }

  /**
   * Assert snapshot matches expectation. On failure, dumps report and throws.
   */
  async assertStateMatches(expectation, label = 'state') {
    this._assertionsRun++;
    try {
      const snap = await this.snapshot();
      this._stateInspector.assertMatches(snap, expectation, label);
    } catch (e) {
      this._assertionsFailed++;
      if (this._autoReport) this.report({ error: e, label });
      throw e;
    }
  }

  /**
   * Assert a chain of event types occurred in order.
   */
  assertCausality(chain, label = 'causality') {
    this._assertionsRun++;
    const result = this._eventRecorder.matchChain(chain);
    if (!result.matched) {
      this._assertionsFailed++;
      const err = new Error(
        `Phase7RuntimeSimulator: ${label} chain not matched (expected: ${chain.join(' → ')})`
      );
      err.expected = chain;
      err.timeline = this._eventRecorder.timeline();
      if (this._autoReport) this.report({ error: err, label });
      throw err;
    }
  }

  /**
   * Assert a mutation occurred.
   */
  assertMutation({ store, predicate, label = 'mutation' }) {
    this._assertionsRun++;
    try {
      return this._mutationTracker.assertMutation({ store, predicate, label });
    } catch (e) {
      this._assertionsFailed++;
      if (this._autoReport) this.report({ error: e, label });
      throw e;
    }
  }

  /**
   * Assert no governance decisions were made by workers.
   */
  assertWorkersDidNotGovern() {
    this._assertionsRun++;
    try {
      this._governanceObserver.assertWorkersDidNotGovern(this._workerTracer);
    } catch (e) {
      this._assertionsFailed++;
      if (this._autoReport) this.report({ error: e, label: 'governance-boundary' });
      throw e;
    }
  }

  /**
   * Generate runtime report. Called automatically on failure; can be
   * called manually for any test.
   */
  report(extra = {}) {
    const reportDir = path.join(this._reportDir, this._runId);
    try {
      fs.mkdirSync(reportDir, { recursive: true });
    } catch (_) {}

    const report = {
      runId: this._runId,
      timestamp: new Date().toISOString(),
      booted: this._booted,
      assertionsRun: this._assertionsRun,
      assertionsFailed: this._assertionsFailed,
      bootDurationMs: this._bootStartTime ? Date.now() - this._bootStartTime : 0,
      timeline: this.timeline(),
      workerTrace: this._workerTracer.records(),
      governanceLog: this.governanceLog(),
      mutations: this.mutations(),
      error: extra.error
        ? {
            message: extra.error.message,
            label: extra.label,
            stack: extra.error.stack,
            violations: extra.error.violations,
          }
        : null,
    };

    try {
      fs.writeFileSync(
        path.join(reportDir, 'report.json'),
        JSON.stringify(report, null, 2)
      );
    } catch (_) {}

    return report;
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Accessors
  // ═══════════════════════════════════════════════════════════════════════════

  get eventRecorder() { return this._eventRecorder; }
  get stateInspector() { return this._stateInspector; }
  get mutationTracker() { return this._mutationTracker; }
  get workerTracer() { return this._workerTracer; }
  get governanceObserver() { return this._governanceObserver; }
  get graphSimulator() { return this._graphSimulator; }
  get runtime() { return this._runtime; }
  get runId() { return this._runId; }
}

module.exports = { Phase7RuntimeSimulator };
