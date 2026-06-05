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

    this._eventRecorder = new EventRecorder();
    this._stateInspector = new StateInspector({
      ck: CK,
      lineageLedger,
      capabilityFsm: graphCapabilityFsm,
    });
    this._mutationTracker = new MutationTracker();
    this._workerTracer = new WorkerTracer();
    this._governanceObserver = new GovernanceObserver();

    this._graphSimulator = startGraphSimulator ? new GraphSimulator() : null;

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

    // Start graph simulator first so workers have a target
    if (this._graphSimulator) {
      await this._graphSimulator.start();
    }

    // Boot the runtime (17 steps, unchanged)
    await this._runtime.boot();

    // Wire observation surface (additive)
    this._wireObservation();

    this._booted = true;
  }

  async shutdown() {
    if (!this._booted) return;
    await this._runtime.shutdown();
    if (this._graphSimulator) {
      await this._graphSimulator.stop();
    }
    this._booted = false;
  }

  _wireObservation() {
    // Event recorder hooks into observability if available
    try {
      const observability = require('../../../control-plane/observability/index.js');
      this._eventRecorder.attach(observability);
      this._workerTracer.attach({ observability });
    } catch (_) {
      // observability may not be available; recorders operate in passive mode
    }

    // Mutation tracker hooks into lineage ledger and CK
    this._mutationTracker.attach({
      ck: CK,
      lineageLedger,
      capabilityFsm: graphCapabilityFsm,
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
