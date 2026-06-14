// Phase 9 — Runtime Harness.
// Wraps Phase7RuntimeSimulator (which boots the REAL control-plane: CK,
// lineage-ledger, FSMs, mutation-substrate, workers) and adds the
// phase-9 observation surface: passive recorder-observer, ownership
// tracer, snapshot deriver, drift detector, replay engine.
//
// Phase 9 does NOT modify the runtime. It only observes what the
// runtime naturally does. The recorder is a subscriber, not an actor.

import path from 'node:path';
import { fileURLToPath } from 'node:url';
import fs from 'node:fs';

import { Phase7RuntimeSimulator } from '../../phase-7/runtime/index.js';
import { RecorderObserver } from './recorder-observer.mjs';
import { OwnershipTracer } from './ownership-tracer.mjs';
import { SnapshotDeriver } from './snapshot-deriver.mjs';
import { DriftDetector } from './drift-detector.mjs';
import { ReplayEngine } from './replay-engine.mjs';
import { ReportWriter } from './report-writer.mjs';
import { dbReset } from './db-reset.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_REPORT_DIR = path.join(__dirname, '..', 'reports');

export class RuntimeHarness {
  constructor({ runId = null, bootOpts = {}, startGraphSimulator = true, reportDir = DEFAULT_REPORT_DIR } = {}) {
    this._runId = runId || `p9-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    this._bootOpts = bootOpts;
    this._startGraphSimulator = startGraphSimulator;
    this._reportDir = path.join(reportDir, this._runId);
    this._booted = false;

    // Lazy — populated in boot() so the underlying simulator can
    // allocate ports asynchronously before any listeners bind.
    this._simulator = null;
    this._recorder = null;
    this._ownershipTracer = null;
    this._snapshotDeriver = null;
    this._driftDetector = null;
    this._replayEngine = null;
    this._writer = null;
  }

  async boot() {
    if (this._booted) return;
    fs.mkdirSync(this._reportDir, { recursive: true });

    // Reset test-DB tables before the runtime boots so the
    // lineage ledger starts clean.
    await dbReset();

    // The Phase7 simulator boots the real CK, all FSMs, the
    // lineage ledger, the mutation substrate, and the workers.
    this._simulator = new Phase7RuntimeSimulator({
      runId: this._runId,
      bootOpts: this._bootOpts,
      startGraphSimulator: this._startGraphSimulator,
      autoReport: false,
      reportDir: this._reportDir,
    });
    await this._simulator.boot();

    // Wire the phase-9 observation surface. Each observer reads
    // from the running runtime; none of them write back.
    this._recorder = new RecorderObserver();
    this._recorder.attach(this._simulator);

    this._ownershipTracer = new OwnershipTracer();
    this._ownershipTracer.attach(this._simulator);

    this._snapshotDeriver = new SnapshotDeriver();
    this._snapshotDeriver.attach(this._simulator);
    this._snapshotDeriver._recorder = this._recorder; // share the lineage-aware snapshot

    this._driftDetector = new DriftDetector();
    this._driftDetector.reset(); // clear stale findings from any prior test run
    this._driftDetector.attach(this._simulator);

    this._replayEngine = new ReplayEngine();
    this._replayEngine.attach(this._simulator);

    this._writer = new ReportWriter({ reportDir: this._reportDir, runId: this._runId });

    this._booted = true;
  }

  async shutdown() {
    if (!this._booted) return;
    // Flush observations BEFORE shutdown — the simulator's
    // shutdown may drain observers of their own.
    await this._flushArtifacts();
    try { await this._simulator.shutdown(); } catch (_) { /* best-effort */ }
    this._booted = false;
  }

  // Public accessors — tests use these to drive the runtime.
  get simulator() { return this._simulator; }
  get recorder() { return this._recorder; }
  get ownershipTracer() { return this._ownershipTracer; }
  get snapshotDeriver() { return this._snapshotDeriver; }
  get driftDetector() { return this._driftDetector; }
  get replayEngine() { return this._replayEngine; }
  get writer() { return this._writer; }
  get runId() { return this._runId; }
  get reportDir() { return this._reportDir; }

  /**
   * Inject an event into the runtime via the underlying simulator.
   * Phase-9 tests deliver webhook payloads through this seam.
   */
  injectEvent(evt) {
    return this._simulator.injectEvent(evt);
  }

  /**
   * Run n accelerated cadence ticks.
   */
  async tick(n = 1) {
    return this._simulator.tick(n);
  }

  /**
   * Flush every phase-9 artifact to disk. Called by shutdown() and
   * by tests that want to inspect mid-run.
   */
  async _flushArtifacts() {
    const observation = this._recorder ? this._recorder.snapshot() : [];
    const ownership = this._ownershipTracer ? this._ownershipTracer.snapshot() : {};
    const snapshot = this._snapshotDeriver ? this._snapshotDeriver.derive() : {};
    const drift = this._driftDetector ? this._driftDetector.snapshot() : [];
    const replay = this._replayEngine ? await this._replayEngine.replay() : { diverged_keys: [], missing_observations: [] };

    fs.writeFileSync(
      path.join(this._reportDir, 'lineage-observation.json'),
      JSON.stringify(observation, null, 2)
    );
    fs.writeFileSync(
      path.join(this._reportDir, 'ownership-trace.json'),
      JSON.stringify(ownership, null, 2)
    );
    fs.writeFileSync(
      path.join(this._reportDir, 'lineage-snapshot.json'),
      JSON.stringify(snapshot, null, 2)
    );
    fs.writeFileSync(
      path.join(this._reportDir, 'drift-findings.json'),
      JSON.stringify(drift, null, 2)
    );
    fs.writeFileSync(
      path.join(this._reportDir, 'replay-delta.json'),
      JSON.stringify(replay, null, 2)
    );
  }
}

export const PHASE9_REPORT_DIR = DEFAULT_REPORT_DIR;
