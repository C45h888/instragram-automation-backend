// control-plane/telemetry-workers/base-projection-worker.js
// Base Projection Worker: shared infrastructure for bounded telemetry projection.
//
// Owns: polling cadence, cursor tracking, projection versioning, replay determinism,
//        deterministic projection synthesis from raw telemetry signals.
//
// Does NOT own: governance decisions, lineage persistence, FSM semantics,
//               interpreter namespace filtering.
//
// All projection workers inherit from this base. Subclasses override:
//   _getSnapshotSource() — returns the raw telemetry source(s) to consume
//   _synthesize(projectionState, signals) — converts raw signals → semantic projection
//   _projectType — unique identifier for this projection type
//
// Projection Output Contract:
// {
//   projectionId, projectionType, projectionVersion, timestamp, traceId,
//   correlationId, causationId, domain, sourceTelemetryWindow,
//   projectionPayload, confidence, integrityScore
// }
//
// Replay determinism:
//   same signals + same version = same projection (always)
//   projection synthesis NEVER depends on: runtime timing, worker execution order,
//   transient memory, async race conditions

// TODO(architecture): Replace setInterval polling with observability stream consumer.
// Polling introduces temporal aliasing and missed state edges. Long-term direction:
// observability stream → projection stream processor (event-driven, not periodic sampling).
// Do NOT harden polling as permanent architecture.

const crypto = require('crypto');

// ── Versioning ───────────────────────────────────────────────────────────────────

const PROJECTION_VERSION = '1.0.0';

// ── Abstract base class ─────────────────────────────────────────────────────────

class BaseProjectionWorker {
  constructor({ pollIntervalMs = 30_000, workerName }) {
    if (!workerName || typeof workerName !== 'string') {
      throw new Error('[base-projection-worker] workerName is required');
    }

    this.workerName = workerName;
    this.pollIntervalMs = pollIntervalMs;
    this._pollTimer = null;
    this._running = false;
    this._startedAt = null;
    this._lastTick = null;
    this._tickCount = 0;
    this._projectionVersion = PROJECTION_VERSION;
    this._projectionCache = null; // cached projection output
    this._lastProjectionTs = null;
    this._consecutiveFailures = 0;

    // Telemetry window tracking
    this._sourceTelemetryWindow = {
      openedAt: null,
      closedAt: null,
      entryCount: 0,
    };
  }

  // ── Subclass override points ──────────────────────────────────────────────────

  /**
   * Returns the projection type identifier.
   * Must be overridden by subclass.
   * @returns {string}
   */
  get _projectType() {
    throw new Error('[base-projection-worker] _projectType must be overridden');
  }

  /**
   * Returns the domain this projection worker operates within.
   * @returns {string}
   */
  get _domain() {
    return 'projection';
  }

  /**
   * Fetch the raw telemetry source(s) for this projection tick.
   * Subclasses override to return their specific telemetry inputs.
   *
   * @returns {Promise<object>} raw telemetry signals
   */
  async _getSnapshotSource() {
    return {};
  }

  async _getNormalizedInputWindow() {
    return this._getSnapshotSource();
  }

  /**
   * Synthesize semantic meaning from raw telemetry signals.
   * Subclasses override to implement projection-specific logic.
   *
   * MUST be deterministic: same signals + same version ALWAYS produces same output.
   * MUST NOT depend on: current time (use timestamp from signals), runtime memory,
   *   async ordering, or external state.
   *
   * @param {object} projectionState — current projection state (may be used for delta)
   * @param {object} signals — raw telemetry signals from _getSnapshotSource()
   * @returns {object} projection payload
   */
  _synthesize(projectionState, signals) {
    return {};
  }

  _runSynthesis(projectionState, normalizedWindow) {
    return this._synthesize(projectionState, normalizedWindow);
  }

  /**
   * Compute the confidence score for this projection tick.
   * Range: 0.0 – 1.0
   *
   * @param {object} signals
   * @returns {number}
   */
  _computeConfidence(signals) {
    return 1.0;
  }

  /**
   * Compute the integrity score for this projection tick.
   * Range: 0.0 – 1.0 (1.0 = fully coherent, 0.0 = coherence broken)
   *
   * @param {object} signals
   * @returns {number}
   */
  _computeIntegrityScore(signals) {
    return 1.0;
  }

  // ── Core tick ────────────────────────────────────────────────────────────────

  // ── Core tick ────────────────────────────────────────────────────────────────

  /**
   * Get the current lineage cursor (ledger sequence id) for replay watermarking.
   * Subclasses may override to provide their specific cursor source.
   * The default returns the current transition log size as a proxy cursor.
   *
   * @returns {number} current lineage cursor
   */
  _getLineageCursor() {
    try {
      // eslint-disable-next-line global-require
      const { getLogSize } = require('../observability');
      return getLogSize();
    } catch {
      return 0;
    }
  }

  async _tick() {
    this._lastTick = Date.now();
    this._tickCount++;

    try {
      // Capture lineage cursor range for replay determinism watermarking.
      // These cursors allow forensic reconstruction and reconciliation
      // verification of which telemetry window was consumed.
      const lineageStartCursor = this._getLineageCursor();
      const signals = await this._getNormalizedInputWindow();
      const lineageEndCursor = this._getLineageCursor();

      // Build source telemetry window metadata with replay cursors
      const windowMeta = {
        openedAt: signals.windowOpenedAt || this._lastTick - this.pollIntervalMs,
        closedAt: this._lastTick,
        entryCount: signals.entryCount || 0,
        lineageStartCursor,
        lineageEndCursor,
        telemetryWindowStart: signals.windowOpenedAt || this._lastTick - this.pollIntervalMs,
        telemetryWindowEnd: this._lastTick,
      };

      // Synthesize semantic projection (deterministic)
      const payload = this._runSynthesis(this._projectionCache || {}, signals);

      // Compute scores
      const confidence = signals.noiseGate ? 0.0 : this._computeConfidence(signals);
      const integrityScore = this._computeIntegrityScore(signals);

      // Build the projection output contract
      const projection = {
        projectionId: crypto.randomUUID(),
        projectionType: this._projectType,
        projectionVersion: this._projectionVersion,
        timestamp: this._lastTick,
        traceId: this._generateTraceId(),
        correlationId: this._generateCorrelationId(),
        causationId: null,
        domain: this._domain,
        sourceTelemetryWindow: windowMeta,
        projectionPayload: payload,
        confidence,
        integrityScore,
      };

      // Cache for next tick delta (if needed by subclass)
      this._projectionCache = payload;
      this._lastProjectionTs = this._lastTick;
      this._consecutiveFailures = 0;

      // Emit the projection to the observability plane
      this._emitProjectionTransition(projection);

    } catch (err) {
      this._consecutiveFailures++;
      console.error(`[${this.workerName}] Tick error:`, err.message);
    }
  }

  /**
   * Emit a SEMANTIC_PROJECTION_TRANSITION into the observability plane.
   * This is the ONLY emission path.
   *
   * @param {object} projection — the projection output contract
   */
  _emitProjectionTransition(projection) {
    try {
      // eslint-disable-next-line global-require
      const observability = require('../observability/emitters/transition-emitter');
      observability.transition({
        domain: this._domain,
        entity: 'semantic_projection',
        entityId: this._projectType,
        previousState: this._lastProjectionTs ? `${this._projectType}:active` : null,
        nextState: `${this._projectType}:projected`,
        authority: this.workerName,
        raw: {
          entryType: 'SEMANTIC_PROJECTION_TRANSITION',
          ...projection,
        },
      });
    } catch (err) {
      console.warn(`[${this.workerName}] Projection emit error:`, err.message);
    }
  }

  // ── Trace/correlation ID generation ────────────────────────────────────────

  _generateTraceId() {
    return crypto.randomUUID();
  }

  _generateCorrelationId() {
    return `${this.workerName}:${this._tickCount}:${this._projectionVersion}`;
  }

  // ── Public API ───────────────────────────────────────────────────────────────

  /**
   * Start the projection worker.
   * Runs an immediate tick, then schedules the polling loop.
   *
   * @param {number} [pollIntervalMs] — override default poll interval
   */
  async start(pollIntervalMs) {
    if (this._running) {
      console.warn(`[${this.workerName}] Already running`);
      return;
    }

    this._running = true;
    this._startedAt = Date.now();

    // Run initial tick immediately
    await this._tick();

    const interval = pollIntervalMs || this.pollIntervalMs;
    this._pollTimer = setInterval(() => {
      this._tick().catch(err => {
        console.error(`[${this.workerName}] Tick error:`, err.message);
      });
    }, interval);
    this._pollTimer.unref();

    console.log(`[${this.workerName}] Started — projectionType=${this._projectType}, poll=${interval}ms`);
  }

  /**
   * Stop the projection worker gracefully.
   */
  async stop() {
    if (!this._running) return;
    this._running = false;

    if (this._pollTimer) {
      clearInterval(this._pollTimer);
      this._pollTimer = null;
    }

    console.log(`[${this.workerName}] Stopped — ticks=${this._tickCount}`);
  }

  /**
   * Return cached projection state for external consumers.
   *
   * @returns {object|null}
   */
  getProjection() {
    return this._projectionCache ? JSON.parse(JSON.stringify(this._projectionCache)) : null;
  }

  /**
   * Return worker health signals.
   */
  getHealth() {
    return {
      workerName: this.workerName,
      projectionType: this._projectType,
      projectionVersion: this._projectionVersion,
      running: this._running,
      uptimeMs: this._startedAt ? Date.now() - this._startedAt : 0,
      lastTick: this._lastTick,
      tickCount: this._tickCount,
      consecutiveFailures: this._consecutiveFailures,
    };
  }
}

module.exports = { BaseProjectionWorker, PROJECTION_VERSION };
