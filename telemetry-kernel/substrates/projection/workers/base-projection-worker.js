// telemetry-kernel/substrates/projection/workers/base-projection-worker.js
// Base Projection Worker: shared infrastructure for bounded telemetry projection.
// Copied from control-plane/telemetry-workers/base-projection-worker.js
// with observability path adjusted for kernel location.
//
// Owns: event-driven tick scheduling, cursor tracking, projection versioning,
//        deterministic projection synthesis from raw telemetry signals,
//        replay determinism.
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
//
// Trigger model (event-driven, not periodic sampling):
//   Workers subscribe to the observability stream via onWrite(). When a new
//   transition lands in the worker's domain partition, a debounced tick is
//   scheduled. This eliminates temporal aliasing and missed state edges that
//   setInterval polling introduces. The debounce interval (pollIntervalMs)
//   prevents excessive ticks on rapid write bursts while ensuring no state
//   edge is missed — every write event is a scheduling signal.

const crypto = require('crypto');
const monotonicClock = require('../../../../control-plane/runtime/monotonic-clock');
const { getRedisClient } = require('../../../../config/redis');
// Lazy import to avoid circular dep at module load time.
// The emitter is used only in the failure path, which never fires at boot.
// eslint-disable-next-line global-require
let _observabilityEmitter = null;
function _getEmitter() {
  if (!_observabilityEmitter) {
    try {
      _observabilityEmitter = require('../../../../control-plane/observability/emitters/transition-emitter');
    } catch (_) {
      _observabilityEmitter = null;
    }
  }
  return _observabilityEmitter;
}

// ── Versioning ───────────────────────────────────────────────────────────────────

const PROJECTION_VERSION = '1.0.0';

// ── Staging buffer constants ────────────────────────────────────────────────────
// Bounded Redis list per namespace. Holds failed projection payloads
// so future retry workers can replay them without losing the synthesized
// projection (signals are gone after the tick returns — only the staged
// projection + signals hash survive).
const STAGING_KEY_PREFIX = 'lineage:projection-staging:';
const STAGING_MAX_ENTRIES = 10_000;
const STAGING_TTL_S = 3600; // 1 hour

// ── Abstract base class ─────────────────────────────────────────────────────────

class BaseProjectionWorker {
  constructor({ pollIntervalMs = 30_000, workerName }) {
    if (!workerName || typeof workerName !== 'string') {
      throw new Error('[base-projection-worker] workerName is required');
    }

    this.workerName = workerName;
    this.pollIntervalMs = pollIntervalMs;
    this._unsubscribeOnWrite = null;   // onWrite unsubscribe handle
    this._tickPending = false;         // debounce gate: true when a tick is scheduled
    this._tickTimer = null;            // setTimeout handle for debounced tick
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
      const { getLogSize } = require('../../../../control-plane/observability');
      return getLogSize();
    } catch {
      return 0;
    }
  }

  async _tick() {
    this._lastTick = monotonicClock.nextTimestamp();
    this._tickCount++;
    const _wallClock = Date.now();

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
        timestamp: this._lastTick,           // monotonic ticker (constitutional ordering)
        wallClockTimestamp: _wallClock,       // Date.now() (observability only)
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
      // Awaited — ensures the transition is written to Redis bounded partition
      // before the next tick starts. This eliminates the fire-and-forget race
      // where setImmediate(PROCESS_INTENTS) fires before rpush completes.
      await this._emitProjectionTransition(projection);

    } catch (err) {
      this._consecutiveFailures++;
      console.error(`[${this.workerName}] Tick error:`, err.message);
      // Stage the failed projection for potential replay.
      // Fire-and-forget: never blocks the tick, never re-throws.
      this._stageAsync(projection, signals, { lineageStartCursor, lineageEndCursor });
      // Emit the failure intent to the telemetry-failures partition.
      // Async fire-and-forget — the FSM observes via onWrite() subscription
      // and dispatches RETRY_CADENCE_REQUEST. Never blocks the tick.
      this._emitPartitionWriteFailureAsync(projection, err, { lineageStartCursor, lineageEndCursor });
    }
  }

  /**
   * Stage a failed projection to a bounded Redis list for potential replay.
   *
   * Called only on the failure path of _tick() — when observability.transition()
   * throws. The synthesized projection is still in scope at that point; the
   * raw signals are too. We persist the projection payload + a hash of the
   * signals (for forensic verification) so a future retry worker can replay
   * the exact same projection without needing to re-read signals.
   *
   * Fire-and-forget: setImmediate ensures the tick returns immediately.
   * The lpush + ltrim + expire happen in the background. If staging itself
   * fails, the failure is swallowed — staging is insurance, not critical path.
   *
   * @param {object} projection — the synthesized projection (projection output contract)
   * @param {object} signals — raw signals from _getNormalizedInputWindow()
   * @param {object} cursors — { lineageStartCursor, lineageEndCursor }
   */
  _stageAsync(projection, signals, cursors) {
    setImmediate(async () => {
      try {
        const redis = getRedisClient();
        if (!redis || redis.status !== 'ready') return;

        const entry = {
          projectionId: projection.projectionId,
          namespace: this._domain,
          projectionType: this._projectType,
          projectionVersion: this._projectionVersion,
          projectionPayload: projection.projectionPayload,
          confidence: projection.confidence,
          integrityScore: projection.integrityScore,
          sourceTelemetryWindow: projection.sourceTelemetryWindow,
          signalsHash: crypto
            .createHash('sha256')
            .update(JSON.stringify(signals))
            .digest('hex'),
          lineageCursors: cursors,
          stagedAt: Date.now(),
          traceId: projection.traceId,
          correlationId: projection.correlationId,
        };

        const key = STAGING_KEY_PREFIX + this._domain;
        const serialized = JSON.stringify(entry);

        await Promise.all([
          redis.lpush(key, serialized),
          redis.ltrim(key, 0, STAGING_MAX_ENTRIES - 1),
          redis.expire(key, STAGING_TTL_S),
        ]);
      } catch (_) {
        // Best-effort. Staging is insurance, not critical path.
        // If this fails, the projection is lost — same as before this change.
      }
    });
  }

  /**
   * Emit a PROJECTION_PARTITION_WRITE_FAILED failure intent.
   *
   * Called on the failure path of _tick() — when observability.transition()
   * throws. The failure intent lands in the 'telemetry-failures' domain
   * partition (separate from the 5 namespace partitions). The
   * telemetry-coordination-fsm observes it via onWrite() and dispatches
   * RETRY_CADENCE_REQUEST to the engagement-fsm.
   *
   * Fire-and-forget via setImmediate. Never blocks the tick. If the emit
   * itself fails, the staging entry (written in the same catch block) is
   * the only survivor — the FSM has no way to know about the failure in
   * that case, which is the accepted trade-off for never blocking the tick.
   *
   * @param {object} projection — the synthesized projection that failed
   * @param {Error} err — the error that caused the failure
   * @param {object} cursors — { lineageStartCursor, lineageEndCursor }
   */
  _emitPartitionWriteFailureAsync(projection, err, cursors) {
    setImmediate(async () => {
      try {
        const emitter = _getEmitter();
        if (!emitter || !emitter.transition) return;

        const signalsHash = crypto
          .createHash('sha256')
          .update(JSON.stringify(projection.sourceTelemetryWindow || {}))
          .digest('hex');

        await emitter.transition({
          domain: 'telemetry-failures',
          entity: 'projection_intent_failure',
          entityId: this._projectType,
          previousState: 'PROJECTION_INTENT',
          nextState: 'PROJECTION_PARTITION_WRITE_FAILED',
          authority: this.workerName,
          raw: {
            namespace: this._domain,
            projectionType: this._projectType,
            projectionId: projection.projectionId,
            projectionVersion: this._projectionVersion,
            errorMessage: err.message,
            errorName: err.name,
            signalsHash,
            lineageCursors: cursors,
            consecutiveFailures: this._consecutiveFailures,
            failedAt: Date.now(),
            traceId: projection.traceId,
            correlationId: projection.correlationId,
          },
        });
      } catch (_) {
        // Best-effort. The staging entry is the last-resort backup.
      }
    });
  }

  /**
   * Emit a PROJECTION_INTENT into the observability plane.
   *
   * Projection workers NO LONGER emit SEMANTIC_PROJECTION_TRANSITION directly.
   * Instead, they emit PROJECTION_INTENT — an ingress request that must be
   * validated, ordered, and serialized by the Telemetry Coordination FSM
   * before entering canonical lineage.
   *
   * The FSM is the sole serializer. This worker declares intent only.
   *
   * @param {object} projection — the projection output contract
   */
  async _emitProjectionTransition(projection) {
    try {
      // eslint-disable-next-line global-require
      const observability = require('../../../../control-plane/observability/emitters/transition-emitter');
      // PROJECTION_INTENT entries MUST land in the projection domain — NOT the
      // operational domain (e.g. 'acquisition', 'publishing'). Writing into the
      // operational domain partition creates a self-triggering feedback loop
      // (the worker's own onWrite fires on its PROJECTION_INTENT emissions)
      // and pollutes the operational partition with telemetry-plane entries.
      // The operational namespace is preserved in raw.projectionNamespace for
      // the FSM to route to the correct transition writer.
      await observability.transition({
        domain: 'projection',
        entity: 'projection_intent',
        entityId: this._projectType,
        previousState: this._lastProjectionTs ? `${this._projectType}:intent` : null,
        nextState: 'PROJECTION_INTENT',
        authority: this.workerName,
        raw: {
          intentType: 'PROJECTION_INTENT',
          projectionNamespace: this._domain,
          projectionType: this._projectType,
          projectionVersion: this._projectionVersion,
          projectionPayload: projection.projectionPayload,
          confidence: projection.confidence,
          integrityScore: projection.integrityScore,
          sourceTelemetryWindow: projection.sourceTelemetryWindow,
          traceId: projection.traceId,
          correlationId: projection.correlationId,
        },
      });
    } catch (err) {
      console.warn(`[${this.workerName}] Projection intent emit error:`, err.message);
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
   * Runs an immediate tick, then subscribes to the observability stream for
   * event-driven scheduling — no setInterval polling.
   *
   * Each write event in this worker's domain triggers a debounced tick:
   * if a tick is not already pending, one is scheduled after pollIntervalMs.
   * Rapid write bursts are batched into a single tick. An immediate tick runs
   * on start to establish the baseline projection.
   *
   * @param {number} [pollIntervalMs] — override default debounce interval
   */
  async start(pollIntervalMs) {
    if (this._running) {
      console.warn(`[${this.workerName}] Already running`);
      return;
    }

    this._running = true;
    this._startedAt = Date.now();

    const interval = pollIntervalMs || this.pollIntervalMs;
    const domain = this._domain;

    // Subscribe to observability stream — event-driven trigger
    try {
      // eslint-disable-next-line global-require
      const observability = require('../../../../control-plane/observability');
      this._unsubscribeOnWrite = observability.onWrite((transition) => {
        // Only react to entries landing in this worker's domain
        if (transition.domain !== domain) return;
        // Never self-trigger on our own PROJECTION_INTENT emissions.
        // Defense-in-depth: the emission domain is now 'projection' (not the
        // operational domain), so this guard is secondary — but it prevents
        // re-triggering if emission domain ever changes back.
        if (transition.entity === 'projection_intent') return;
        // Event-driven trigger: fire within 100ms, not the full pollIntervalMs.
        // This eliminates the ~30s latency between a transition being written
        // and the projection worker reading it. The poll interval only applies
        // to the fallback setInterval path and the initial tick baseline.
        this._scheduleTick(100);
      });
    } catch (err) {
      console.error(`[${this.workerName}] Failed to subscribe to onWrite, falling back to polling:`, err.message);
      // Fallback: setInterval polling if onWrite subscription fails
      this._tickTimer = setInterval(() => {
        if (this._running) {
          this._tick().catch(e => console.error(`[${this.workerName}] Tick error:`, e.message));
        }
      }, interval);
      this._tickTimer.unref();
    }

    // Run initial tick immediately to establish baseline projection
    await this._tick();

    console.log(`[${this.workerName}] Started — projectionType=${this._projectType}, debounce=${interval}ms (onWrite-driven)`);
  }

  /**
   * Schedule a debounced tick. If a tick is already pending, this is a no-op.
   * After pollIntervalMs, the pending tick fires and the gate resets.
   *
   * @param {number} delayMs — debounce interval
   */
  _scheduleTick(delayMs) {
    if (this._tickPending || !this._running) return;
    this._tickPending = true;

    this._tickTimer = setTimeout(() => {
      this._tickPending = false;
      this._tickTimer = null;
      if (!this._running) return;
      this._tick().catch(err => {
        console.error(`[${this.workerName}] Tick error:`, err.message);
      });
    }, delayMs);
    // Don't unref — we want the tick to fire even if the event loop is idle
  }

  /**
   * Stop the projection worker gracefully.
   */
  async stop() {
    if (!this._running) return;
    this._running = false;

    // Unsubscribe from onWrite stream
    if (this._unsubscribeOnWrite) {
      try { this._unsubscribeOnWrite(); } catch (_) {}
      this._unsubscribeOnWrite = null;
    }

    // Clear any pending tick timer
    if (this._tickTimer) {
      clearTimeout(this._tickTimer);
      this._tickTimer = null;
    }
    this._tickPending = false;

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
