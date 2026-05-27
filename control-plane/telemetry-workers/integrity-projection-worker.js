// control-plane/telemetry-workers/integrity-projection-worker.js
// Integrity Projection Worker: synthesizes replayContinuity, causationIntegrity,
// epochContinuity, transitionDiscontinuities.
//
// Owns: semantic synthesis of lineage integrity signals.
// Does NOT own: governance decisions, lineage persistence, FSM semantics.
//
// Projection Type: INTEGRITY_PROJECTION
// Source: in-memory lineage buffer state from lineage-worker (via direct read)
//
// Determinism contract:
//   same lineageBuffer state + same tickCount + same version
//   = ALWAYS same replayContinuity, causationIntegrity, epochContinuity

const { BaseProjectionWorker } = require('./base-projection-worker');

const PROJECTION_TYPE = 'INTEGRITY_PROJECTION';
const POLL_INTERVAL_MS = 30_000;

// Memory for continuity tracking (reset on worker stop)
let _lastTickCount = 0;
let _lastCausedBy = new Map(); // ledgerId → count

class IntegrityProjectionWorker extends BaseProjectionWorker {
  constructor() {
    super({ pollIntervalMs: POLL_INTERVAL_MS, workerName: 'integrity-projection-worker' });
    this._replayContinuity = 1.0;
    this._causationIntegrity = 1.0;
    this._epochContinuity = 1.0;
    this._transitionDiscontinuities = 0;
    this._projectedLedgerEntries = []; // track projected entries to detect duplicates
  }

  get _projectType() {
    return PROJECTION_TYPE;
  }

  get _domain() {
    return 'integrity';
  }

  /**
   * Fetch from lineage-worker state (in-memory).
   * We read the lineage worker module directly to access its buffer.
   * This avoids circular import because we only import at tick time.
   *
   * @returns {Promise<object>}
   */
  async _getSnapshotSource() {
    try {
      // eslint-disable-next-line global-require
      const lineageWorker = require('../governance/lineage-worker');
      const ledgerSize = lineageWorker.getLedgerSize();
      const divergences = lineageWorker.getDivergences();
      return {
        ledgerSize,
        divergences,
        tickCount: this._tickCount,
        windowOpenedAt: Date.now() - POLL_INTERVAL_MS,
        entryCount: ledgerSize,
        noiseGate: ledgerSize < 1,
      };
    } catch {
      return {
        ledgerSize: 0,
        divergences: [],
        tickCount: this._tickCount,
        windowOpenedAt: Date.now() - POLL_INTERVAL_MS,
        entryCount: 0,
        noiseGate: true,
      };
    }
  }

  /**
   * Synthesize replayContinuity, causationIntegrity, epochContinuity,
   * transitionDiscontinuities.
   * Deterministic: uses only signals passed as argument.
   *
   * @param {object} projectionState — previous cached projection
   * @param {object} signals — from _getSnapshotSource()
   * @returns {object} projectionPayload
   */
  _synthesize(projectionState, signals) {
    const { ledgerSize, divergences } = signals;

    // Detect transition discontinuities from divergence log
    const structuralAnomalies = (divergences || [])
      .filter(d => d.category === 'structural')
      .slice(-20); // last 20

    const runtimeAnomalies = (divergences || [])
      .filter(d => d.category === 'runtime_interpretation')
      .slice(-10);

    const transitionDiscontinuities = structuralAnomalies.length + runtimeAnomalies.length;

    // Replay continuity: if ledger size is growing, continuity is preserved
    const replayContinuity = this._deriveReplayContinuity(ledgerSize, projectionState);

    // Causation integrity: broken causation chains indicate coherence break
    const causationIntegrity = this._deriveCausationIntegrity(structuralAnomalies);

    // Epoch continuity: stability of epoch boundary markers
    const epochContinuity = this._deriveEpochContinuity(divergences);

    return {
      replayContinuity,
      causationIntegrity,
      epochContinuity,
      transitionDiscontinuities,
      structuralAnomalyCount: structuralAnomalies.length,
      runtimeAnomalyCount: runtimeAnomalies.length,
      totalLedgerEntries: ledgerSize,
    };
  }

  _deriveReplayContinuity(ledgerSize, projectionState) {
    const prev = projectionState.totalLedgerEntries || 0;
    if (ledgerSize === 0) return 1.0;
    // Continuity grows as ledger grows; degrades if gap detected
    const gap = ledgerSize - prev;
    if (gap === 0) return Math.max(0.5, this._replayContinuity);
    if (gap < 0) return Math.max(0, this._replayContinuity - 0.3);
    return Math.min(1.0, this._replayContinuity + 0.1);
  }

  _deriveCausationIntegrity(structuralAnomalies) {
    const brokenChain = structuralAnomalies.filter(a => a.type === 'BROKEN_CAUSATION_CHAIN');
    if (brokenChain.length === 0) return 1.0;
    return Math.max(0, 1.0 - brokenChain.length * 0.2);
  }

  _deriveEpochContinuity(divergences) {
    if (!divergences || divergences.length === 0) return 1.0;
    const epochGaps = divergences.filter(d => d.type === 'EPOCH_GAP');
    return Math.max(0, 1.0 - epochGaps.length * 0.25);
  }

  _computeConfidence(signals) {
    if (signals.noiseGate) return 0.0;
    if (signals.ledgerSize < 10) return 0.3;
    if (signals.ledgerSize < 50) return 0.7;
    return 1.0;
  }

  _computeIntegrityScore(signals) {
    const { divergences, ledgerSize } = signals;
    if (!divergences) return 1.0;
    const structuralCount = divergences.filter(d => d.category === 'structural').length;
    if (structuralCount === 0) return 1.0;
    const anomalyRate = structuralCount / Math.max(1, ledgerSize);
    return Math.max(0, 1.0 - anomalyRate * 10);
  }
}

module.exports = IntegrityProjectionWorker;
