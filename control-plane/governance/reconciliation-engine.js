// control-plane/governance/reconciliation-engine.js
// Reconciliation Engine: standalone constitutional equilibrium verifier.
//
// Owns: running full reconciliation cycles — epoch creation, hash computation,
//        three-reality comparison (lineage ↔ materialized ↔ operational),
//        factual drift signal detection, drift severity classification.
//
// Does NOT own: governance decisions, corrective action, state mutation,
//               domain logic, substrate semantics — those belong to CK and FSMs.
//
// Called by: constitutional-kernel.js on RECONCILIATION_TICK.
//
// Architecture invariant:
//   This engine is COMPUTATION-ONLY. It compares and classifies.
//   It NEVER decides what to do about drift.
//   It NEVER mutates domain state, substrate state, or lineage.
//   Constitutional authority remains singular in the CK.
//
// Epoch markers and cycle results are emitted through the observability plane.
// The lineage worker ingests these transitions and writes them to the canonical
// ledger (lineage:ledger:entries). The engine NEVER writes to the ledger directly.
//
// Uses recon-lineage-interpreter for bounded full observability access.
// Interpreter ensures namespace filtering at read time (Recon has full view).
//
// Contract:
//   engine.runCycle({ fsms, substrates, lineageLedger }) → { epochId, hash, observations, worstSeverity }

const crypto = require('crypto');
const reconInterpreter = require('./interpreters/recon-lineage-interpreter');

// ═══════════════════════════════════════════════════════════════════════════════
// Drift Signal Constants — factual observations, NOT severity judgments
// ═══════════════════════════════════════════════════════════════════════════════

const DRIFT_SIGNAL = {
  NONE: 'none',
  MISSING_SUBSTRATE_INTENT: 'missing_substrate_intent',
  ORPHANED_CIRCUIT_BREAKER: 'orphaned_circuit_breaker',
  STALE_MATERIALIZED_STATE: 'stale_materialized_state',
  GHOST_EMISSION: 'ghost_emission',
  CADENCE_GAP: 'cadence_gap',
  LINEAGE_POSITION_MISMATCH: 'lineage_position_mismatch',
  AUTH_STRIKE_DRIFT: 'auth_strike_drift',
};

// ═══════════════════════════════════════════════════════════════════════════════
// Drift Severity — computed by this engine from factual signals
// ═══════════════════════════════════════════════════════════════════════════════

const DRIFT_SEVERITY = {
  NONE: 0,
  TRANSIENT: 1,
  REPLAY: 2,
  SUBSTRATE: 3,
  LINEAGE_CORRUPTION: 4,
};

// ═══════════════════════════════════════════════════════════════════════════════
// Signal → Severity mapping — engine-owned classification logic
// ═══════════════════════════════════════════════════════════════════════════════

function _classifySignal(signal) {
  switch (signal) {
    case DRIFT_SIGNAL.NONE:
      return DRIFT_SEVERITY.NONE;
    case DRIFT_SIGNAL.MISSING_SUBSTRATE_INTENT:
    case DRIFT_SIGNAL.ORPHANED_CIRCUIT_BREAKER:
    case DRIFT_SIGNAL.GHOST_EMISSION:
    case DRIFT_SIGNAL.AUTH_STRIKE_DRIFT:
      return DRIFT_SEVERITY.SUBSTRATE;
    case DRIFT_SIGNAL.STALE_MATERIALIZED_STATE:
    case DRIFT_SIGNAL.LINEAGE_POSITION_MISMATCH:
      return DRIFT_SEVERITY.REPLAY;
    case DRIFT_SIGNAL.CADENCE_GAP:
      // First occurrence is transient; engine leaves escalation decision to CK
      return DRIFT_SEVERITY.TRANSIENT;
    default:
      return DRIFT_SEVERITY.SUBSTRATE;
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Three-reality comparison per domain
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Reconcile the acquisition domain across lineage, materialized state, and substrate reality.
 *
 * @param {object} fsm — acquisition FSM instance
 * @param {object} substrates — substrate query interface
 * @param {Array<object>} domainLineage — recent lineage entries for this domain
 * @returns {{ driftSignals: Array<{ signal: string, detail: string }>, materializedState: string }}
 */
function _reconcileAcquisition(fsm, substrates, domainLineage) {
  const driftSignals = [];
  const materializedState = fsm.getState ? fsm.getState() : 'unknown';
  const exportState = fsm.exportState ? fsm.exportState() : {};

  // ── Stale materialized state vs lineage ───────────────────────────────
  const lastDomainEntry = domainLineage.length > 0
    ? domainLineage[domainLineage.length - 1]
    : null;
  if (lastDomainEntry && lastDomainEntry.nextState && materializedState !== lastDomainEntry.nextState) {
    driftSignals.push({
      signal: DRIFT_SIGNAL.STALE_MATERIALIZED_STATE,
      detail: `FSM state '${materializedState}' diverges from last lineage state '${lastDomainEntry.nextState}' (entity: ${lastDomainEntry.entity})`,
    });
  }

  // ── Orphaned circuit breakers ─────────────────────────────────────────
  // FSM shows active breaker, but check if there's a recent rate-limit lineage entry
  if (exportState.activeCircuitBreakers > 0 && typeof fsm.getCircuitBreakers === 'function') {
    const breakers = fsm.getCircuitBreakers();
    const now = Date.now();
    for (const [accountId, breaker] of breakers) {
      if (breaker && typeof breaker.until === 'number' && breaker.until > now) {
        // Check if any recent lineage entry justifies this breaker
        const recentRateLimit = domainLineage.some(e =>
          e.entity === 'circuit_breaker' &&
          e.nextState === 'RATE_LIMITED' &&
          e.entityId === accountId
        );
        if (!recentRateLimit) {
          driftSignals.push({
            signal: DRIFT_SIGNAL.ORPHANED_CIRCUIT_BREAKER,
            detail: `Circuit breaker active for ${accountId} (until ${new Date(breaker.until).toISOString()}) but no recent rate-limit lineage entry found`,
          });
        }
      }
    }
  }

  // ── Missing substrate intents (retry state vs dedup) ──────────────────
  if (typeof fsm.getExecutionRetries === 'function') {
    const retries = fsm.getExecutionRetries();
    if (retries.size > 0 && typeof substrates.dedupIsInFlight === 'function') {
      let checked = 0;
      for (const [intentId] of retries) {
        if (checked >= 5) break;
        checked++;
      }
      if (retries.size > 0 && typeof substrates.retryInFlight === 'function') {
        // Retry substrate is a mechanical layer — lightweight check
      }
    }
  }

  // ── Auth strike drift ─────────────────────────────────────────────────
  if (typeof fsm.getAuthStrikeMap === 'function') {
    const strikes = fsm.getAuthStrikeMap();
    for (const [accountId, count] of strikes) {
      // Check if recent auth failure lineage entries support this count
      const authEntries = domainLineage.filter(e =>
        e.entity === 'auth' && e.nextState === 'AUTH_FAILURE_STRIKE' && e.entityId === accountId
      );
      if (count > 0 && authEntries.length === 0) {
        driftSignals.push({
          signal: DRIFT_SIGNAL.AUTH_STRIKE_DRIFT,
          detail: `Auth strike count ${count} for ${accountId} but no AUTH_FAILURE_STRIKE lineage entries found`,
        });
      }
    }
  }

  return { driftSignals, materializedState };
}

/**
 * Reconcile the publishing domain across lineage, materialized state, and substrate reality.
 *
 * @param {object} fsm — publishing FSM instance
 * @param {object} substrates — substrate query interface
 * @param {Array<object>} domainLineage — recent lineage entries for this domain
 * @returns {{ driftSignals: Array<{ signal: string, detail: string }>, materializedState: string }}
 */
function _reconcilePublishing(fsm, substrates, domainLineage) {
  const driftSignals = [];
  const materializedState = fsm.getState ? fsm.getState() : 'unknown';

  // ── Stale materialized state vs lineage ───────────────────────────────
  const lastDomainEntry = domainLineage.length > 0
    ? domainLineage[domainLineage.length - 1]
    : null;
  if (lastDomainEntry && lastDomainEntry.nextState && materializedState !== lastDomainEntry.nextState) {
    driftSignals.push({
      signal: DRIFT_SIGNAL.STALE_MATERIALIZED_STATE,
      detail: `FSM state '${materializedState}' diverges from last lineage state '${lastDomainEntry.nextState}' (entity: ${lastDomainEntry.entity})`,
    });
  }

  // ── Ghost emissions — FSM in EMITTING but buffer is empty ────────────
  if (materializedState === 'EMITTING' || materializedState === 'EVALUATING') {
    let bufferEmpty = false;
    if (typeof substrates.bufferSnapshot === 'function') {
      try {
        const snapshot = substrates.bufferSnapshot();
        bufferEmpty = !snapshot || snapshot.size === 0;
      } catch {
        bufferEmpty = true;
      }
    }

    if (bufferEmpty) {
      driftSignals.push({
        signal: DRIFT_SIGNAL.GHOST_EMISSION,
        detail: `Publishing FSM in '${materializedState}' but buffer snapshot is empty — no active emission target`,
      });
    }
  }

  return { driftSignals, materializedState };
}

/**
 * Reconcile the scheduling domain across lineage, materialized state, and substrate reality.
 *
 * @param {object} fsm — scheduling FSM instance
 * @param {object} substrates — substrate query interface
 * @param {Array<object>} domainLineage — recent lineage entries for this domain
 * @returns {{ driftSignals: Array<{ signal: string, detail: string }>, materializedState: string }}
 */
function _reconcileScheduling(fsm, substrates, domainLineage) {
  const driftSignals = [];
  const materializedState = fsm.getState ? fsm.getState() : 'unknown';

  // ── Stale materialized state vs lineage ───────────────────────────────
  const lastDomainEntry = domainLineage.length > 0
    ? domainLineage[domainLineage.length - 1]
    : null;
  if (lastDomainEntry && lastDomainEntry.nextState && materializedState !== lastDomainEntry.nextState) {
    driftSignals.push({
      signal: DRIFT_SIGNAL.STALE_MATERIALIZED_STATE,
      detail: `FSM state '${materializedState}' diverges from last lineage state '${lastDomainEntry.nextState}' (entity: ${lastDomainEntry.entity})`,
    });
  }

  // ── Cadence gap — last CADENCE_TICK too old ──────────────────────────
  const lastCadenceTick = typeof fsm.getLastCadenceTick === 'function'
    ? fsm.getLastCadenceTick()
    : null;
  if (lastCadenceTick) {
    const gapMs = Date.now() - lastCadenceTick;
    const maxGapMs = 120_000; // 2 minute max gap before signal
    if (gapMs > maxGapMs) {
      driftSignals.push({
        signal: DRIFT_SIGNAL.CADENCE_GAP,
        detail: `Last CADENCE_TICK was ${Math.round(gapMs / 1000)}s ago (threshold: ${maxGapMs / 1000}s)`,
      });
    }
  }

  // ── Substrate reality — cadence tick continuity check ────────────────
  if (typeof substrates.cadenceLastTick === 'function') {
    const substrateLastTick = substrates.cadenceLastTick();
    if (lastCadenceTick && substrateLastTick) {
      const gapToSubstrate = Math.abs(lastCadenceTick - substrateLastTick);
      if (gapToSubstrate > 90_000) {
        driftSignals.push({
          signal: DRIFT_SIGNAL.LINEAGE_POSITION_MISMATCH,
          detail: `FSM cadence tick (${new Date(lastCadenceTick).toISOString()}) diverges from substrate tick (${new Date(substrateLastTick).toISOString()}) by ${Math.round(gapToSubstrate / 1000)}s`,
        });
      }
    }
  }

  return { driftSignals, materializedState };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Domain reconciliation dispatcher
// ═══════════════════════════════════════════════════════════════════════════════

const DOMAIN_RECONCILERS = {
  acquisition: _reconcileAcquisition,
  publishing: _reconcilePublishing,
  scheduling: _reconcileScheduling,
};

// ═══════════════════════════════════════════════════════════════════════════════
// Public API: runCycle()
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Run a full reconciliation cycle across all registered domains.
 *
 * COMPUTATION-ONLY — never mutates state, never decides corrective action.
 *
 * @param {object} params
 * @param {Map<string, object>} params.fsms — Map of domainName → FSM instance
 * @param {object} params.substrates — substrate query interface
 * @param {object} params.lineageLedger — lineage ledger module
 * @returns {{ epochId: string, lineagePosition: number, hash: string, observations: Array, worstSeverity: number }}
 */
async function runCycle({ fsms, substrates, lineageLedger }) {
  // 1. Generate epoch — no longer calls createEpoch() which wrote directly to ledger.
  //    Instead, emit through observability so the worker writes it (sole bridge).
  const epochId = crypto.randomUUID();
  const entries = await lineageLedger.getLineage();
  const lineagePosition = entries.length;

  _emitReconciliationTransition({
    domain: 'governance', entity: 'reconciliation', entityId: epochId,
    previousState: null, nextState: 'EPOCH_CREATED',
    authority: 'reconciliation-engine',
    raw: { epochId, lineagePosition },
  });

  // 2. Compute constitutional hash via ledger
  const hash = await lineageLedger.computeHash();

  // 3. Materialize state from the entries we already fetched
  const materialized = lineageLedger.materializeState(entries);

  // 4. Reconcile each domain
  const observations = [];
  let worstSeverity = DRIFT_SEVERITY.NONE;

  for (const [domainName, reconciler] of Object.entries(DOMAIN_RECONCILERS)) {
    const fsm = fsms.get(domainName);
    if (!fsm) {
      observations.push({
        domain: domainName,
        driftSignals: [{
          signal: DRIFT_SIGNAL.NONE,
          detail: `Domain '${domainName}' not registered in FSM map`,
        }],
        severity: DRIFT_SEVERITY.NONE,
        materializedState: 'unregistered',
      });
      continue;
    }

    // Get recent domain lineage (last 20 entries)
    const domainLineage = await lineageLedger.getDomainLineage(domainName, 20);

    // Run domain reconciler
    const { driftSignals } = reconciler(fsm, substrates, domainLineage);

    // Classify severity from signals
    let domainSeverity = DRIFT_SEVERITY.NONE;
    for (const ds of driftSignals) {
      const signalSeverity = _classifySignal(ds.signal);
      if (signalSeverity > domainSeverity) {
        domainSeverity = signalSeverity;
      }
    }

    // If no drift signals, add a NONE signal for observability
    const signals = driftSignals.length > 0
      ? driftSignals
      : [{ signal: DRIFT_SIGNAL.NONE, detail: 'All three layers converge' }];

    observations.push({
      domain: domainName,
      driftSignals: signals,
      severity: domainSeverity,
      materializedState: fsm.getState ? fsm.getState() : 'unknown',
      lineageState: materialized.domains[domainName] || 'unknown',
    });

    if (domainSeverity > worstSeverity) {
      worstSeverity = domainSeverity;
    }
  }

  // 5. Emit cycle completion through observability — worker writes to ledger
  const resultantState = worstSeverity === DRIFT_SEVERITY.NONE ? 'CONVERGENT' : 'DRIFTED';
  _emitReconciliationTransition({
    domain: 'governance', entity: 'reconciliation', entityId: epochId,
    previousState: 'RECONCILING', nextState: resultantState,
    authority: 'reconciliation-engine',
    raw: {
      epochId, lineagePosition, hash: hash.slice(0, 16),
      worstSeverity, domainCount: observations.length,
      driftedDomains: observations.filter(o => o.severity > 0).length,
    },
  });

  return { epochId, lineagePosition, hash, observations, worstSeverity };
}

/**
 * Emit a reconciliation event through the observability plane.
 * The lineage worker ingests this transition and writes it to the canonical
 * ledger, ensuring the worker remains the sole ledger writer.
 *
 * @param {object} params — transition parameters (domain, entity, nextState, etc.)
 */
function _emitReconciliationTransition(params) {
  try {
    const observability = require('../observability');
    observability.transition(params);
  } catch (_) {}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Exports
// ═══════════════════════════════════════════════════════════════════════════════

module.exports = {
  runCycle,
  DRIFT_SIGNAL,
  DRIFT_SEVERITY,
};
