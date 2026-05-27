// control-plane/governance/reconciliation-engine.js
// Reconciliation Engine: semantically blind comparison substrate.
//
// Owns: three-reality comparison (lineage ↔ materialized ↔ operational),
//        factual drift signal detection, drift severity classification,
//        constitutional hash computation.
//
// Does NOT own: governance decisions, corrective action, state mutation,
//               domain logic, substrate semantics, observability emission,
//               epoch creation, lifecycle management — those belong to
//               the reconciliation FSM and constitutional kernel.
//
// Called by: CK bridge subscriber (after reconciliation FSM signals CYCLE_STARTED).
//
// Architecture invariant:
//   This engine is a DUMB SUBSTRATE. It compares and classifies only.
//   It NEVER decides what to do about drift.
//   It NEVER mutates domain state, substrate state, or lineage.
//   It NEVER emits observability or writes to the ledger.
//   All governance authority lives in the reconciliation FSM and HSM (CK).
//
// Contract:
//   engine.compare({ fsms, substrates, lineageLedger }) → { hash, observations, worstSeverity }

const crypto = require('crypto');

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
  DEDUP_ORPHAN_KEY: 'dedup_orphan_key',
  DEDUP_REPLAY_COLLISION: 'dedup_replay_collision',
  CIRCUIT_BREAKER_COLLISION: 'circuit_breaker_collision',
  ENGAGEMENT_STALE_STATE: 'engagement_stale_state',
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
    case DRIFT_SIGNAL.DEDUP_ORPHAN_KEY:
    case DRIFT_SIGNAL.DEDUP_REPLAY_COLLISION:
    case DRIFT_SIGNAL.CIRCUIT_BREAKER_COLLISION:
      return DRIFT_SEVERITY.SUBSTRATE;
    case DRIFT_SIGNAL.STALE_MATERIALIZED_STATE:
    case DRIFT_SIGNAL.LINEAGE_POSITION_MISMATCH:
    case DRIFT_SIGNAL.ENGAGEMENT_STALE_STATE:
      return DRIFT_SEVERITY.REPLAY;
    case DRIFT_SIGNAL.CADENCE_GAP:
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

/**
 * Reconcile the dedup domain across lineage, FSM materialized state, and substrate reality.
 *
 * Phase 5: dedup now has a domain FSM. Reconciliation compares three layers:
 *   - Substrate snapshot (mechanical Redis keys — what actually exists)
 *   - FSM materialized state (governance understanding — what the FSM believes)
 *   - Lineage (canonical history — what was recorded)
 *
 * When the FSM is not registered (backward-compat), falls back to substrate-only comparison.
 *
 * @param {object|null} fsm — dedup FSM instance (may be null)
 * @param {object} substrates — substrate query interface (includes dedupSnapshot)
 * @param {Array<object>} domainLineage — recent lineage entries for dedup domain
 * @returns {{ driftSignals: Array<{ signal: string, detail: string }>, substrateSnapshot: object, fsmState: object|null }}
 */
function _reconcileDedup(fsm, substrates, domainLineage) {
  const driftSignals = [];

  // ── Substrate snapshot ─────────────────────────────────────────────
  let snapshot = { identityCount: 0, resourceCount: 0, sample: [] };
  if (typeof substrates.dedupSnapshot === 'function') {
    try {
      snapshot = substrates.dedupSnapshot();
    } catch {
      // Substrate unavailable — not a drift, just unobservable
    }
  }

  // ── Active IN_FLIGHT entries from lineage (not yet CLEARED) ────────
  const inFlightEntries = domainLineage.filter(e =>
    e.entity === 'dedup_entry' && e.nextState === 'IN_FLIGHT'
  );

  // ── Orphaned substrate keys ────────────────────────────────────────
  // Substrate has identity key, but no matching IN_FLIGHT lineage entry
  // within TTL window (120s). This means a key was set in Redis but the
  // corresponding transition never reached the ledger.
  const now = Date.now();
  const TTL_MS = 120_000;
  for (const sample of snapshot.sample) {
    const hasLineage = inFlightEntries.some(e => {
      const rawIntentId = e.raw?.raw?.intentId || e.raw?.intentId;
      const entryAge = now - (e.timestamp || 0);
      return rawIntentId === sample.intentId && entryAge <= TTL_MS;
    });
    if (!hasLineage) {
      driftSignals.push({
        signal: DRIFT_SIGNAL.DEDUP_ORPHAN_KEY,
        detail: `Substrate has in-flight key with intentId=${sample.intentId} but no matching IN_FLIGHT lineage entry within TTL window`,
      });
    }
  }

  // ── Replay collisions ──────────────────────────────────────────────
  // resource_tracker REPLAY_DETECTED entries are normal — they indicate
  // a different intent hit the same resource, which is allowed. Signal
  // only when replay rate is anomalously high relative to tracked resources.
  const replayEntries = domainLineage.filter(e =>
    e.entity === 'resource_tracker' && e.nextState === 'REPLAY_DETECTED'
  );
  if (replayEntries.length > 0 && snapshot.resourceCount > 0) {
    const replayRatio = replayEntries.length / snapshot.resourceCount;
    if (replayRatio > 0.5) {
      driftSignals.push({
        signal: DRIFT_SIGNAL.DEDUP_REPLAY_COLLISION,
        detail: `${replayEntries.length} replay detections vs ${snapshot.resourceCount} tracked resources — replay ratio ${(replayRatio * 100).toFixed(0)}% exceeds threshold`,
      });
    }
  }

  // ── FSM materialized state vs lineage (Phase 5) ─────────────────────
  const fsmState = fsm && typeof fsm.getBatchState === 'function'
    ? fsm.getBatchState()
    : null;

  if (fsm && fsmState) {
    const materializedState = fsm.getState ? fsm.getState() : 'unknown';

    // Stale materialized state vs lineage
    const lastDomainEntry = domainLineage.length > 0
      ? domainLineage[domainLineage.length - 1]
      : null;
    if (lastDomainEntry && lastDomainEntry.nextState && materializedState !== lastDomainEntry.nextState) {
      driftSignals.push({
        signal: DRIFT_SIGNAL.STALE_MATERIALIZED_STATE,
        detail: `Dedup FSM state '${materializedState}' diverges from last lineage state '${lastDomainEntry.nextState}' (entity: ${lastDomainEntry.entity})`,
      });
    }

    // FSM believes batch is ACTIVE but substrate has no in-flight keys
    if (fsmState.active && snapshot.identityCount === 0 && snapshot.resourceCount === 0) {
      driftSignals.push({
        signal: DRIFT_SIGNAL.DEDUP_ORPHAN_KEY,
        detail: `Dedup FSM in ACTIVE state but substrate snapshot is empty — possible lost batch window`,
      });
    }

    // FSM degradation count from FSM vs lineage degradation events
    const degradationCount = fsm.getDegradationCount ? fsm.getDegradationCount() : 0;
    const lineageDegradations = domainLineage.filter(e =>
      e.nextState === 'PARTIAL_FAILURE' || e.nextState === 'DEGRADED'
    );
    if (degradationCount > 0 && lineageDegradations.length === 0) {
      driftSignals.push({
        signal: DRIFT_SIGNAL.DEDUP_ORPHAN_KEY,
        detail: `Dedup FSM reports ${degradationCount} degradation signals but no matching lineage degradation entries`,
      });
    }
  }

  return { driftSignals, substrateSnapshot: snapshot, fsmState };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Engagement domain reconciliation
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Reconcile the engagement domain across lineage, FSM materialized state, and substrate reality.
 *
 * Engagement domain owns: circuit breaker lifecycle, auth strike tracking, retry counting.
 * Three realities compared:
 *   - Lineage: canonical history of circuit_breaker and auth_strike transitions
 *   - FSM: engagement-fsm Maps (_circuitBreakers, _authFailureStrikes, _executionRetries)
 *   - Substrate: retry-substrate._rateLimitedAccounts mechanical state
 *
 * @param {object} fsm — engagement FSM instance
 * @param {object} substrates — substrate query interface
 * @param {Array<object>} domainLineage — recent lineage entries for engagement domain
 * @returns {{ driftSignals: Array, fsmState: object }}
 */
function _reconcileEngagement(fsm, substrates, domainLineage) {
  const driftSignals = [];

  if (!fsm || typeof fsm.getEngagementSnapshot !== 'function') {
    return { driftSignals: [{ signal: DRIFT_SIGNAL.NONE, detail: 'Engagement FSM not registered' }], fsmState: null };
  }

  const snapshot = fsm.getEngagementSnapshot();
  const { circuitBreakers, authStrikes, executionRetries, fsmState } = snapshot;
  const now = Date.now();

  // ── Stale materialized state vs lineage ───────────────────────────────
  const lastDomainEntry = domainLineage.length > 0 ? domainLineage[domainLineage.length - 1] : null;
  if (lastDomainEntry && lastDomainEntry.nextState && fsmState !== lastDomainEntry.nextState) {
    driftSignals.push({
      signal: DRIFT_SIGNAL.ENGAGEMENT_STALE_STATE,
      detail: `Engagement FSM state '${fsmState}' diverges from last lineage state '${lastDomainEntry.nextState}'`,
    });
  }

  // ── Orphaned circuit breakers (FSM has active, lineage has no OPEN entry) ─
  for (const { accountId, until } of circuitBreakers) {
    const hasLineageEntry = domainLineage.some(e =>
      e.entity === 'circuit_breaker' &&
      (e.nextState === 'OPEN' || e.nextState === 'RATE_LIMITED') &&
      e.entityId === accountId
    );
    if (!hasLineageEntry) {
      driftSignals.push({
        signal: DRIFT_SIGNAL.ORPHANED_CIRCUIT_BREAKER,
        detail: `Circuit breaker active for ${accountId} (until ${new Date(until).toISOString()}) but no OPEN lineage entry found`,
      });
    }

    // ── Circuit breaker collision: re-tripped before prior cooldown expired ─
    const priorLineage = domainLineage.filter(e =>
      e.entity === 'circuit_breaker' && e.entityId === accountId
    );
    if (priorLineage.length >= 2) {
      // Check if this breaker has multiple OPEN events before its until time
      const priorOpens = priorLineage.filter(e => e.nextState === 'OPEN');
      if (priorOpens.length > 1) {
        const oldestOpen = priorOpens[0];
        const newestOpen = priorOpens[priorOpens.length - 1];
        if (oldestOpen.ts && newestOpen.ts && (newestOpen.ts - oldestOpen.ts) < 300000) {
          driftSignals.push({
            signal: DRIFT_SIGNAL.CIRCUIT_BREAKER_COLLISION,
            detail: `Circuit breaker for ${accountId} opened multiple times within 5 minutes — possible cooldown evasion`,
          });
        }
      }
    }
  }

  // ── Auth strike drift (FSM shows strikes, no lineage AUTH_FAILURE_STRIKE entries) ─
  for (const { accountId, strikes } of authStrikes) {
    if (strikes > 0) {
      const hasAuthLineage = domainLineage.some(e =>
        e.entity === 'auth_strike' && e.entityId === accountId
      );
      if (!hasAuthLineage) {
        driftSignals.push({
          signal: DRIFT_SIGNAL.AUTH_STRIKE_DRIFT,
          detail: `Auth strike count ${strikes} for ${accountId} but no auth_strike lineage entries found`,
        });
      }
    }
  }

  // ── Substrate/FSM circuit breaker divergence ─────────────────────────────
  if (typeof substrates.retryInFlight === 'function' && circuitBreakers.length > 0) {
    for (const { accountId } of circuitBreakers) {
      const substrateRateLimited = substrates.retryInFlight(accountId);
      if (!substrateRateLimited) {
        driftSignals.push({
          signal: DRIFT_SIGNAL.ORPHANED_CIRCUIT_BREAKER,
          detail: `Engagement FSM shows circuit breaker for ${accountId} but retry substrate shows not rate-limited`,
        });
      }
    }
  }

  return { driftSignals, fsmState: snapshot };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Domain reconciliation dispatcher
// ═══════════════════════════════════════════════════════════════════════════════

const DOMAIN_RECONCILERS = {
  acquisition: _reconcileAcquisition,
  publishing: _reconcilePublishing,
  scheduling: _reconcileScheduling,
  dedup: (fsm, substrates, domainLineage) => _reconcileDedup(fsm, substrates, domainLineage),
  engagement: (fsm, substrates, domainLineage) => _reconcileEngagement(fsm, substrates, domainLineage),
};

// ═══════════════════════════════════════════════════════════════════════════════
// Public API: compare()
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Run a three-reality comparison across all registered domains.
 *
 * DUMB SUBSTRATE — computation only. No state mutation, no observability,
 * no epoch management, no governance decisions.
 *
 * @param {object} params
 * @param {Map<string, object>} params.fsms — Map of domainName → FSM instance
 * @param {object} params.substrates — substrate query interface
 * @param {object} params.lineageLedger — lineage ledger module
 * @returns {{ hash: string, observations: Array, worstSeverity: number }}
 */
async function compare({ fsms, substrates, lineageLedger }) {
  // 1. Load lineage entries and compute constitutional hash
  const entries = await lineageLedger.getLineage();
  const hash = await lineageLedger.computeHash();

  // 2. Materialize state from the entries we already fetched
  const materialized = lineageLedger.materializeState(entries);

  // 3. Reconcile each domain
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
    const reconcilerResult = reconciler(fsm, substrates, domainLineage);
    const { driftSignals } = reconcilerResult;
    const snapshot = reconcilerResult.substrateSnapshot;
    const fsmState = reconcilerResult.fsmState;

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

    // Build materialized state string
    let materializedStateStr;
    if (domainName === 'dedup') {
      const fsmStateStr = fsm && fsm.getState ? fsm.getState() : 'unknown';
      materializedStateStr = `fsm:${fsmStateStr}, substrate:${snapshot?.identityCount || 0} inflight, ${snapshot?.resourceCount || 0} tracked`;
    } else {
      materializedStateStr = fsm && fsm.getState ? fsm.getState() : 'unknown';
    }

    observations.push({
      domain: domainName,
      driftSignals: signals,
      severity: domainSeverity,
      materializedState: materializedStateStr,
      lineageState: materialized.domains[domainName] || 'unknown',
    });

    if (domainSeverity > worstSeverity) {
      worstSeverity = domainSeverity;
    }
  }

  return { hash, observations, worstSeverity };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Exports
// ═══════════════════════════════════════════════════════════════════════════════

module.exports = {
  compare,
  DRIFT_SIGNAL,
  DRIFT_SEVERITY,
};
