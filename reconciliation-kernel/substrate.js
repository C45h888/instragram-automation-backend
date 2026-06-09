// reconciliation-kernel/substrate.js
// Reconciliation Substrate: operational boundary for the reconciliation worker.
//
// Owns ALL constitutional authority artifacts:
//   - dispatch (constitutional action fabric)
//   - checkpointer (canonical checkpoint authority)
//   - canCheckpoint() evaluation (governance gate)
//   - snapshot building (lineageLedger.getLineageWithHash())
//   - _buildSubstrateQueries() (dedup, retry, metrics, cadence, buffer)
//
// Does NOT own: worker logic (that lives in reconciliation-worker.js)
//
// Worker receives only: { entries, fsms, substrates } — data only
// Worker does NOT touch dispatch, checkpointer, governance decisions

const lineageLedger = require('../control-plane/governance/lineage-ledger');
const checkpointer = require('../control-plane/governance/lineage-checkpointer');
const ck = require('../control-plane/governance/constitutional-kernel');
const worker = require('./worker');
const ingressSubstrate = require('../control-plane/governance/ingress-consistency/substrate');

// ── Substrate Query Interface ────────────────────────────────────────────────

function _buildSubstrateQueries() {
  const dedupOrch = require('../../dedup-kernel/orchestrator');
  const metricsSubstrate = require('../../scheduling-kernel/substrates/metrics');
  const cadence = require('../scheduling-kernel/substrates/cadence/cadence');

  return {
    dedupIsInFlight: async (accountId, actionType, resourceId) => {
      return dedupOrch.checkDedup(accountId, actionType, resourceId, null);
    },
    retryInFlight: (accountId) => {
      // Canonical read: engagement-fsm owns circuit-breaker state.
      // CK exposes it via isCircuitBreakerActive().
      return ck.isCircuitBreakerActive(accountId);
    },
    bufferSnapshot: () => {
      const buffer = require('../control-plane/runtime/buffer');
      try {
        return buffer.snapshot ? buffer.snapshot() : { size: 0, flushing: false };
      } catch {
        return { size: 0, flushing: false };
      }
    },
    metricsSignals: () => {
      return metricsSubstrate.getHealthSignals ? metricsSubstrate.getHealthSignals() : {};
    },
    cadenceLastTick: () => {
      return cadence.lastTick ? cadence.lastTick() : null;
    },
    dedupSnapshot: () => {
      return dedupOrch.getInflightSnapshot();
    },
  };
}

// ── Checkpoint Gate Evaluation ───────────────────────────────────────────────

/**
 * Evaluate whether the runtime is in a constitutionally stable state
 * suitable for creating a checkpoint snapshot.
 *
 * Gates:
 *   G1: Governance must be HEALTHY
 *   G2: Reconciliation FSM must be IDLE
 *   G3: No active drift (consecutiveDrifted === 0)
 *   G4: No escalation signaled
 *   G5: Ingestion lag bounded (< 5 entry gap between log and ledger)
 *
 * @param {object} params
 * @param {string} params.currentState — CK global state
 * @param {Map}    params.fsms         — domain FSMs map
 * @returns {boolean}
 */
function canCheckpoint({ currentState, fsms }) {
  // G1: Governance must be HEALTHY
  if (currentState !== 'HEALTHY') return false;

  // G2: Reconciliation FSM must be IDLE
  const reconFsm = fsms ? fsms.get('reconciliation') : null;
  if (!reconFsm || reconFsm.getState() !== 'IDLE') return false;

  // G3: No active drift
  const health = reconFsm.getHealth ? reconFsm.getHealth() : {};
  if (health.signals && health.signals.consecutiveDrifted > 0) return false;

  // G4: No escalation signaled
  if (health.signals && health.signals.escalationSignaled) return false;

  // G5: Ingestion lag bounded — ingress consistency substrate
  try {
    const state = ingressSubstrate && ingressSubstrate.getIngressState
      ? ingressSubstrate.getIngressState()
      : null;

    if (!state) return false;           // substrate unavailable — fail closed
    if (state.stale) return false;      // stale sample — treat as uncertain
    if (state.status === 'CRITICAL' || state.status === 'DEGRADED') return false;
    if (state.lag > 5) return false;
  } catch (_) {
    return false;
  }

  return true;
}

// ── Reconciliation Cycle Trigger ─────────────────────────────────────────────

/**
 * Trigger a complete reconciliation cycle.
 *
 * This is the only public entry point for the reconciliation substrate.
 * Called by CK.triggerReconciliation() with HSM authorization.
 *
 * Lifecycle:
 *   1. Capture immutable constitutional snapshot (lineageLedger.getLineageWithHash())
 *   2. Build substrate query interface (dedup, retry, metrics, cadence, buffer)
 *   3. Call worker.run({ entries, fsms, substrates }) — data only, no authority
 *   4. Evaluate canCheckpoint() — substrate owns this decision
 *   5. If all gates pass → checkpointer.createSnapshot()
 *   6. Return result to CK (CK dispatches FSM transitions)
 *
 * @param {object} params
 * @param {Map}    params.fsms         — domain FSMs map (CK _domains)
 * @param {string} params.currentState — CK global state (_currentState)
 *
 * @returns {Promise<{ observations: Array, worstSeverity: number, hash: string, snapshotHash: string }>|null}
 */
async function triggerCycle({ fsms, currentState }) {
  // ── 1. Capture immutable constitutional snapshot ─────────────────────────
  let snapshot;
  try {
    snapshot = await lineageLedger.getLineageWithHash();
  } catch (err) {
    console.error('[reconciliation-substrate] Failed to capture constitutional snapshot:', err.message);
    return null;
  }

  const { entries, hash: snapshotHash } = snapshot;

  // ── 2. Build substrate query interface ────────────────────────────────────
  const substrates = _buildSubstrateQueries();

  // ── 3. Call worker — data only, no authority artifacts ───────────────────
  let results;
  try {
    results = await worker.run({ entries, fsms, substrates });
  } catch (err) {
    console.error('[reconciliation-substrate] Worker run failed:', err.message);
    return null;
  }

  // ── 4. Evaluate checkpoint gate ───────────────────────────────────────────
  if (canCheckpoint({ currentState, fsms })) {
    try {
      const reconFsm = fsms.get('reconciliation');
      checkpointer.createSnapshot({
        entries: entries.slice(-200),  // last 200 entries
        hash: snapshotHash,
        entryCount: entries.length,
        epochCount: reconFsm && typeof reconFsm.getEpochCount === 'function'
          ? reconFsm.getEpochCount() : 0,
      });
    } catch (e) {
      console.error('[reconciliation-substrate] Checkpoint creation failed:', e.message);
    }
  }

  // ── 5. Return result to CK ─────────────────────────────────────────────────
  return {
    observations: results.observations,
    worstSeverity: results.worstSeverity,
    hash: results.hash,
    snapshotHash,
  };
}

module.exports = {
  triggerCycle,
  canCheckpoint,
  _buildSubstrateQueries, // exported for testing only — do not call in production
};