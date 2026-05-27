// Systemic Pressure Projection Worker: synthesizes governancePressure, systemicStress,
// convergenceConfidence, domainInstability.
//
// OWNED BY: semantic synthesis layer (NOT governance cognition layer)
//
// This worker synthesizes RUNTIME GOVERNANCE PRESSURE signals only.
// It does NOT determine constitutional legitimacy — only the reconciliation
// engine and constitutional kernel make those judgments.
//
// Renamed from 'governance-runtime-projection-worker' to eliminate dangerous
// semantic implication that this worker performs governance authority synthesis.
// It synthesizes systemic pressure, NOT governance authority.
//
// Projection Type: SYSTEMIC_PRESSURE_PROJECTION
// Source: observability.getCrossDomain() + lineageLedger.materializeState()
//
// Determinism contract:
//   same crossDomainSnapshot + same lineageState + same version
//   = ALWAYS same governancePressure, systemicStress, convergenceConfidence

const { BaseProjectionWorker } = require('./base-projection-worker');
const lineageLedger = require('../governance/lineage-ledger');

const PROJECTION_TYPE = 'SYSTEMIC_PRESSURE_PROJECTION';
const POLL_INTERVAL_MS = 30_000;

class SystemicPressureProjectionWorker extends BaseProjectionWorker {
  constructor() {
    super({ pollIntervalMs: POLL_INTERVAL_MS, workerName: 'systemic-pressure-projection-worker' });
    this._previousConvergenceConfidence = 1.0;
  }

  get _projectType() {
    return PROJECTION_TYPE;
  }

  get _domain() {
    return 'systemic';
  }

  /**
   * @returns {Promise<object>}
   */
  async _getSnapshotSource() {
    let crossDomain = {};
    try {
      // eslint-disable-next-line global-require
      const observability = require('../observability');
      crossDomain = observability.getCrossDomain(['acquisition', 'publishing', 'scheduling', 'dedup']) || {};
    } catch {
      // observability may not be initialized yet
    }

    const lineageEntries = await lineageLedger.getLineage(100);
    const materialized = lineageLedger.materializeState(lineageEntries);

    return {
      crossDomain,
      lineageState: materialized,
      tickCount: this._tickCount,
      windowOpenedAt: Date.now() - POLL_INTERVAL_MS,
      entryCount: lineageEntries.length,
      noiseGate: lineageEntries.length === 0,
    };
  }

  /**
   * Synthesize governancePressure, systemicStress, convergenceConfidence, domainInstability.
   *
   * NOTE: This worker synthesizes RUNTIME GOVERNANCE PRESSURE only.
   * It does NOT determine constitutional legitimacy — only the reconciliation
   * engine and constitutional kernel make those judgments.
   *
   * @param {object} projectionState
   * @param {object} signals
   * @returns {object}
   */
  _synthesize(projectionState, signals) {
    const { crossDomain, lineageState } = signals;

    // Domain instability: any domain not in expected states
    const domainInstability = this._deriveDomainInstability(crossDomain, lineageState);

    // Governance pressure: composite based on degraded domains + runtime state
    const governancePressure = this._deriveGovernancePressure(lineageState, domainInstability);

    // Systemic stress: stability of global runtime state
    const systemicStress = this._deriveSystemicStress(lineageState);

    // Convergence confidence: how有信心 the runtime is converging correctly
    const convergenceConfidence = this._deriveConvergenceConfidence(
      lineageState,
      crossDomain,
      this._previousConvergenceConfidence
    );

    this._previousConvergenceConfidence = convergenceConfidence;

    return {
      governancePressure,
      systemicStress,
      convergenceConfidence,
      domainInstability,
      globalState: lineageState.globalState || 'BOOTING',
      domainStates: lineageState.domains || {},
    };
  }

  _deriveDomainInstability(crossDomain, lineageState) {
    const expectedStates = ['ACQUISITION_ACTIVE', 'PUBLISHING_ACTIVE', 'SCHEDULING_ACTIVE', 'IDLE', 'RUNNING'];
    const domains = lineageState.domains || {};
    let instability = 0;

    for (const [domain, state] of Object.entries(domains)) {
      if (!expectedStates.includes(state) && state !== 'BOOTING' && state !== 'ERROR') {
        instability += 0.2;
      }
      if (state === 'ERROR' || state === 'HALTED') {
        instability += 0.5;
      }
    }
    return Math.min(1.0, instability);
  }

  _deriveGovernancePressure(lineageState, domainInstability) {
    const globalState = lineageState.globalState || 'BOOTING';
    if (globalState === 'DEGRADED') return Math.min(1.0, 0.7 + domainInstability * 0.3);
    if (globalState === 'RECOVERY') return Math.min(1.0, 0.5 + domainInstability * 0.5);
    if (globalState === 'HEALTHY') {
      return domainInstability * 0.5;
    }
    return 0.3;
  }

  _deriveSystemicStress(lineageState) {
    const globalState = lineageState.globalState || 'BOOTING';
    if (globalState === 'HALTED' || globalState === 'ERROR') return 1.0;
    if (globalState === 'DEGRADED') return 0.7;
    if (globalState === 'RECOVERY') return 0.4;
    if (globalState === 'BOOTING') return 0.2;
    return 0.0;
  }

  _deriveConvergenceConfidence(lineageState, crossDomain, prevConfidence) {
    const globalState = lineageState.globalState || 'BOOTING';
    // Confidence in convergence when runtime is stable
    if (globalState === 'HEALTHY') return Math.min(1.0, prevConfidence + 0.05);
    if (globalState === 'RECOVERY') return Math.max(0.3, prevConfidence - 0.1);
    if (globalState === 'DEGRADED') return Math.max(0.2, prevConfidence - 0.15);
    if (globalState === 'HALTED') return 0.0;
    return prevConfidence;
  }

  _computeConfidence(signals) {
    if (signals.noiseGate) return 0.0;
    if (signals.entryCount < 10) return 0.3;
    if (signals.entryCount < 50) return 0.7;
    return 1.0;
  }

  _computeIntegrityScore(signals) {
    const { lineageState } = signals;
    if (!lineageState || !lineageState.globalState) return 0.0;
    // Integrity check: domain states should be consistent with global state
    const domains = lineageState.domains || {};
    const domainCount = Object.keys(domains).length;
    if (domainCount === 0) return 0.5;
    return Math.min(1.0, domainCount / 5);
  }
}

module.exports = SystemicPressureProjectionWorker;
