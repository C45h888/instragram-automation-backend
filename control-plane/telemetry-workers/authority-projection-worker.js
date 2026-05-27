// control-plane/telemetry-workers/authority-projection-worker.js
// Authority Projection Worker: synthesizes authorityContinuity, escalationPressure,
// authorityOscillation, sovereigntyStress.
//
// Owns: semantic synthesis of authority continuity signals from domain metrics.
// Does NOT own: governance decisions, lineage, FSM semantics.
//
// Projection Type: AUTHORITY_PROJECTION
// Source: metricsSubstrate domain breakdown + lineage ledger domain state
//
// Determinism contract:
//   same domainBreakdown + same domainLineage + same version
//   = ALWAYS same authorityContinuity, escalationPressure, authorityOscillation

const { BaseProjectionWorker } = require('./base-projection-worker');
const metricsSubstrate = require('../../substrates/metrics-substrate');
const lineageLedger = require('../governance/lineage-ledger');

const PROJECTION_TYPE = 'AUTHORITY_PROJECTION';
const POLL_INTERVAL_MS = 30_000;

class AuthorityProjectionWorker extends BaseProjectionWorker {
  constructor() {
    super({ pollIntervalMs: POLL_INTERVAL_MS, workerName: 'authority-projection-worker' });
    this._oscillationHistory = new Map(); // domain → [timestamps]
  }

  get _projectType() {
    return PROJECTION_TYPE;
  }

  get _domain() {
    return 'authority';
  }

  /**
   * @returns {Promise<object>}
   */
  async _getSnapshotSource() {
    const domainBreakdown = metricsSubstrate.getDomainBreakdown();
    const lineageEntries = await lineageLedger.getLineage(100);
    return {
      domainBreakdown,
      lineageEntries,
      tickCount: this._tickCount,
      windowOpenedAt: Date.now() - POLL_INTERVAL_MS,
      entryCount: domainBreakdown ? Object.keys(domainBreakdown).length : 0,
      noiseGate: !domainBreakdown || Object.keys(domainBreakdown).length === 0,
    };
  }

  /**
   * Synthesize authorityContinuity, escalationPressure, authorityOscillation,
   * sovereigntyStress.
   *
   * @param {object} projectionState
   * @param {object} signals
   * @returns {object}
   */
  _synthesize(projectionState, signals) {
    const { domainBreakdown, lineageEntries } = signals;

    const domains = ['acquisition', 'publishing', 'scheduling', 'dedup'];
    const authorityMap = {};
    let maxEscalationPressure = 0;
    let maxAuthorityOscillation = 0;

    for (const domain of domains) {
      const breakdown = domainBreakdown[domain] || { total: 0, failed: 0, failureRate: 0 };
      const lineageDomain = this._getLastDomainState(lineageEntries, domain);

      // Authority stability: failureRate inverse
      const authorityStability = breakdown.total > 0
        ? Math.max(0, 1.0 - breakdown.failureRate * 2)
        : 1.0;

      // Authority oscillation detection
      const oscillation = this._detectOscillation(domain, breakdown, lineageDomain);

      authorityMap[domain] = {
        state: lineageDomain || 'IDLE',
        authorityStability,
        failureRate: breakdown.failureRate,
        oscillationDetected: oscillation > 0.5,
      };

      maxAuthorityOscillation = Math.max(maxAuthorityOscillation, oscillation);
      maxEscalationPressure = Math.max(maxEscalationPressure, breakdown.failureRate);
    }

    const authorityContinuity = this._deriveAuthorityContinuity(authorityMap);
    const escalationPressure = maxEscalationPressure;
    const sovereigntyStress = maxAuthorityOscillation;

    return {
      authorityContinuity,
      escalationPressure,
      authorityOscillation: maxAuthorityOscillation,
      sovereigntyStress,
      domains: authorityMap,
    };
  }

  _getLastDomainState(lineageEntries, domain) {
    if (!lineageEntries || lineageEntries.length === 0) return null;
    for (let i = lineageEntries.length - 1; i >= 0; i--) {
      const entry = lineageEntries[i];
      if (entry.domain === domain && entry.nextState) {
        return entry.nextState;
      }
    }
    return null;
  }

  _detectOscillation(domain, breakdown, lastDomainState) {
    if (breakdown.total < 5) return 0;
    if (!this._oscillationHistory.has(domain)) {
      this._oscillationHistory.set(domain, []);
    }
    const history = this._oscillationHistory.get(domain);

    // Track failure rate transitions
    if (lastDomainState) {
      history.push({ failureRate: breakdown.failureRate, ts: Date.now() });
      if (history.length > 10) history.splice(0, history.length - 10);
    }

    // Oscillation: failure rate toggling between high and low
    if (history.length >= 4) {
      const recent = history.slice(-4);
      const high = recent.filter(h => h.failureRate >= 0.3).length;
      const low = recent.filter(h => h.failureRate < 0.1).length;
      if (high >= 2 && low >= 2) return 0.7;
    }
    return breakdown.failureRate >= 0.5 ? breakdown.failureRate : 0;
  }

  _deriveAuthorityContinuity(authorityMap) {
    const values = Object.values(authorityMap);
    if (values.length === 0) return 1.0;
    const avgStability = values.reduce((sum, d) => sum + d.authorityStability, 0) / values.length;
    return avgStability;
  }

  _computeConfidence(signals) {
    if (signals.noiseGate) return 0.0;
    const { domainBreakdown } = signals;
    const total = domainBreakdown
      ? Object.values(domainBreakdown).reduce((s, d) => s + d.total, 0)
      : 0;
    if (total < 10) return 0.3;
    return 1.0;
  }

  _computeIntegrityScore(signals) {
    const { lineageEntries } = signals;
    if (!lineageEntries || lineageEntries.length === 0) return 0.5;
    const recentCount = lineageEntries.filter(e => e.domain === 'authority').length;
    return recentCount > 0 ? 1.0 : 0.5;
  }
}

module.exports = AuthorityProjectionWorker;
