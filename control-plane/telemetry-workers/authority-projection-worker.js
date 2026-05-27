// control-plane/telemetry-workers/authority-projection-worker.js
// Authority Projection Worker: synthesizes authorityContinuity, escalationPressure,
// authorityOscillation, sovereigntyStress, RATE_LIMIT_PRESSURE.
//
// Owns: semantic synthesis of authority continuity signals from domain metrics.
// Does NOT own: governance decisions, lineage, FSM semantics.
//
// Projection Type: AUTHORITY_PROJECTION
// Source: observability snapshots + lineage ledger
//
// Semantic synthesis ownership:
//   RATE_LIMIT_PRESSURE  — inferred from cooldownMs > 0 on active circuit breakers
//   authorityOscillation — derived from failure rate toggling across domains
//   sovereigntyStress    — composite of authority oscillation magnitude
//
// Determinism contract:
//   same domainBreakdown + same domainLineage + same version
//   = ALWAYS same authorityContinuity, escalationPressure, authorityOscillation

const { BaseProjectionWorker } = require('./base-projection-worker');

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
   * Fetch authority synthesis sources from the observability plane.
   * The worker reads from immutable observability snapshots, NOT live substrate memory.
   * This preserves replay determinism — same snapshot + same version = same output.
   *
   * @returns {Promise<object>}
   */
  async _getSnapshotSource() {
    let crossDomain = {};
    let domainBreakdown = {};
    let rateLimitWindows = [];

    try {
      // eslint-disable-next-line global-require
      const observability = require('../observability');

      // Cross-domain state from observability plane (immutable snapshot)
      crossDomain = observability.getCrossDomain(['acquisition', 'publishing', 'scheduling', 'dedup']) || {};

      // Derive domain breakdown from cross-domain snapshot
      for (const [domain, state] of Object.entries(crossDomain)) {
        if (typeof state === 'object' && state !== null) {
          domainBreakdown[domain] = state;
        } else {
          domainBreakdown[domain] = { total: 0, failed: 0, failureRate: 0, state };
        }
      }

      // Read RAW_RATE_LIMIT_WINDOW entries from the transition log.
      // This reads from the observability plane, NOT direct retry._rateLimitedAccounts.
      const logSize = observability.query.getLogSize();
      const allEntries = [];
      for (let i = Math.max(0, logSize - 500); i < logSize; i++) {
        const result = observability.query.getEntriesSince(i);
        if (result && result.entries) {
          for (const entry of result.entries) {
            if (entry.raw && entry.raw.entryType === 'RAW_RATE_LIMIT_WINDOW') {
              rateLimitWindows.push(entry.raw);
            }
          }
        }
      }
    } catch {
      // observability may not be initialized yet — degraded but not broken
    }

    return {
      crossDomain,
      domainBreakdown,
      rateLimitWindows,
      tickCount: this._tickCount,
      windowOpenedAt: Date.now() - POLL_INTERVAL_MS,
      entryCount: domainBreakdown ? Object.keys(domainBreakdown).length : 0,
      noiseGate: !domainBreakdown || Object.keys(domainBreakdown).length === 0,
    };
  }

  /**
   * Synthesize authorityContinuity, escalationPressure, authorityOscillation,
   * sovereigntyStress, RATE_LIMIT_PRESSURE.
   *
   * RATE_LIMIT_PRESSURE is inferred here from RAW_RATE_LIMIT_WINDOW entries
   * in the observability plane. The adapter emits raw circuit breaker windows;
   * this worker synthesizes semantic pressure from them.
   *
   * @param {object} projectionState
   * @param {object} signals — { crossDomain, domainBreakdown, rateLimitWindows, ... }
   * @returns {object}
   */
  _synthesize(projectionState, signals) {
    const { crossDomain, domainBreakdown, rateLimitWindows } = signals;

    const domains = ['acquisition', 'publishing', 'scheduling', 'dedup'];
    const authorityMap = {};
    let maxEscalationPressure = 0;
    let maxAuthorityOscillation = 0;

    for (const domain of domains) {
      const breakdown = domainBreakdown[domain] || { total: 0, failed: 0, failureRate: 0 };
      const crossDomainState = crossDomain[domain] || null;

      // Authority stability: failureRate inverse
      const authorityStability = breakdown.total > 0
        ? Math.max(0, 1.0 - breakdown.failureRate * 2)
        : 1.0;

      // Authority oscillation detection
      const oscillation = this._detectOscillation(domain, breakdown);

      authorityMap[domain] = {
        state: crossDomainState || 'IDLE',
        authorityStability,
        failureRate: breakdown.failureRate,
        oscillationDetected: oscillation > 0.5,
      };

      maxAuthorityOscillation = Math.max(maxAuthorityOscillation, oscillation);
      maxEscalationPressure = Math.max(maxEscalationPressure, breakdown.failureRate);
    }

    // RATE_LIMIT_PRESSURE: semantic synthesis from RAW_RATE_LIMIT_WINDOW entries.
    // The adapter emits raw circuit breaker windows; this worker infers pressure.
    const rateLimitPressure = this._deriveRateLimitPressure(rateLimitWindows);

    const authorityContinuity = this._deriveAuthorityContinuity(authorityMap);
    const escalationPressure = maxEscalationPressure;
    const sovereigntyStress = maxAuthorityOscillation;

    return {
      authorityContinuity,
      escalationPressure,
      authorityOscillation: maxAuthorityOscillation,
      sovereigntyStress,
      rateLimitPressure,
      domains: authorityMap,
    };
  }

  /**
   * Derive RATE_LIMIT_PRESSURE from RAW_RATE_LIMIT_WINDOW entries.
   * Semantic synthesis owned by this worker — not the adapter.
   *
   * @param {Array} rateLimitWindows — RAW_RATE_LIMIT_WINDOW entries from observability
   * @returns {number} 0.0 – 1.0
   */
  _deriveRateLimitPressure(rateLimitWindows) {
    if (!rateLimitWindows || rateLimitWindows.length === 0) return 0;
    // Pressure based on number of accounts under active circuit breaker
    // and their remaining cooldown duration
    let totalCooldown = 0;
    for (const win of rateLimitWindows) {
      if (win.cooldownMs > 0) {
        totalCooldown += win.cooldownMs;
      }
    }
    const avgCooldown = totalCooldown / rateLimitWindows.length;
    const accountPressure = Math.min(1.0, rateLimitWindows.length / 10);
    const timePressure = Math.min(1.0, avgCooldown / 60_000);
    return Math.min(1.0, accountPressure * 0.6 + timePressure * 0.4);
  }

  _detectOscillation(domain, breakdown) {
    if (breakdown.total < 5) return 0;
    if (!this._oscillationHistory.has(domain)) {
      this._oscillationHistory.set(domain, []);
    }
    const history = this._oscillationHistory.get(domain);

    // Track failure rate transitions
    history.push({ failureRate: breakdown.failureRate, ts: Date.now() });
    if (history.length > 10) history.splice(0, history.length - 10);

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
    const { crossDomain } = signals;
    const domainCount = crossDomain ? Object.keys(crossDomain).length : 0;
    if (domainCount === 0) return 0.5;
    return Math.min(1.0, domainCount / 5);
  }
}

module.exports = AuthorityProjectionWorker;
