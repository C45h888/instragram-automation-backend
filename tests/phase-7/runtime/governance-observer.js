/**
 * GovernanceObserver — Every CK/FSM decision recorded (Phase 7 contract §9, §15)
 * ═══════════════════════════════════════════════════════════════════════════════
 *
 * Records every governance decision: validation calls, dispatch outcomes,
 * FSM transitions. The constitutional test is that workers do NOT make
 * governance decisions; the observer makes that detectable.
 *
 * Usage:
 *   const obs = new GovernanceObserver();
 *   obs.attach(ck);
 *   ... do work ...
 *   const decisions = obs.decisions();
 *   obs.assertWorkersDidNotGovern(workerTracer);
 */

class GovernanceObserver {
  constructor() {
    this._decisions = [];
    this._counter = 0;
  }

  /** Wrap CK methods to observe all governance activity. */
  attach(ck) {
    if (!ck) return;
    const self = this;

    if (typeof ck.validate === 'function' && !ck._phase7_obs_validate) {
      const orig = ck.validate.bind(ck);
      ck.validate = function (...args) {
        const result = orig(...args);
        self._record({
          type: 'validate',
          source: 'CK',
          from: args[0],
          to: args[1],
          via: args[2] && args[2].type,
          result: result && (result.valid || result.success || result.accepted),
        });
        return result;
      };
      ck._phase7_obs_validate = true;
    }

    if (typeof ck.dispatch === 'function' && !ck._phase7_obs_dispatch) {
      const orig = ck.dispatch.bind(ck);
      ck.dispatch = function (evt) {
        self._record({
          type: 'dispatch',
          source: 'CK',
          via: evt && evt.type,
          accepted: true,
        });
        return orig(evt);
      };
      ck._phase7_obs_dispatch = true;
    }

    if (typeof ck.validateDomainTransition === 'function' && !ck._phase7_obs_vdt) {
      const orig = ck.validateDomainTransition.bind(ck);
      ck.validateDomainTransition = function (domain, from, to, evt) {
        const result = orig(domain, from, to, evt);
        self._record({
          type: 'validateDomainTransition',
          source: 'CK',
          domain,
          from,
          to,
          via: evt && evt.type,
          result: result && (result.valid || result.success || result.accepted),
        });
        return result;
      };
      ck._phase7_obs_vdt = true;
    }
  }

  _record(decision) {
    this._counter++;
    this._decisions.push({
      id: this._counter,
      timestamp: Date.now(),
      ...decision,
    });
  }

  decisions(filter = {}) {
    return this._decisions.filter((d) => {
      if (filter.type && d.type !== filter.type) return false;
      if (filter.source && d.source !== filter.source) return false;
      if (filter.domain && d.domain !== filter.domain) return false;
      return true;
    });
  }

  /**
   * Assert that no decision was made by a worker (i.e. source != CK and
   * source != a domain FSM). Workers must consult the CK, not act as it.
   */
  assertWorkersDidNotGovern(workerTracer) {
    const workerRecords = workerTracer.records();
    const workerNames = new Set(workerRecords.map((r) => r.worker).filter(Boolean));
    const violations = this._decisions.filter((d) => workerNames.has(d.source));
    if (violations.length > 0) {
      const err = new Error(
        `GovernanceObserver: ${violations.length} decision(s) made by worker(s)`
      );
      err.violations = violations;
      throw err;
    }
  }

  reset() {
    this._decisions = [];
    this._counter = 0;
  }

  get size() {
    return this._decisions.length;
  }
}

module.exports = { GovernanceObserver };
