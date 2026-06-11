// graph-capability-kernel/substrates/workers/quota-intelligence-worker.js
// Quota Intelligence Worker — monitors Instagram Graph API usage telemetry,
// predicts quota exhaustion, and gates operations on quota state.
//
// Consumes: ig-reliability-substrate §5 quota analysis (x-app-usage,
//           x-business-use-case-usage, x-page-usage headers → pressureLevel,
//           pressureScore, throttledCallTypes, recommendedConcurrencyDelta)
//
// Owns:
//   - Per-call x-app-usage tracking via response headers
//   - Account-level consumption aggregation
//   - Business Use Case utilization by call_type
//   - Application-level utilization scoring
//   - Request density by endpoint
//   - Quota recovery window estimation
//   - Predicted exhaustion timestamp
//   - Account saturation risk scoring
//   - Critical operations remaining counter
//   - Pressure-level boundary crossing detection
//
// Does NOT own:
//   - Error classification (ig-reliability-substrate §2)
//   - Rate-limit recovery policy (FSM owns)
//   - Raw API calls (vault.* owns)
//   - DB reads/writes (governed through CK)
//
// States: MONITORING → THROTTLING → DEFERRING → LOCKED_OUT
//
// Membrane interface: start(governance), stop(), isStarted()

const igReliability = require('../../../substrates/ig-reliability-substrate');
const fsm = require('../../fsm');

// ── Private: ring buffer for rolling window tracking ───────────────────────

class RollingWindow {
  constructor(windowMs) {
    this._windowMs = windowMs;
    this._samples = [];
  }

  push(usage) {
    const now = Date.now();
    this._samples.push({ ts: now, usage });
    this._prune(now);
  }

  count() {
    this._prune(Date.now());
    return this._samples.length;
  }

  sum(field) {
    this._prune(Date.now());
    return this._samples.reduce((s, e) => s + (e.usage[field] || 0), 0);
  }

  ratePerMinute(field) {
    const n = this.count();
    if (n === 0) return 0;
    const total = this.sum(field);
    const spanMs = this._windowMs;
    return (total / spanMs) * 60000;
  }

  _prune(now) {
    const cutoff = now - this._windowMs;
    this._samples = this._samples.filter(s => s.ts > cutoff);
  }
}

// ── Quota Intelligence Worker ─────────────────────────────────────────────

class QuotaIntelligenceWorker {
  constructor() {
    this._started = false;
    this._governance = null;

    // Rolling windows for consumption tracking
    this._hourlyWindow = new RollingWindow(60 * 60 * 1000);     // 1h
    this._dailyWindow = new RollingWindow(24 * 60 * 60 * 1000);  // 24h

    // Endpoint hit counters
    this._endpointHits = new Map();

    // Account saturation tracking: businessAccountId → { calls, lastSeen }
    this._accountSaturation = new Map();

    // Quota state
    this._state = 'MONITORING';
    this._lastPressureTransition = null;
    this._criticalOpsRemaining = null;
    this._predictedExhaustionAt = null;
  }

  // ── Membrane interface ──────────────────────────────────────────────────

  start(governance) {
    if (this._started) return;
    this._started = true;
    this._governance = governance;

    governance.subscribeAction('RUN_QUOTA_CHECK', (action) => {
      this.executeQuotaCheck(action).catch(err => {
        console.error('[quota-intelligence] RUN_QUOTA_CHECK failed:', err.message);
      });
    });

    console.log('[quota-intelligence] Membrane wired — subscribed to RUN_QUOTA_CHECK');
  }

  stop() {
    this._started = false;
  }

  isStarted() {
    return this._started;
  }

  // ── Public: record API response headers (called by transport layer) ─────

  /**
   * Record quota telemetry from an API response. Called by transport
   * substrate after every IG Graph API call. Consumes response headers
   * and feeds into the rolling window.
   *
   * @param {{ headers: object, endpoint: string, businessAccountId: string|null }} ctx
   */
  recordResponse({ headers = {}, endpoint = 'unknown', businessAccountId = null }) {
    if (!headers) return;

    const usage = {
      appUsage: headers['x-app-usage'] || null,
      businessUseCaseUsage: headers['x-business-use-case-usage'] || null,
      pageUsage: headers['x-page-usage'] || null,
    };

    this._hourlyWindow.push(usage);
    this._dailyWindow.push(usage);

    // Endpoint hit tracking
    const key = endpoint;
    this._endpointHits.set(key, (this._endpointHits.get(key) || 0) + 1);

    // Account saturation
    if (businessAccountId) {
      const entry = this._accountSaturation.get(businessAccountId) || { calls: 0, lastSeen: null };
      entry.calls++;
      entry.lastSeen = Date.now();
      this._accountSaturation.set(businessAccountId, entry);
    }

    // Evaluate pressure transition
    this._evaluatePressure();
  }

  // ── Public: execute periodic quota check ────────────────────────────────

  async executeQuotaCheck(action = {}) {
    const hourlyCount = this._hourlyWindow.count();
    const dailyCount = this._dailyWindow.count();

    const state = {
      state: this._state,
      hourlyRequestCount: hourlyCount,
      dailyRequestCount: dailyCount,
      topEndpoints: this._getTopEndpoints(5),
      saturatedAccounts: this._getSaturatedAccounts(),
      predictedExhaustionAt: this._predictedExhaustionAt,
      criticalOpsRemaining: this._criticalOpsRemaining,
      lastPressureTransition: this._lastPressureTransition,
    };

    console.log(`[quota-intelligence] State: ${state.state} | hourly: ${hourlyCount} | daily: ${dailyCount}`);

    // Emit envelope to FSM for observation
    fsm.dispatch({
      type: 'CAPABILITY_OBSERVATION',
      envelope: {
        businessAccountId: action.businessAccountId || '__system__',
        userId: null,
        scope: { grantedScopes: [], cacheAgeMs: 0 },
        detection: {
          isValid: true,
          reliabilityImpaired: this._state === 'LOCKED_OUT',
          reason: this._state !== 'MONITORING' ? `quota_state:${this._state}` : null,
        },
      },
    });

    return state;
  }

  // ── Internal: pressure evaluation ───────────────────────────────────────

  _evaluatePressure() {
    const hourlyRate = this._hourlyWindow.ratePerMinute('appUsage');
    const prevState = this._state;

    // Pressure thresholds (IG app-level: 200 calls/hour/user ≈ 3.3/min)
    // These are conservative defaults — the IG reliability substrate's
    // §5 provides real x-app-usage percentages for precision.
    if (hourlyRate > 200) {
      this._state = 'LOCKED_OUT';
    } else if (hourlyRate > 150) {
      this._state = 'DEFERRING';
    } else if (hourlyRate > 100) {
      this._state = 'THROTTLING';
    } else {
      this._state = 'MONITORING';
    }

    if (this._state !== prevState) {
      const now = new Date().toISOString();
      this._lastPressureTransition = now;

      // Fire THROTTLE action if entering DEFERRING or LOCKED_OUT
      if (this._state === 'DEFERRING' || this._state === 'LOCKED_OUT') {
        fsm.dispatch({
          type: 'CAPABILITY_OBSERVATION',
          envelope: {
            businessAccountId: '__system__',
            detection: {
              isValid: true,
              reliabilityImpaired: true,
              reason: `quota_pressure:${this._state}`,
              quotaState: this._state,
              hourlyRate,
            },
          },
        });
      }

      console.log(`[quota-intelligence] Pressure transition: ${prevState} → ${this._state} (rate: ${hourlyRate.toFixed(1)}/min)`);
    }
  }

  _getTopEndpoints(n) {
    const sorted = [...this._endpointHits.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, n);
    return sorted.map(([ep, count]) => ({ endpoint: ep, hits: count }));
  }

  _getSaturatedAccounts() {
    const saturated = [];
    for (const [baId, entry] of this._accountSaturation) {
      if (entry.calls > 50) {
        saturated.push({ businessAccountId: baId, calls: entry.calls });
      }
    }
    return saturated.sort((a, b) => b.calls - a.calls).slice(0, 10);
  }
}

module.exports = QuotaIntelligenceWorker;
