// control-plane/governance/interpreters/engagement-telemetry-interpreter.js
// Engagement Telemetry Interpreter: bounded raw telemetry pre-processor.
//
// Owns: polling raw signals from metricsSubstrate, quota substrate, and retry
//        substrate; applying noise gates and thresholds; emitting pre-interpreted
//        engagement domain signals to the observability plane.
//
// Does NOT own: governance decisions, FSM lifecycle, credential cache clearing,
//               execution mechanics.
//
// Architecture:
//   Raw Substrates → Interpreter (polls at 30s) → Observability Plane → Lineage Worker
//                                                                  ↓
//                                             engagement-fsm receives interpreted signals only
//
// The interpreter is a periodic worker, NOT a domain FSM. It has no governance
// authority. It performs mechanical translation: raw counts → domain signals.
// All governance semantics live in engagement-fsm.
//
// Noise gate: total < 5 samples → no signal (too few to be meaningful)
// Threshold: failureRate >= 0.5 → RETRY_PRESSURE
// Quota tiers: pct >= 80 → CRITICAL, pct >= 50 → ELEVATED
// Rate limit: any account in retry._rateLimitedAccounts → RATE_LIMIT_PRESSURE

const metricsSubstrate = require('../../../substrates/metrics-substrate');
const quota = require('../../../substrates/quota');

const POLL_INTERVAL_MS = 30_000;
const NOISE_GATE_MIN_SAMPLES = 5;
const RETRY_PRESSURE_THRESHOLD = 0.5;

let _pollTimer = null;
let _running = false;
let _startedAt = null;
let _governance = null;

// ── Governance reference (set at wire time) ──────────────────────────────────

function setGovernance(gov) {
  _governance = gov;
}

// ── Core polling tick ────────────────────────────────────────────────────────

function _tick() {
  _evaluateMetricsPressure();
  _evaluateQuotaPressure();
  _evaluateRateLimitPressure();
}

/**
 * Evaluate retry pressure from raw metrics substrate signals.
 * Emits RETRY_PRESSURE to observability plane when threshold is met.
 */
function _evaluateMetricsPressure() {
  const signals = metricsSubstrate.getHealthSignals();
  const { total, failed, failureRate } = signals;

  // Noise gate — too few samples to be meaningful
  if (total < NOISE_GATE_MIN_SAMPLES) return;

  // Threshold evaluation — interpreter owns this policy, not the substrate
  if (failureRate >= RETRY_PRESSURE_THRESHOLD) {
    _emitInterpretedSignal({
      signalType: 'RETRY_PRESSURE',
      domain: 'engagement',
      raw: {
        total,
        failed,
        failureRate,
        windowMs: signals.windowMs,
        threshold: RETRY_PRESSURE_THRESHOLD,
      },
    });
  }
}

/**
 * Evaluate quota pressure from raw quota substrate data.
 * Emits QUOTA_PRESSURE to observability plane when threshold is met.
 */
function _evaluateQuotaPressure() {
  const entries = quota._quotaUsage;
  if (!entries || entries.size === 0) return;

  for (const [accountId, entry] of entries) {
    const age = Date.now() - entry.recordedAt;
    if (age > quota.QUOTA_USAGE_TTL_MS) continue; // stale reading, skip

    const pct = entry.pct;
    let tier = 'NORMAL';
    if (pct >= 80) tier = 'CRITICAL';
    else if (pct >= 50) tier = 'ELEVATED';

    if (tier !== 'NORMAL') {
      _emitInterpretedSignal({
        signalType: 'QUOTA_PRESSURE',
        domain: 'engagement',
        entityId: accountId,
        raw: {
          accountId,
          usagePct: pct,
          tier,
          threshold: tier === 'CRITICAL' ? 80 : 50,
          ageMs: age,
        },
      });
    }
  }
}

/**
 * Evaluate rate limit pressure from retry substrate's circuit breaker state.
 * For any account with an active (non-expired) circuit breaker, emits RATE_LIMIT_PRESSURE.
 * Also dispatches CLEAR_CREDENTIAL_CACHE upward through governance.
 */
function _evaluateRateLimitPressure() {
  const retry = require('../../../substrates/retry');
  const rateLimitedAccounts = retry._rateLimitedAccounts;
  if (!rateLimitedAccounts || rateLimitedAccounts.size === 0) return;

  const now = Date.now();
  for (const [accountId, unblockedAt] of rateLimitedAccounts) {
    if (now < unblockedAt) {
      _emitInterpretedSignal({
        signalType: 'RATE_LIMIT_PRESSURE',
        domain: 'engagement',
        entityId: accountId,
        raw: {
          accountId,
          cooldownMs: unblockedAt - now,
          triggeredAt: unblockedAt - (unblockedAt - now),
        },
      });

      // CLEAR_CREDENTIAL_CACHE flows through governance — this is the governance path
      // The interpreter dispatches the action upward; CK routes to the appropriate handler
      if (_governance) {
        _governance.dispatch({
          type: 'CLEAR_CREDENTIAL_CACHE',
          accountId,
          reason: 'rate_limit_detected_by_interpreter',
        });
      }
    }
  }
}

/**
 * Emit an interpreted signal to the observability plane.
 * Lineage worker consumes from observability plane and persists to lineage ledger.
 * No governance decision is made here — signal is raw domain data for FSM consumption.
 */
function _emitInterpretedSignal({ signalType, domain, entityId = 'engagement-interpreter', raw }) {
  try {
    const observability = require('../../observability/emitters/transition-emitter');
    observability.transition({
      domain,
      entity: 'interpreted_signal',
      entityId: entityId,
      previousState: null,
      nextState: signalType,
      authority: 'engagement-telemetry-interpreter',
      raw: {
        signalType,
        interpreter: 'engagement-telemetry-interpreter',
        interpretedAt: Date.now(),
        ...raw,
      },
    });
  } catch (err) {
    console.warn('[engagement-telemetry-interpreter] Signal emit error:', err.message);
  }
}

// ── Public API ─────────────────────────────────────────────────────────────────

/**
 * Start the engagement telemetry interpreter.
 * Poll interval: 30 seconds.
 *
 * @param {number} [pollIntervalMs=30000]
 */
function start(pollIntervalMs = POLL_INTERVAL_MS) {
  if (_running) {
    console.warn('[engagement-telemetry-interpreter] Already running');
    return;
  }

  _running = true;
  _startedAt = Date.now();

  // Run initial tick immediately to avoid waiting for first interval
  _tick();

  _pollTimer = setInterval(_tick, pollIntervalMs);
  _pollTimer.unref();

  console.log(`[engagement-telemetry-interpreter] Started — polling every ${pollIntervalMs}ms`);
}

/**
 * Stop the interpreter gracefully.
 */
function stop() {
  if (!_running) return;
  _running = false;
  if (_pollTimer) {
    clearInterval(_pollTimer);
    _pollTimer = null;
  }
  console.log('[engagement-telemetry-interpreter] Stopped');
}

/**
 * Return interpreter health signals.
 */
function getHealth() {
  return {
    running: _running,
    uptimeMs: _startedAt ? Date.now() - _startedAt : 0,
    pollIntervalMs: POLL_INTERVAL_MS,
  };
}

module.exports = {
  start,
  stop,
  setGovernance,
  getHealth,
};
