// control-plane/governance/interpreters/engagement-telemetry-adapter.js
// Engagement Telemetry Adapter: bounded raw telemetry pre-processor.
//
// Owns: polling raw signals from metricsSubstrate, quota substrate, and retry
//        substrate; applying noise gates; emitting raw bounded telemetry windows.
//
// Does NOT own: governance decisions, FSM lifecycle, semantic classification,
//               threshold inference, pressure synthesis, credential cache clearing.
//
// Architecture:
//   Raw Substrates → Adapter (polls at 30s) → Observability Plane
//                                                      ↓
//                              Projection Workers (semantic synthesis layer)
//                                                      ↓
//                              Lineage Worker (Layer A — persists)
//
// The adapter is a bounded telemetry normalizer. It ONLY:
//   - polls raw substrate state
//   - applies noise gates
//   - emits raw bounded telemetry windows
//
// The adapter MUST NEVER:
//   - classify semantic meaning (RETRY_PRESSURE, QUOTA_PRESSURE, etc.)
//   - infer runtime pressure or state
//   - dispatch governance actions
//   - apply threshold policy
//
// All semantic synthesis is exclusively the domain of projection workers.

const metricsSubstrate = require('../../../substrates/metrics-substrate');
const quota = require('../../../substrates/quota');

const POLL_INTERVAL_MS = 30_000;
const NOISE_GATE_MIN_SAMPLES = 5;

let _pollTimer = null;
let _running = false;
let _startedAt = null;

// ── Core polling tick ────────────────────────────────────────────────────────

function _tick() {
  _emitMetricsWindow();
  _emitQuotaWindow();
  _emitRateLimitWindow();
}

/**
 * Emit a raw metrics telemetry window from the metrics substrate.
 * No threshold inference. No semantic classification.
 */
function _emitMetricsWindow() {
  const signals = metricsSubstrate.getHealthSignals();
  const { total, failed, failureRate } = signals;

  // Noise gate — too few samples to be meaningful
  if (total < NOISE_GATE_MIN_SAMPLES) return;

  // Adapter only: emit raw bounded window, no inference
  _emitTelemetryWindow({
    signalType: 'RAW_METRICS_WINDOW',
    domain: 'engagement',
    raw: {
      total,
      failed,
      failureRate,
      windowMs: signals.windowMs,
    },
  });
}

/**
 * Emit raw quota usage windows for each account.
 * No tiering. No semantic pressure classification.
 */
function _emitQuotaWindow() {
  const entries = quota._quotaUsage;
  if (!entries || entries.size === 0) return;

  const now = Date.now();
  for (const [accountId, entry] of entries) {
    const age = now - entry.recordedAt;
    if (age > quota.QUOTA_USAGE_TTL_MS) continue; // stale reading, skip

    // Adapter only: emit raw bounded window with usage percentage
    _emitTelemetryWindow({
      signalType: 'RAW_QUOTA_WINDOW',
      domain: 'engagement',
      entityId: accountId,
      raw: {
        accountId,
        usagePct: entry.pct,
        ageMs: age,
      },
    });
  }
}

/**
 * Emit raw rate limit windows from retry substrate circuit breaker state.
 * No semantic pressure classification. No governance dispatch.
 */
function _emitRateLimitWindow() {
  const retry = require('../../../substrates/retry');
  const rateLimitedAccounts = retry._rateLimitedAccounts;
  if (!rateLimitedAccounts || rateLimitedAccounts.size === 0) return;

  const now = Date.now();
  for (const [accountId, unblockedAt] of rateLimitedAccounts) {
    if (now < unblockedAt) {
      _emitTelemetryWindow({
        signalType: 'RAW_RATE_LIMIT_WINDOW',
        domain: 'engagement',
        entityId: accountId,
        raw: {
          accountId,
          cooldownMs: unblockedAt - now,
          triggeredAt: unblockedAt - (unblockedAt - now),
        },
      });
    }
  }
}

/**
 * Emit a bounded telemetry window to the observability plane.
 * The adapter only emits raw windows — semantic synthesis happens in projection workers.
 */
function _emitTelemetryWindow({ signalType, domain, entityId = 'engagement-adapter', raw }) {
  try {
    const observability = require('../../observability/emitters/transition-emitter');
    observability.transition({
      domain,
      entity: 'telemetry_window',
      entityId: entityId,
      previousState: null,
      nextState: signalType,
      authority: 'engagement-telemetry-adapter',
      raw: {
        signalType,
        adapter: 'engagement-telemetry-adapter',
        windowOpenedAt: _startedAt,
        emittedAt: Date.now(),
        ...raw,
      },
    });
  } catch (err) {
    console.warn('[engagement-telemetry-adapter] Window emit error:', err.message);
  }
}

// ── Public API ─────────────────────────────────────────────────────────────────

/**
 * Start the engagement telemetry adapter.
 * Poll interval: 30 seconds.
 */
function start(pollIntervalMs = POLL_INTERVAL_MS) {
  if (_running) {
    console.warn('[engagement-telemetry-adapter] Already running');
    return;
  }

  _running = true;
  _startedAt = Date.now();

  // Run initial tick immediately to avoid waiting for first interval
  _tick();

  _pollTimer = setInterval(_tick, pollIntervalMs);
  _pollTimer.unref();

  console.log(`[engagement-telemetry-adapter] Started — polling every ${pollIntervalMs}ms`);
}

/**
 * Stop the adapter gracefully.
 */
function stop() {
  if (!_running) return;
  _running = false;
  if (_pollTimer) {
    clearInterval(_pollTimer);
    _pollTimer = null;
  }
  console.log('[engagement-telemetry-adapter] Stopped');
}

/**
 * Return adapter health signals.
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
  getHealth,
};
