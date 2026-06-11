// graph-capability-kernel/substrates/workers/dependency-recovery-worker.js
// Dependency Recovery Worker — handles Graph API outages, endpoint instability,
// latency events, network interruptions, and temporary service degradation.
// Dependency-level recovery, not account-level recovery.
//
// Consumes: ig-reliability-substrate §11 dependency health (dependencyState,
//           platformFailure, endpointUnstable, elevatedLatency,
//           reclassifiedToDependency)
//
// Owns:
//   - Circuit breaker: open on 3+ consecutive dependency failures
//   - Endpoint health scoring: per-endpoint success/failure ratio
//   - Latency trending: moving average of executionMs
//   - Network interruption detection (ECONNREFUSED, ETIMEDOUT patterns)
//   - Temporary service degradation handling
//   - Dependency-level backoff (not account-level)
//   - Recovery probing: half-open circuit → test call → close or re-open
//
// Does NOT own:
//   - Account-level recovery (Token Health Worker owns)
//   - Error classification (ig-reliability-substrate §11)
//   - Raw API calls (vault.* owns)
//
// States: HEALTHY → DEGRADED → CIRCUIT_OPEN → RECOVERING
//
// Membrane interface: start(governance), stop(), isStarted()

const igReliability = require('../../../substrates/ig-reliability-substrate');
const fsm = require('../../fsm');

// ── Circuit breaker constants ─────────────────────────────────────────────

const CIRCUIT_OPEN_THRESHOLD = 3;       // consecutive failures to open
const CIRCUIT_HALF_OPEN_AFTER_MS = 30000; // 30s before probing
const CIRCUIT_CLOSE_ON_SUCCESS = 2;     // successes to close from half-open
const ENDPOINT_WINDOW_SIZE = 50;         // samples for endpoint health

// ── Dependency Recovery Worker ────────────────────────────────────────────

class DependencyRecoveryWorker {
  constructor() {
    this._started = false;
    this._governance = null;

    // Circuit breaker state
    this._circuitState = 'HEALTHY';  // HEALTHY | DEGRADED | CIRCUIT_OPEN | RECOVERING
    this._consecutiveFailures = 0;
    this._circuitOpenedAt = null;
    this._recoverySuccesses = 0;
    this._lastFailureAt = null;
    this._lastFailureError = null;

    // Endpoint health: endpoint → { successes, failures, latencies[] }
    this._endpointHealth = new Map();

    // Global latency trending: moving window of recent execution times
    this._recentLatencies = [];
  }

  // ── Membrane interface ──────────────────────────────────────────────────

  start(governance) {
    if (this._started) return;
    this._started = true;
    this._governance = governance;

    governance.subscribeAction('DEPENDENCY_HEALTH_CHECK', (action) => {
      this.executeHealthCheck(action).catch(err => {
        console.error('[dependency-recovery] DEPENDENCY_HEALTH_CHECK failed:', err.message);
      });
    });

    console.log('[dependency-recovery] Membrane wired — subscribed to DEPENDENCY_HEALTH_CHECK');
  }

  stop() {
    this._started = false;
  }

  isStarted() {
    return this._started;
  }

  // ── Public: record API call outcome for circuit breaker ─────────────────

  /**
   * Record the outcome of a Graph API call. Updates circuit breaker and
   * endpoint health. Called by transport substrate after every IG call.
   *
   * @param {{ success: boolean, endpoint: string, executionMs: number|null,
   *           error: object|null }} outcome
   * @returns {{ circuitState: string, allowed: boolean }}
   */
  recordCall({ success, endpoint = 'unknown', executionMs = null, error = null }) {
    // Update endpoint health
    if (!this._endpointHealth.has(endpoint)) {
      this._endpointHealth.set(endpoint, { successes: 0, failures: 0, latencies: [] });
    }
    const health = this._endpointHealth.get(endpoint);
    if (success) {
      health.successes++;
    } else {
      health.failures++;
    }
    if (executionMs != null) {
      health.latencies.push(executionMs);
      if (health.latencies.length > ENDPOINT_WINDOW_SIZE) health.latencies.shift();
      this._recentLatencies.push(executionMs);
      if (this._recentLatencies.length > ENDPOINT_WINDOW_SIZE) this._recentLatencies.shift();
    }

    // Circuit breaker logic
    if (this._circuitState === 'CIRCUIT_OPEN') {
      // Check if enough time has passed to try recovery
      if (this._circuitOpenedAt && (Date.now() - this._circuitOpenedAt) > CIRCUIT_HALF_OPEN_AFTER_MS) {
        this._circuitState = 'RECOVERING';
        this._recoverySuccesses = 0;
        console.log('[dependency-recovery] Circuit half-open — probing');
        return { circuitState: this._circuitState, allowed: true };
      }
      return { circuitState: this._circuitState, allowed: false };
    }

    if (this._circuitState === 'RECOVERING') {
      if (success) {
        this._recoverySuccesses++;
        if (this._recoverySuccesses >= CIRCUIT_CLOSE_ON_SUCCESS) {
          this._circuitState = 'HEALTHY';
          this._consecutiveFailures = 0;
          console.log('[dependency-recovery] Circuit closed — recovery successful');
        }
      } else {
        // Failure during recovery → re-open
        this._circuitState = 'CIRCUIT_OPEN';
        this._circuitOpenedAt = Date.now();
        console.log('[dependency-recovery] Circuit re-opened — probe failed');
      }
      return { circuitState: this._circuitState, allowed: this._circuitState === 'RECOVERING' };
    }

    // HEALTHY or DEGRADED
    if (!success) {
      this._consecutiveFailures++;
      this._lastFailureAt = Date.now();
      this._lastFailureError = error;

      if (this._consecutiveFailures >= CIRCUIT_OPEN_THRESHOLD) {
        this._circuitState = 'CIRCUIT_OPEN';
        this._circuitOpenedAt = Date.now();
        console.log('[dependency-recovery] Circuit OPEN — 3+ consecutive failures');

        // Emit degraded envelope to FSM
        fsm.dispatch({
          type: 'CAPABILITY_OBSERVATION',
          envelope: {
            businessAccountId: '__system__',
            detection: {
              isValid: true,
              reliabilityImpaired: true,
              reason: 'dependency_circuit_open',
            },
          },
        });

        return { circuitState: this._circuitState, allowed: false };
      } else if (this._consecutiveFailures >= 2) {
        this._circuitState = 'DEGRADED';
      }
    } else {
      // Success resets the counter
      this._consecutiveFailures = 0;
      if (this._circuitState === 'DEGRADED') {
        this._circuitState = 'HEALTHY';
      }
    }

    return { circuitState: this._circuitState, allowed: this._circuitState !== 'CIRCUIT_OPEN' };
  }

  // ── Public: periodic health check ───────────────────────────────────────

  async executeHealthCheck(action = {}) {
    const endorsementHealth = this._getEndpointHealthSummary();
    const avgLatency = this._computeAvgLatency();

    const state = {
      circuitState: this._circuitState,
      consecutiveFailures: this._consecutiveFailures,
      circuitOpenedAt: this._circuitOpenedAt,
      lastFailureAt: this._lastFailureAt,
      avgLatencyMs: avgLatency,
      endpointHealth: endorsementHealth,
    };

    // Rate endpoints by failure ratio
    const degradedEndpoints = [];
    for (const [endpoint, health] of Object.entries(endorsementHealth)) {
      const total = health.successes + health.failures;
      if (total > 0 && health.failures / total > 0.3) {
        degradedEndpoints.push({ endpoint, failureRatio: health.failures / total });
      }
    }

    if (degradedEndpoints.length > 0) {
      console.log(`[dependency-recovery] Degraded endpoints: ${degradedEndpoints.map(e => e.endpoint).join(', ')}`);
    }

    console.log(`[dependency-recovery] Circuit: ${state.circuitState} | failures: ${state.consecutiveFailures} | avgLatency: ${avgLatency.toFixed(0)}ms`);

    return state;
  }

  // ── Internal ────────────────────────────────────────────────────────────

  _getEndpointHealthSummary() {
    const summary = {};
    for (const [endpoint, health] of this._endpointHealth) {
      summary[endpoint] = {
        successes: health.successes,
        failures: health.failures,
        avgLatency: health.latencies.length > 0
          ? health.latencies.reduce((a, b) => a + b, 0) / health.latencies.length
          : null,
      };
    }
    return summary;
  }

  _computeAvgLatency() {
    if (this._recentLatencies.length === 0) return 0;
    return this._recentLatencies.reduce((a, b) => a + b, 0) / this._recentLatencies.length;
  }
}

module.exports = DependencyRecoveryWorker;
