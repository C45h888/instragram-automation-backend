// control-plane/governance/interpreters/hsm-telemetry-interpreter.js
// HSM Telemetry Interpreter: full telemetry observability for HSM consumers.
//
// Owns: providing HSM with complete telemetry access for hierarchical analysis.
// Does NOT own: FSM domain filtering, reconciliation telemetry.
//
// HSM requires full telemetry observability to:
// - Evaluate state hierarchies across domain boundaries
// - Analyze cross-domain execution patterns
// - Detect hierarchical state anomalies
//
// Filter criteria: NONE (pass-through — full observability)

const metricsSubstrate = require('../../../substrates/metrics-substrate');

/**
 * Get all telemetry signals (full observability for HSM).
 *
 * @returns {Promise<object>} complete health signals
 */
async function getHSMTelemetry() {
  const signals = await metricsSubstrate.getHealthSignals();
  return {
    observability: 'full',
    signals,
  };
}

/**
 * Get telemetry across all domains for hierarchical analysis.
 *
 * @returns {Promise<object>} cross-domain telemetry
 */
async function getCrossDomainTelemetry() {
  const signals = await metricsSubstrate.getHealthSignals();
  const domains = ['acquisition', 'publishing', 'scheduling'];
  const breakdown = {};

  for (const domain of domains) {
    breakdown[domain] = await metricsSubstrate.getDomainBreakdown(domain);
  }

  return {
    observability: 'full',
    signals,
    domainBreakdown: breakdown,
  };
}

/**
 * Get hierarchical telemetry signals (execution across domains).
 *
 * @returns {Promise<object>} hierarchical telemetry view
 */
async function getHierarchicalTelemetry() {
  const signals = await metricsSubstrate.getHealthSignals();
  const domains = ['acquisition', 'publishing', 'scheduling'];

  const hierarchicalView = {
    global: signals,
    domains: {},
  };

  for (const domain of domains) {
    hierarchicalView.domains[domain] = await metricsSubstrate.getDomainBreakdown(domain);
  }

  return hierarchicalView;
}

/**
 * Get aggregate execution health across all domains.
 *
 * @returns {Promise<object>} aggregate health signals
 */
async function getAggregateHealth() {
  const signals = await metricsSubstrate.getHealthSignals();
  return {
    aggregateHealth: signals.healthStatus,
    totalSignals: signals.totalSignals,
    retryPressure: signals.retryPressure,
    executionHealth: signals.executionHealth,
  };
}

module.exports = {
  getHSMTelemetry,
  getCrossDomainTelemetry,
  getHierarchicalTelemetry,
  getAggregateHealth,
};
