// control-plane/governance/interpreters/fsm-telemetry-interpreter.js
// FSM Telemetry Interpreter: bounded telemetry filtering for FSM consumers.
//
// Owns: filtering telemetry data specific to FSM domain operations.
// Does NOT own: HSM telemetry, reconciliation telemetry, cross-domain telemetry.
//
// FSM telemetry is scoped to domain-specific execution signals:
// - State transitions within the FSM's domain
// - Authority changes for entities within the FSM's jurisdiction
// - Execution outcomes (attempt, completion, failure) for domain operations
//
// Filter criteria: domain field matches FSM's jurisdiction

const metricsSubstrate = require('../../../substrates/metrics-substrate');

const FSM_DOMAINS = {
  acquisition: 'acquisition',
  publishing: 'publishing',
  scheduling: 'scheduling',
  dedup: 'dedup',
};

/**
 * Get telemetry signals filtered to a specific FSM domain.
 *
 * @param {string} domainName — 'acquisition' | 'publishing' | 'scheduling'
 * @returns {Promise<object>} domain-specific health signals
 */
async function getFSMTelemetry(domainName) {
  if (!FSM_DOMAINS[domainName]) {
    console.warn(`[fsm-telemetry-interpreter] Unknown FSM domain: ${domainName}`);
    return { domain: null, signals: {} };
  }

  const allSignals = await metricsSubstrate.getHealthSignals();
  const domainBreakdown = await metricsSubstrate.getDomainBreakdown(domainName);

  return {
    domain: domainName,
    signals: {
      healthStatus: allSignals.healthStatus,
      totalSignals: allSignals.totalSignals,
      domainBreakdown,
    },
  };
}

/**
 * Get FSM-specific execution signals.
 *
 * @param {string} domainName
 * @returns {Promise<object>} execution telemetry for domain
 */
async function getFSMExecutionSignals(domainName) {
  if (!FSM_DOMAINS[domainName]) {
    return { domain: null, executions: {} };
  }

  const domainBreakdown = await metricsSubstrate.getDomainBreakdown(domainName);

  return {
    domain: domainName,
    executions: {
      total: domainBreakdown.total || 0,
      success: domainBreakdown.success || 0,
      failed: domainBreakdown.failed || 0,
      retrying: domainBreakdown.retrying || 0,
    },
  };
}

/**
 * Check if telemetry signal is within FSM jurisdiction.
 *
 * @param {object} signal — telemetry signal
 * @param {string} domainName — FSM domain to check
 * @returns {boolean}
 */
function isInFSMJurisdiction(signal, domainName) {
  return signal.domain === domainName || signal.source === domainName;
}

/**
 * Get account health signals for a specific domain.
 *
 * @param {string} domainName
 * @param {string} accountId
 * @returns {Promise<object>} account health for domain
 */
async function getAccountHealthForDomain(domainName, accountId) {
  return metricsSubstrate.getAccountHealth(accountId);
}

module.exports = {
  FSM_DOMAINS,
  getFSMTelemetry,
  getFSMExecutionSignals,
  isInFSMJurisdiction,
  getAccountHealthForDomain,
};
