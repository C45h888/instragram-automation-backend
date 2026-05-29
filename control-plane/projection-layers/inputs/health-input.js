const metricsSubstrate = require('../../../substrates/metrics-substrate');

async function getNormalizedInputWindow({ pollIntervalMs, tickCount, failureHistory = [], lastFailureRate = 0 }) {
  const healthSignals = metricsSubstrate.getHealthSignals();
  const domainBreakdown = metricsSubstrate.getDomainBreakdown();
  let observabilitySnapshot = {};
  try {
    const observability = require('../../observability');
    observabilitySnapshot = observability.query.getFullSnapshot() || {};
  } catch (_) {}

  return {
    healthSignals,
    rawSignals: { healthSignals, domainBreakdown },
    observabilitySnapshot,
    failureHistory,
    lastFailureRate,
    tickCount,
    windowOpenedAt: Date.now() - pollIntervalMs,
    entryCount: healthSignals.total,
    noiseGate: healthSignals.total < 5,
  };
}

module.exports = { getNormalizedInputWindow };
