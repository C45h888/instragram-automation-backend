const metricsSubstrate = require('../../../../scheduling-kernel/substrates/metrics');

async function getNormalizedInputWindow({ pollIntervalMs, tickCount }) {
  const healthSignals = metricsSubstrate.getHealthSignals();
  const domainBreakdown = metricsSubstrate.getDomainBreakdown();
  return {
    healthSignals,
    domainBreakdown,
    tickCount,
    windowOpenedAt: Date.now() - pollIntervalMs,
    entryCount: healthSignals.total,
    noiseGate: healthSignals.total < 5,
  };
}

module.exports = { getNormalizedInputWindow };
