const lineageLedger = require('../../governance/lineage-ledger');

async function getNormalizedInputWindow({ pollIntervalMs, tickCount, previousConvergenceConfidence = 1.0 }) {
  let crossDomain = {};
  try {
    const observability = require('../../observability');
    crossDomain = observability.query.getCrossDomain(['acquisition', 'publishing', 'scheduling', 'dedup']) || {};
  } catch (_) {}
  const lineageEntries = await lineageLedger.getLineage(100);
  const lineageState = lineageLedger.materializeState(lineageEntries);
  return {
    crossDomain,
    lineageState,
    previousConvergenceConfidence,
    tickCount,
    windowOpenedAt: Date.now() - pollIntervalMs,
    entryCount: lineageEntries.length,
    noiseGate: lineageEntries.length === 0,
  };
}

module.exports = { getNormalizedInputWindow };
