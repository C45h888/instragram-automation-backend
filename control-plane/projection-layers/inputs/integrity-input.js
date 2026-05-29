
const lineageLedger = require('../../governance/lineage-ledger');

async function getNormalizedInputWindow({ pollIntervalMs, tickCount }) {
  const ledgerSize = await lineageLedger.getSize();
  const recent = await lineageLedger.getLineage(200);
  const divergences = recent
    .filter((e) => e.entryType === 'divergence')
    .map((e) => ({
      category: e.divergenceCategory,
      type: e.nextState,
      timestamp: e.timestamp,
      details: e.divergenceDetails,
    }));

  return {
    ledgerSize,
    divergences,
    tickCount,
    windowOpenedAt: Date.now() - pollIntervalMs,
    entryCount: ledgerSize,
    noiseGate: ledgerSize < 1,
  };
}

module.exports = { getNormalizedInputWindow };
