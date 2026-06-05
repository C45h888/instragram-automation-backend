function synthesize(_projectionState, signals) {
  const { ledgerSize, divergences } = signals;
  const structuralAnomalies = (divergences || []).filter((d) => d.category === 'structural').slice(-20);
  const runtimeAnomalies = (divergences || []).filter((d) => d.category === 'runtime_interpretation').slice(-10);
  const transitionDiscontinuities = structuralAnomalies.length + runtimeAnomalies.length;
  const replayContinuity = deriveReplayContinuity(ledgerSize, _projectionState);
  const causationIntegrity = deriveCausationIntegrity(structuralAnomalies);
  const epochContinuity = deriveEpochContinuity(divergences);
  return {
    replayContinuity,
    causationIntegrity,
    epochContinuity,
    transitionDiscontinuities,
    structuralAnomalyCount: structuralAnomalies.length,
    runtimeAnomalyCount: runtimeAnomalies.length,
    totalLedgerEntries: ledgerSize,
  };
}

function deriveReplayContinuity(ledgerSize, projectionState) {
  const prev = projectionState.totalLedgerEntries || 0;
  if (ledgerSize === 0) return 1.0;
  const gap = ledgerSize - prev;
  if (gap === 0) return 0.5;
  if (gap < 0) return 0.2;
  return 1.0;
}

function deriveCausationIntegrity(structuralAnomalies) {
  const brokenChain = structuralAnomalies.filter((a) => a.type === 'BROKEN_CAUSATION_CHAIN');
  if (brokenChain.length === 0) return 1.0;
  return Math.max(0, 1.0 - brokenChain.length * 0.2);
}

function deriveEpochContinuity(divergences) {
  if (!divergences || divergences.length === 0) return 1.0;
  const epochGaps = divergences.filter((d) => d.type === 'EPOCH_GAP');
  return Math.max(0, 1.0 - epochGaps.length * 0.25);
}

function computeConfidence(signals) {
  if (signals.noiseGate) return 0.0;
  if (signals.ledgerSize < 10) return 0.3;
  if (signals.ledgerSize < 50) return 0.7;
  return 1.0;
}

function computeIntegrityScore(signals) {
  const { divergences, ledgerSize } = signals;
  if (!divergences) return 1.0;
  const structuralCount = divergences.filter((d) => d.category === 'structural').length;
  if (structuralCount === 0) return 1.0;
  const anomalyRate = structuralCount / Math.max(1, ledgerSize);
  return Math.max(0, 1.0 - anomalyRate * 10);
}

module.exports = { synthesize, computeConfidence, computeIntegrityScore };
