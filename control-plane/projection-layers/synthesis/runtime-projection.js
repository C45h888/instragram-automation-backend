function synthesize(projectionState, signals) {
  const { healthSignals, domainBreakdown } = signals;
  const { total, failed, failureRate } = healthSignals;
  const runtimeState = deriveRuntimeState(failureRate, total);
  const executionPressure = Math.min(1.0, (failureRate * 0.7) + (total > 100 ? 0.3 : total / 333));
  const retryPressure = deriveRetryPressure(failed, total, domainBreakdown);
  const cadenceHealth = deriveCadenceHealth(domainBreakdown, projectionState);
  return {
    runtimeState,
    executionPressure,
    retryPressure,
    cadenceHealth,
    totalSamples: total,
    failedSamples: failed,
    failureRate,
  };
}

function deriveRuntimeState(failureRate, total) {
  if (total === 0) return 'IDLE';
  if (failureRate >= 0.6) return 'CRITICAL';
  if (failureRate >= 0.4) return 'DEGRADED';
  if (failureRate >= 0.2) return 'ELEVATED';
  return 'HEALTHY';
}

function deriveRetryPressure(failed, total, domainBreakdown) {
  if (total === 0) return 0;
  let oscillation = 0;
  if (domainBreakdown) {
    const rates = Object.values(domainBreakdown).map((d) => d.failureRate);
    const above = rates.filter((r) => r >= 0.3).length;
    oscillation = rates.length > 0 ? above / rates.length : 0;
  }
  const basePressure = failed / Math.max(1, total);
  return Math.min(1.0, basePressure + oscillation * 0.3);
}

function deriveCadenceHealth(domainBreakdown, projectionState) {
  if (!domainBreakdown) return 1.0;
  const rates = Object.values(domainBreakdown).map((d) => d.failureRate);
  if (rates.length === 0) return 1.0;
  const prev = projectionState.recentFailureRates || [];
  const allRates = [...prev, ...rates];
  if (allRates.length < 4) return 1.0;
  const mean = allRates.reduce((s, r) => s + r, 0) / allRates.length;
  const variance = allRates.reduce((s, r) => s + Math.pow(r - mean, 2), 0) / allRates.length;
  const stdDev = Math.sqrt(variance);
  return Math.max(0, 1.0 - stdDev * 4);
}

function computeConfidence(signals) {
  if (signals.noiseGate) return 0.0;
  if (signals.healthSignals.total < 5) return 0.0;
  if (signals.healthSignals.total < 20) return 0.5;
  return 1.0;
}

function computeIntegrityScore(signals) {
  const { healthSignals } = signals;
  if (!healthSignals) return 0.0;
  const fr = healthSignals.failureRate;
  if (isNaN(fr) || fr < 0 || fr > 1) return 0.0;
  return 1.0;
}

module.exports = { synthesize, computeConfidence, computeIntegrityScore };
