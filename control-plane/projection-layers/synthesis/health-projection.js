function synthesize(_projectionState, signals) {
  const { healthSignals } = signals;
  if (!healthSignals) {
    return { degradationSignals: {}, failureRate: 0, runtimeEntropy: 0, operationalStress: 0, retryPressure: 0 };
  }
  const { total, failed, failureRate } = healthSignals;
  const retryPressure = deriveRetryPressure(failureRate, total);
  const runtimeEntropy = deriveRuntimeEntropy(signals.failureHistory || [], failureRate);
  const operationalStress = deriveOperationalStress(healthSignals);
  const degradationSignals = deriveDegradationSignals(failureRate, total, operationalStress);
  return {
    degradationSignals,
    failureRate,
    runtimeEntropy,
    operationalStress,
    retryPressure,
    totalSamples: total,
    failedSamples: failed,
  };
}

function deriveRetryPressure(failureRate, total) {
  if (total === 0) return 0;
  if (failureRate >= 0.5) return 0.8;
  if (failureRate >= 0.3) return 0.5;
  if (failureRate >= 0.15) return 0.2;
  return 0;
}

function deriveRuntimeEntropy(history, currentFailureRate) {
  const series = [...history, currentFailureRate].slice(-20);
  if (series.length < 3) return 0;
  const mean = series.reduce((s, v) => s + v, 0) / series.length;
  const variance = series.reduce((s, v) => s + Math.pow(v - mean, 2), 0) / series.length;
  const stdDev = Math.sqrt(variance);
  return Math.min(1.0, stdDev * 3);
}

function deriveOperationalStress(healthSignals) {
  const failureComponent = healthSignals.failureRate;
  const retryComponent = healthSignals.total > 0 ? (healthSignals.failed / Math.max(1, healthSignals.total)) * 0.4 : 0;
  return Math.min(1.0, failureComponent + retryComponent);
}

function deriveDegradationSignals(failureRate, total, operationalStress) {
  const out = {};
  if (total === 0) out.IDLE = true;
  else if (failureRate >= 0.6) out.CRITICAL = true;
  else if (failureRate >= 0.4) out.ELEVATED = true;
  else if (failureRate >= 0.2) out.DEGRADED = true;
  else out.HEALTHY = true;
  if (operationalStress >= 0.7) out.HIGH_STRESS = true;
  return out;
}

function computeConfidence(signals) {
  if (signals.noiseGate) return 0.0;
  if (signals.healthSignals.total < 5) return 0.0;
  if (signals.healthSignals.total < 20) return 0.5;
  return 1.0;
}

function computeIntegrityScore(signals) {
  const { healthSignals, lastFailureRate = 0 } = signals;
  if (!healthSignals) return 0.0;
  const delta = Math.abs(healthSignals.failureRate - lastFailureRate);
  return delta <= 0.3 ? 1.0 : Math.max(0, 1.0 - delta * 2);
}

module.exports = { synthesize, computeConfidence, computeIntegrityScore };
