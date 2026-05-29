function synthesize(_projectionState, signals) {
  const { crossDomain, lineageState, previousConvergenceConfidence = 1.0 } = signals;
  const domainInstability = deriveDomainInstability(crossDomain, lineageState);
  const governancePressure = deriveGovernancePressure(lineageState, domainInstability);
  const systemicStress = deriveSystemicStress(lineageState);
  const convergenceConfidence = deriveConvergenceConfidence(lineageState, previousConvergenceConfidence);
  return {
    governancePressure,
    systemicStress,
    convergenceConfidence,
    domainInstability,
    globalState: lineageState.globalState || 'BOOTING',
    domainStates: lineageState.domains || {},
  };
}

function deriveDomainInstability(_crossDomain, lineageState) {
  const expectedStates = ['ACQUISITION_ACTIVE', 'PUBLISHING_ACTIVE', 'SCHEDULING_ACTIVE', 'IDLE', 'RUNNING'];
  const domains = lineageState.domains || {};
  let instability = 0;
  for (const state of Object.values(domains)) {
    if (!expectedStates.includes(state) && state !== 'BOOTING' && state !== 'ERROR') instability += 0.2;
    if (state === 'ERROR' || state === 'HALTED') instability += 0.5;
  }
  return Math.min(1.0, instability);
}

function deriveGovernancePressure(lineageState, domainInstability) {
  const globalState = lineageState.globalState || 'BOOTING';
  if (globalState === 'DEGRADED') return Math.min(1.0, 0.7 + domainInstability * 0.3);
  if (globalState === 'RECOVERY') return Math.min(1.0, 0.5 + domainInstability * 0.5);
  if (globalState === 'HEALTHY') return domainInstability * 0.5;
  return 0.3;
}

function deriveSystemicStress(lineageState) {
  const globalState = lineageState.globalState || 'BOOTING';
  if (globalState === 'HALTED' || globalState === 'ERROR') return 1.0;
  if (globalState === 'DEGRADED') return 0.7;
  if (globalState === 'RECOVERY') return 0.4;
  if (globalState === 'BOOTING') return 0.2;
  return 0.0;
}

function deriveConvergenceConfidence(lineageState, prevConfidence) {
  const globalState = lineageState.globalState || 'BOOTING';
  if (globalState === 'HEALTHY') return Math.min(1.0, prevConfidence + 0.05);
  if (globalState === 'RECOVERY') return Math.max(0.3, prevConfidence - 0.1);
  if (globalState === 'DEGRADED') return Math.max(0.2, prevConfidence - 0.15);
  if (globalState === 'HALTED') return 0.0;
  return prevConfidence;
}

function computeConfidence(signals) {
  if (signals.noiseGate) return 0.0;
  if (signals.entryCount < 10) return 0.3;
  if (signals.entryCount < 50) return 0.7;
  return 1.0;
}

function computeIntegrityScore(signals) {
  const { lineageState } = signals;
  if (!lineageState || !lineageState.globalState) return 0.0;
  const domainCount = Object.keys(lineageState.domains || {}).length;
  if (domainCount === 0) return 0.5;
  return Math.min(1.0, domainCount / 5);
}

module.exports = { synthesize, computeConfidence, computeIntegrityScore };
