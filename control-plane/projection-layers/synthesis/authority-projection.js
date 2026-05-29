function synthesize(_projectionState, signals) {
  const { crossDomain, domainBreakdown, rateLimitWindows } = signals;
  const domains = ['acquisition', 'publishing', 'scheduling', 'dedup'];
  const authorityMap = {};
  let maxEscalationPressure = 0;
  let maxAuthorityOscillation = 0;

  for (const domain of domains) {
    const breakdown = domainBreakdown[domain] || { total: 0, failed: 0, failureRate: 0 };
    const crossDomainState = crossDomain[domain] || null;
    const authorityStability = breakdown.total > 0 ? Math.max(0, 1.0 - breakdown.failureRate * 2) : 1.0;
    const oscillation = breakdown.failureRate >= 0.5 ? breakdown.failureRate : 0;
    authorityMap[domain] = {
      state: crossDomainState || 'IDLE',
      authorityStability,
      failureRate: breakdown.failureRate,
      oscillationDetected: oscillation > 0.5,
    };
    maxAuthorityOscillation = Math.max(maxAuthorityOscillation, oscillation);
    maxEscalationPressure = Math.max(maxEscalationPressure, breakdown.failureRate);
  }

  const rateLimitPressure = deriveRateLimitPressure(rateLimitWindows);
  const authorityContinuity = Object.values(authorityMap).length > 0
    ? Object.values(authorityMap).reduce((sum, d) => sum + d.authorityStability, 0) / Object.values(authorityMap).length
    : 1.0;

  return {
    authorityContinuity,
    escalationPressure: maxEscalationPressure,
    authorityOscillation: maxAuthorityOscillation,
    sovereigntyStress: maxAuthorityOscillation,
    rateLimitPressure,
    domains: authorityMap,
  };
}

function deriveRateLimitPressure(rateLimitWindows) {
  if (!rateLimitWindows || rateLimitWindows.length === 0) return 0;
  let totalCooldown = 0;
  for (const win of rateLimitWindows) {
    if (win.cooldownMs > 0) totalCooldown += win.cooldownMs;
  }
  const avgCooldown = totalCooldown / rateLimitWindows.length;
  const accountPressure = Math.min(1.0, rateLimitWindows.length / 10);
  const timePressure = Math.min(1.0, avgCooldown / 60000);
  return Math.min(1.0, accountPressure * 0.6 + timePressure * 0.4);
}

function computeConfidence(signals) {
  if (signals.noiseGate) return 0.0;
  const { domainBreakdown } = signals;
  const total = domainBreakdown ? Object.values(domainBreakdown).reduce((s, d) => s + (d.total || 0), 0) : 0;
  if (total < 10) return 0.3;
  return 1.0;
}

function computeIntegrityScore(signals) {
  const { crossDomain } = signals;
  const domainCount = crossDomain ? Object.keys(crossDomain).length : 0;
  if (domainCount === 0) return 0.5;
  return Math.min(1.0, domainCount / 5);
}

module.exports = { synthesize, computeConfidence, computeIntegrityScore };
