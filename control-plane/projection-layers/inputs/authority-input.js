async function getNormalizedInputWindow({ pollIntervalMs, tickCount }) {
  let crossDomain = {};
  const domainBreakdown = {};
  const rateLimitWindows = [];

  try {
    const observability = require('../../observability');
    crossDomain = observability.query.getCrossDomain(['acquisition', 'publishing', 'scheduling', 'dedup']) || {};
    for (const [domain, state] of Object.entries(crossDomain)) {
      domainBreakdown[domain] = typeof state === 'object' && state !== null
        ? state
        : { total: 0, failed: 0, failureRate: 0, state };
    }
    const { entries } = observability.query.getEntriesSince(Math.max(0, observability.query.getLogSize() - 500));
    for (const entry of entries) {
      if (entry.raw && entry.raw.entryType === 'RAW_RATE_LIMIT_WINDOW') {
        rateLimitWindows.push(entry.raw);
      }
    }
  } catch (_) {}

  return {
    crossDomain,
    domainBreakdown,
    rateLimitWindows,
    tickCount,
    windowOpenedAt: Date.now() - pollIntervalMs,
    entryCount: Object.keys(domainBreakdown).length,
    noiseGate: Object.keys(domainBreakdown).length === 0,
  };
}

module.exports = { getNormalizedInputWindow };
