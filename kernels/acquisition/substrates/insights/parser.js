// substrates/insights/parser.js
// Insights parser: validates raw insights API shapes from IG API.
//
// Owns: extracting structured data from account insights and media insights.
// Does NOT own: API transport, DB writes, schema normalization, orchestration.

/**
 * Parse account-level insights response.
 *
 * @param {{ v1Data: Array, v2Data: Array, period: object }} raw
 * @returns {{ v1Data: Array, v2Data: Array, period: object }}
 */
function parseAccountInsights(raw) {
  return {
    v1Data: Array.isArray(raw.v1Data) ? raw.v1Data : [],
    v2Data: Array.isArray(raw.v2Data) ? raw.v2Data : [],
    period: raw.period || {},
  };
}

/**
 * Parse per-media insights batch results.
 * Filters out entries with no media_id (failed lookups or corrupted rows).
 *
 * @param {Array} raw — output from fetchMediaInsightsBatch
 * @returns {Array}
 */
function parseMediaInsights(raw) {
  if (!Array.isArray(raw)) return [];
  return raw.filter(m => m && typeof m.media_id === 'string' && m.media_id.length > 0);
}

module.exports = { parseAccountInsights, parseMediaInsights };
