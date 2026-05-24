// control-plane/execution/ig-fetcher-insights.js
// IG Fetcher: insights domain — pure transport only.
//
// Owns: calling Instagram Graph API for media feed and insights.
// Does NOT own: DB writes, credential resolution, retry logic, orchestration.
//
// Pure transport. No DB writes. No persistence. No loops. No spawning.
// Called by orchestrator under HSM governance.

const transport = require('../../substrates/transport/instagram');

/**
 * Fetches the business account's media feed list.
 *
 * @param {string} accountId
 * @param {string|number} since - ISO date or unix timestamp
 * @param {string|number} until
 * @param {{ igUserId: string, pageToken: string }} credentials
 * @returns {Promise<{success: boolean, mediaList: Array, count: number, _usagePct?: number, error?: string}>}
 */
async function fetchMediaFeed(accountId, since, until, credentials) {
  return transport.fetchMediaFeed(accountId, since, until, credentials);
}

/**
 * Fetches per-media insights for a list of media objects.
 *
 * @param {Array} mediaList - media objects with .id and .media_type
 * @param {string} pageToken
 * @returns {Promise<Array>} mediaInsights entries
 */
async function fetchMediaInsightsBatch(mediaList, pageToken) {
  return transport.fetchMediaInsightsBatch(mediaList, pageToken);
}

module.exports = { fetchMediaFeed, fetchMediaInsightsBatch };
