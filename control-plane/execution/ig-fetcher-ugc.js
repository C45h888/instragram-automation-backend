// control-plane/execution/ig-fetcher-ugc.js
// IG Fetcher: UGC domain — pure transport only.
//
// Owns: calling Instagram Graph API for tagged media and hashtag search.
// Does NOT own: DB writes, credential resolution, retry logic, orchestration.
//
// Pure transport. No DB writes. No persistence. No loops. No spawning.
// Called by orchestrator under HSM governance.

const transport = require('../../substrates/transport/instagram');

/**
 * Fetches posts where the business account is tagged.
 *
 * @param {string} accountId
 * @param {number} [limit=25]
 * @param {{ igUserId: string, pageToken: string }} credentials
 * @returns {Promise<{success: boolean, records: Array, count: number, paging?: object, _usagePct?: number, error?: string}>}
 */
async function fetchTaggedMedia(accountId, limit, credentials) {
  return transport.fetchTaggedMedia(accountId, limit, credentials);
}

/**
 * Searches for media by hashtag.
 *
 * @param {string} accountId
 * @param {string} hashtag - with or without # prefix
 * @param {number} [limit=25]
 * @param {{ igUserId: string, pageToken: string }} credentials
 * @returns {Promise<{success: boolean, records: Array, rawMedia: Array, hashtagId?: string, cleanHashtag?: string, count: number, _usagePct?: number, error?: string}>}
 */
async function fetchHashtagMedia(accountId, hashtag, limit, credentials) {
  return transport.fetchHashtagMedia(accountId, hashtag, limit, credentials);
}

module.exports = { fetchTaggedMedia, fetchHashtagMedia };
