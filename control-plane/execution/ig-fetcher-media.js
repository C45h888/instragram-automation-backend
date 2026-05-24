// control-plane/execution/ig-fetcher-media.js
// IG Fetcher: media domain — pure transport only.
//
// Owns: calling Instagram Graph API for business media posts.
// Does NOT own: DB writes, credential resolution, retry logic, orchestration.
//
// Pure transport. No DB writes. No persistence. No loops. No spawning.
// Called by orchestrator under HSM governance.

const transport = require('../../substrates/transport/instagram');

/**
 * Fetches the business account's own media posts.
 *
 * @param {string} accountId
 * @param {number} [limit=50]
 * @returns {Promise<{success: boolean, posts: Array, count: number, _usagePct?: number, error?: string}>}
 */
async function fetchBusinessPosts(accountId, limit) {
  return transport.fetchBusinessPosts(accountId, limit);
}

module.exports = { fetchBusinessPosts };
