// control-plane/execution/ig-fetcher-messages.js
// IG Fetcher: messages domain — pure transport only.
//
// Owns: calling Instagram Graph API for DM conversations and messages.
// Does NOT own: DB writes, credential resolution, retry logic, orchestration.
//
// Pure transport. No DB writes. No persistence. No loops. No spawning.
// Called by orchestrator under HSM governance.

const transport = require('../../substrates/transport/instagram');

/**
 * Fetches DM conversations.
 *
 * @param {string} accountId
 * @param {number} [limit=20]
 * @param {{ igUserId: string, pageToken: string, pageId?: string }} credentials
 * @returns {Promise<{success: boolean, rawConversations: Array, igUserId: string, pageId: string|null, count: number, paging?: object, _usagePct?: number, error?: string}>}
 */
async function fetchConversations(accountId, limit, credentials) {
  return transport.fetchConversations(accountId, limit, credentials);
}

/**
 * Fetches messages for a single DM conversation.
 *
 * @param {string} accountId
 * @param {string} conversationId - Instagram thread ID
 * @param {number} [limit=20]
 * @param {{ igUserId: string, pageToken: string, pageId?: string }} credentials
 * @returns {Promise<{success: boolean, rawMessages: Array, igUserId: string, pageId: string|null, count: number, paging?: object, error?: string}>}
 */
async function fetchMessages(accountId, conversationId, limit, credentials) {
  return transport.fetchMessages(accountId, conversationId, limit, credentials);
}

module.exports = { fetchConversations, fetchMessages };
