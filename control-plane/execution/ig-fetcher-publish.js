// control-plane/execution/ig-fetcher-publish.js
// IG Fetcher: publish domain — pure transport only.
//
// Owns: calling Instagram Graph API for outbound publishing actions.
// Does NOT own: DB writes, credential resolution, retry logic, orchestration.
//
// Pure transport. No DB writes. No persistence. No loops. No spawning.
// Called by orchestrator under HSM governance.

const publishTransport = require('../../substrates/transport/publishing');

/**
 * Executes a publishing action against the Instagram Graph API.
 * Routes to the correct handler based on actionType.
 *
 * @param {string} actionType - 'publish_post'|'repost_ugc'|'reply_comment'|'reply_dm'|'send_dm'
 * @param {string} accountId
 * @param {{ igUserId: string, pageToken: string, pageId?: string }} credentials
 * @param {object} payload - action-specific payload
 * @param {object} [supabase] - only needed for repost_ugc
 * @returns {Promise<{success: boolean, instagram_id?: string, error?: string, retryable?: boolean, error_category?: string}>}
 */
async function executePublishAction(actionType, accountId, credentials, payload, supabase) {
  return publishTransport.executeAction(actionType, accountId, credentials, payload, supabase);
}

module.exports = { executePublishAction };
