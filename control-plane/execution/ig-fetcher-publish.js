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
 * UGC content is pre-resolved by the control plane before this call.
 *
 * @param {string} actionType - 'publish_post'|'repost_ugc'|'reply_comment'|'reply_dm'|'send_dm'
 * @param {string} accountId
 * @param {{ igUserId: string, pageToken: string, pageId?: string }} credentials
 * @param {object} payload - action-specific payload (pre-resolved for repost_ugc)
 * @returns {Promise<{success: boolean, instagram_id?: string, error?: string, retryable?: boolean, error_category?: string}>}
 */
async function executePublishAction(actionType, accountId, credentials, payload) {
  return publishTransport.executeAction(actionType, accountId, credentials, payload);
}

module.exports = { executePublishAction };
