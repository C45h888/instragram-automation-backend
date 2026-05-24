// helpers/data-fetchers/messaging-fetchers.js
// Domain: messaging — comments, DM conversations, DM messages.
// Transport layer lives in substrates/transport/instagram.js.
// This file is a thin wrapper: transport → normalize → persistence call.
// No req/res dependencies — callable from routes and proactive-sync cron.
//
// All api_usage rows written with domain='messaging' for targeted debugging:
//   SELECT * FROM api_usage WHERE domain = 'messaging' AND success = false ORDER BY created_at DESC
//
// Thin fetch/write split:
//   fetchComments()         → delegates to transport (no DB write)
//   fetchConversations()   → delegates to transport (no DB write)
//   fetchMessages()        → delegates to transport (no DB write)
//   fetchAndStoreComments()      — shim: transport + storeCommentBatches
//   fetchAndStoreMessages()      — shim: transport + storeMessageBatches
//   fetchAndStoreConversations() — shim: transport + storeConversationBatches

const {
  getSupabaseAdmin,
  categorizeIgError,
  logWithDomain,
  transformMessage,
  storeCommentBatches,
  storeConversationBatches,
  storeMessageBatches,
} = require('./base');
const transport = require('../../substrates/transport/instagram');

// ============================================
// COMMENTS — THIN FETCH (delegates to transport)
// ============================================

/**
 * Fetches comments for a media post from the Instagram Graph API.
 * Delegates to transport.fetchComments — signature preserved for backward compat.
 * NO DB write — callers use storeCommentBatches for batch persistence.
 *
 * @param {string} businessAccountId - UUID
 * @param {string} mediaId - Instagram media ID (numeric string)
 * @param {number} [limit=50] - Max comments (capped at 50 per Meta docs)
 * @param {object} [credentials=null] - Pre-resolved credentials
 * @returns {Promise<{success: boolean, records: Array, count: number, paging: Object, error?: string}>}
 */
async function fetchComments(businessAccountId, mediaId, limit = 50, credentials = null) {
  const startTime = Date.now();
  const result = await transport.fetchComments(businessAccountId, mediaId, limit, credentials);

  if (!result.success) {
    const errorMessage = result.error || 'Unknown error';
    const { retryable, error_category, retry_after_seconds } = result;
    await logWithDomain('messaging', {
      endpoint: '/post-comments', method: 'GET',
      business_account_id: businessAccountId,
      success: false, error: errorMessage, latency: Date.now() - startTime,
      status_code: null,
      details: { action: 'proxy_failure', error_category, retryable, retry_after_seconds: retry_after_seconds || null },
    });
    return result;
  }

  await logWithDomain('messaging', {
    endpoint: '/post-comments', method: 'GET',
    business_account_id: businessAccountId,
    success: true, latency: Date.now() - startTime,
  });

  return result;
}

// ============================================
// COMMENTS — SHIM (route backward-compat)
// ============================================

/**
 * Fetches and persists comments for a single media post.
 * Shim wrapping fetchComments + storeCommentBatches.
 * Routes call this unchanged; domain loops call fetchComments directly for batch parallelism.
 *
 * @param {string} businessAccountId
 * @param {string} mediaId
 * @param {number} [limit=50]
 * @returns {Promise<{success: boolean, comments: Array, count: number, paging: Object, error?: string}>}
 */
async function fetchAndStoreComments(businessAccountId, mediaId, limit = 50) {
  const result = await fetchComments(businessAccountId, mediaId, limit);
  if (result.success && result.records.length > 0) {
    await storeCommentBatches(businessAccountId, [{ mediaId, comments: result.records }]);
  }
  // backward-compat: callers expect .comments, not .records
  return { ...result, comments: result.records };
}

// ============================================
// CONVERSATIONS — THIN FETCH (delegates to transport)
// ============================================

/**
 * Fetches DM conversations from the Instagram Graph API.
 * Delegates to transport.fetchConversations — signature preserved for backward compat.
 * NO DB write — callers use storeConversationBatches() for persistence.
 *
 * @param {string} businessAccountId - UUID
 * @param {number} [limit=20] - Max conversations (capped at 50)
 * @param {object} [credentials=null] - Pre-resolved credentials
 * @returns {Promise<{success: boolean, rawConversations: Array, igUserId: string, pageId: string|null, count: number, paging: Object, _usagePct: number|null, error?: string}>}
 */
async function fetchConversations(businessAccountId, limit = 20, credentials = null) {
  const startTime = Date.now();
  const result = await transport.fetchConversations(businessAccountId, limit, credentials);

  if (!result.success) {
    const errorMessage = result.error || 'Unknown error';
    const { retryable, error_category, retry_after_seconds } = result;
    await logWithDomain('messaging', {
      endpoint: '/conversations', method: 'GET',
      business_account_id: businessAccountId,
      success: false, error: errorMessage, latency: Date.now() - startTime,
      status_code: null,
      details: { action: 'proxy_failure', error_category, retryable, retry_after_seconds: retry_after_seconds || null },
    });
    return result;
  }

  await logWithDomain('messaging', {
    endpoint: '/conversations', method: 'GET',
    business_account_id: businessAccountId,
    success: true, latency: Date.now() - startTime,
  });

  return result;
}

// ============================================
// CONVERSATIONS — SHIM (route + sync backward-compat)
// ============================================

/**
 * Fetches and persists DM conversations with corrected 24h window calculation.
 * Shim wrapping fetchConversations + storeConversationBatches.
 * Both buses (sync/engagement.js and routes/agents/engagement.js) call this unchanged.
 *
 * @param {string} businessAccountId
 * @param {number} [limit=20]
 * @returns {Promise<{success: boolean, conversations: Array, count: number, paging: Object, error?: string}>}
 */
async function fetchAndStoreConversations(businessAccountId, limit = 20) {
  const result = await fetchConversations(businessAccountId, limit);
  if (result.success && result.rawConversations.length > 0) {
    try {
      const stored = await storeConversationBatches(
        businessAccountId, result.rawConversations, result.igUserId, result.pageId
      );
      return { ...result, conversations: stored.conversations };
    } catch (wtErr) {
      console.warn('[messaging] Conversation write-through error:', wtErr.message);
      throw wtErr;
    }
  }
  return { ...result, conversations: [] };
}

// ============================================
// CONVERSATION MESSAGES — THIN FETCH (delegates to transport)
// ============================================

/**
 * Fetches messages for a single DM conversation from the Instagram Graph API.
 * Delegates to transport.fetchMessages — signature preserved for backward compat.
 * NO DB write. Returns raw messages plus igUserId/pageId needed by storeMessageBatches.
 *
 * @param {string} businessAccountId - UUID
 * @param {string} conversationId - Instagram thread ID
 * @param {number} [limit=20] - Max messages (capped at 100)
 * @param {object} [credentials=null] - Pre-resolved credentials
 * @returns {Promise<{success: boolean, rawMessages: Array, igUserId: string, pageId: string|null, count: number, paging: Object, error?: string}>}
 */
async function fetchMessages(businessAccountId, conversationId, limit = 20, credentials = null) {
  const startTime = Date.now();
  const result = await transport.fetchMessages(businessAccountId, conversationId, limit, credentials);

  if (!result.success) {
    const errorMessage = result.error || 'Unknown error';
    const { retryable, error_category, retry_after_seconds } = result;
    await logWithDomain('messaging', {
      endpoint: '/conversation-messages', method: 'GET',
      business_account_id: businessAccountId,
      success: false, error: errorMessage, latency: Date.now() - startTime,
      status_code: null,
      details: { action: 'proxy_failure', error_category, retryable, retry_after_seconds: retry_after_seconds || null },
    });
    return result;
  }

  await logWithDomain('messaging', {
    endpoint: '/conversation-messages', method: 'GET',
    business_account_id: businessAccountId,
    success: true, latency: Date.now() - startTime,
  });

  return result;
}

// ============================================
// CONVERSATION MESSAGES — SHIM (route backward-compat)
// ============================================

/**
 * Fetches and persists messages for a single DM conversation.
 * Shim wrapping fetchMessages + storeMessageBatches + DB query-back.
 * Routes call this unchanged; domain loops call fetchMessages directly for batch parallelism.
 *
 * @param {string} businessAccountId
 * @param {string} conversationId - Instagram thread ID
 * @param {number} [limit=20]
 * @returns {Promise<{success: boolean, messages: Array, count: number, paging: Object, error?: string}>}
 */
async function fetchAndStoreMessages(businessAccountId, conversationId, limit = 20) {
  const result = await fetchMessages(businessAccountId, conversationId, limit);

  if (result.success && result.rawMessages.length > 0) {
    const supabase = getSupabaseAdmin();
    if (supabase) {
      // Pass credentials so storeMessageBatches can call ensureConversationRows if needed
      const credentials = result.pageToken
        ? { pageToken: result.pageToken, igUserId: result.igUserId, pageId: result.pageId }
        : null;
      await storeMessageBatches(
        businessAccountId,
        [{ conversationId, rawMessages: result.rawMessages }],
        result.igUserId,
        result.pageId,
        credentials
      );
    }
  }

  // Return raw messages shaped via transformMessage.
  // The DB query-back (for frontend-shaped rows) is route-layer concern — lives in engagement.js.
  const messages = result.rawMessages
    .filter(m => m.id)
    .map(m => transformMessage(m, null, businessAccountId, result.igUserId, result.pageId, null));
  return { ...result, messages, count: messages.length };
}

module.exports = {
  fetchComments,
  fetchConversations,
  fetchMessages,
  fetchAndStoreComments,
  fetchAndStoreConversations,
  fetchAndStoreMessages,
};
