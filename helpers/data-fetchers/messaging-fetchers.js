// backend.api/helpers/data-fetchers/messaging-fetchers.js
// Domain: messaging — comments, DM conversations, DM messages.
// Fetches from Instagram Graph API and upserts to Supabase.
// No req/res dependencies — callable from routes and proactive-sync cron.
//
// All api_usage rows written with domain='messaging' for targeted debugging:
//   SELECT * FROM api_usage WHERE domain = 'messaging' AND success = false ORDER BY created_at DESC
//
// Thin fetch/write split:
//   fetchComments()  — IG API call only, returns raw records
//   fetchMessages()  — IG API call only, returns raw records
//   fetchAndStoreComments()  — shim: fetchComments + storeCommentBatches (route compat)
//   fetchAndStoreMessages()  — shim: fetchMessages + storeMessageBatches + query-back (route compat)
//   fetchAndStoreConversations() — unchanged (called once per account, no inner loop)

const {
  axios,
  getSupabaseAdmin,
  resolveAccountCredentials,
  categorizeIgError,
  GRAPH_API_BASE,
  logWithDomain,
  transformMessage,
  storeCommentBatches,
  storeConversationBatches,
  storeMessageBatches,
  parseUsageHeader,
} = require('./base');

// ============================================
// COMMENTS — THIN FETCH
// ============================================

/**
 * Fetches comments for a media post from the Instagram Graph API.
 * NO DB write — callers use storeCommentBatches for batch persistence.
 *
 * API note: Meta caps comments at 50 per query. Fields verified against Meta docs —
 * replies_count is NOT a valid field (only a replies edge); removed.
 *
 * @param {string} businessAccountId - UUID
 * @param {string} mediaId - Instagram media ID (numeric string)
 * @param {number} [limit=50] - Max comments (capped at 50 per Meta docs)
 * @returns {Promise<{success: boolean, records: Array, count: number, paging: Object, error?: string}>}
 */
async function fetchComments(businessAccountId, mediaId, limit = 50, credentials = null) {
  const startTime = Date.now();
  const fetchLimit = Math.min(parseInt(limit) || 50, 50); // Meta docs: max 50 per query

  try {
    const { pageToken, userId } = credentials || await resolveAccountCredentials(businessAccountId);

    const commentsRes = await axios.get(`${GRAPH_API_BASE}/${mediaId}/comments`, {
      params: {
        fields: 'id,text,timestamp,username,like_count', // replies_count removed — not a valid field
        limit: fetchLimit,
        access_token: pageToken
      },
      timeout: 10000
    });

    const latency = Date.now() - startTime;

    await logWithDomain('messaging', {
      endpoint: '/post-comments',
      method: 'GET',
      business_account_id: businessAccountId,
      user_id: userId,
      success: true,
      latency
    });

    const records = commentsRes.data.data || [];
    const paging = commentsRes.data.paging || {};

    if (paging.next) {
      logWithDomain('messaging', {
        endpoint: '/post-comments/paging', method: 'SYSTEM', success: true,
        business_account_id: businessAccountId,
        details: { action: 'paging_next_detected', items_this_page: records.length, next_cursor_present: true },
      }).catch(() => {});
    }

    return {
      success: true, records, count: records.length, paging,
      _usagePct: parseUsageHeader(commentsRes.headers?.['x-business-use-case-usage']),
    };

  } catch (error) {
    const latency = Date.now() - startTime;
    const errorMessage = error.response?.data?.error?.message || error.message;
    const { retryable, error_category, retry_after_seconds } = categorizeIgError(error);

    await logWithDomain('messaging', {
      endpoint: '/post-comments',
      method: 'GET',
      business_account_id: businessAccountId,
      success: false,
      error: errorMessage,
      latency,
      status_code: error.response?.status || null,
      details: { action: 'proxy_failure', error_category, retryable, retry_after_seconds: retry_after_seconds || null, latency_ms: latency },
    });

    return {
      success: false, records: [], count: 0, paging: {}, error: errorMessage,
      code: error.response?.data?.error?.code,
      retryable, error_category, retry_after_seconds
    };
  }
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
// CONVERSATIONS — THIN FETCH
// ============================================

/**
 * Fetches DM conversations from the Instagram Graph API.
 * NO DB write — callers use storeConversationBatches() for persistence.
 *
 * Uses messages.limit(2) so storeConversationBatches can identify the customer's
 * last message even when the business replied most recently (fixes window bug).
 *
 * @param {string} businessAccountId - UUID
 * @param {number} [limit=20] - Max conversations (capped at 50)
 * @param {object} [credentials=null] - Pre-resolved credentials (avoids extra DB hit)
 * @returns {Promise<{success: boolean, rawConversations: Array, igUserId: string, pageId: string|null, count: number, paging: Object, _usagePct: number|null, error?: string}>}
 */
async function fetchConversations(businessAccountId, limit = 20, credentials = null) {
  const startTime = Date.now();
  const fetchLimit = Math.min(parseInt(limit) || 20, 50);

  try {
    const { igUserId, pageToken, userId, pageId } = credentials || await resolveAccountCredentials(businessAccountId);

    // Meta: GET /{page-id}/conversations?platform=INSTAGRAM
    // pageId = Facebook Page ID, NOT igUserId — required for Instagram DM conversations on business accounts
    const conversationNode = pageId || igUserId;
    const convRes = await axios.get(`${GRAPH_API_BASE}/${conversationNode}/conversations`, {
      params: {
        fields: 'id,participants{id,username},updated_time,message_count,messages.limit(2){created_time,from{id}}',
        platform: 'INSTAGRAM',
        limit: fetchLimit,
        access_token: pageToken,
      },
      timeout: 10000,
    });

    const latency = Date.now() - startTime;

    await logWithDomain('messaging', {
      endpoint: '/conversations',
      method: 'GET',
      business_account_id: businessAccountId,
      user_id: userId,
      success: true,
      latency,
    });

    const rawConversations = convRes.data.data || [];
    const paging = convRes.data.paging || {};

    if (paging.next) {
      logWithDomain('messaging', {
        endpoint: '/conversations/paging', method: 'SYSTEM', success: true,
        business_account_id: businessAccountId,
        details: { action: 'paging_next_detected', items_this_page: rawConversations.length, next_cursor_present: true },
      }).catch(() => {});
    }

    return {
      success: true,
      rawConversations,
      igUserId,
      pageId,
      count: rawConversations.length,
      paging,
      _usagePct: parseUsageHeader(convRes.headers?.['x-business-use-case-usage']),
    };

  } catch (error) {
    const latency = Date.now() - startTime;
    const errorMessage = error.response?.data?.error?.message || error.message;
    const { retryable, error_category, retry_after_seconds } = categorizeIgError(error);

    await logWithDomain('messaging', {
      endpoint: '/conversations',
      method: 'GET',
      business_account_id: businessAccountId,
      success: false,
      error: errorMessage,
      latency,
      status_code: error.response?.status || null,
      details: { action: 'proxy_failure', error_category, retryable, retry_after_seconds: retry_after_seconds || null, latency_ms: latency },
    });

    return {
      success: false, rawConversations: [], igUserId: null, pageId: null,
      count: 0, paging: {}, error: errorMessage,
      code: error.response?.data?.error?.code,
      retryable, error_category, retry_after_seconds,
    };
  }
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
// CONVERSATION MESSAGES — THIN FETCH
// ============================================

/**
 * Fetches messages for a single DM conversation from the Instagram Graph API.
 * NO DB write. Returns raw messages plus igUserId/pageId needed by storeMessageBatches.
 *
 * @param {string} businessAccountId - UUID
 * @param {string} conversationId - Instagram thread ID
 * @param {number} [limit=20] - Max messages (capped at 100)
 * @returns {Promise<{success: boolean, rawMessages: Array, igUserId: string, pageId: string|null, count: number, paging: Object, error?: string}>}
 */
async function fetchMessages(businessAccountId, conversationId, limit = 20, credentials = null) {
  const startTime = Date.now();
  const fetchLimit = Math.min(parseInt(limit) || 20, 100);

  try {
    const { igUserId, pageToken, userId, pageId } = credentials || await resolveAccountCredentials(businessAccountId);

    const msgRes = await axios.get(`${GRAPH_API_BASE}/${conversationId}/messages`, {
      params: {
        fields: 'id,message,from{id,username},to{id,username},created_time,' +
                'attachments{id,image_data{url,preview_url,render_as_sticker,animated_gif_url},file_url,name},' +
                'story,shares,is_unsupported',
        limit: fetchLimit,
        access_token: pageToken
      },
      timeout: 10000
    });

    const latency = Date.now() - startTime;

    await logWithDomain('messaging', {
      endpoint: '/conversation-messages',
      method: 'GET',
      business_account_id: businessAccountId,
      user_id: userId,
      success: true,
      latency
    });

    const rawMessages = msgRes.data.data || [];

    return {
      success: true,
      rawMessages,
      igUserId,
      pageId,
      pageToken,  // passed through so shim can forward to storeMessageBatches credentials
      count: rawMessages.length,
      paging: msgRes.data.paging || {},
      _usagePct: parseUsageHeader(msgRes.headers?.['x-business-use-case-usage']),
    };

  } catch (error) {
    const latency = Date.now() - startTime;
    const errorMessage = error.response?.data?.error?.message || error.message;
    const { retryable, error_category, retry_after_seconds } = categorizeIgError(error);

    await logWithDomain('messaging', {
      endpoint: '/conversation-messages',
      method: 'GET',
      business_account_id: businessAccountId,
      success: false,
      error: errorMessage,
      latency,
      status_code: error.response?.status || null,
      details: { action: 'proxy_failure', error_category, retryable, retry_after_seconds: retry_after_seconds || null, latency_ms: latency },
    });

    return {
      success: false, rawMessages: [], igUserId: null, pageId: null,
      count: 0, paging: {}, error: errorMessage,
      code: error.response?.data?.error?.code,
      retryable, error_category, retry_after_seconds
    };
  }
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
