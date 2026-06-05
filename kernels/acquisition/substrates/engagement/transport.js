// substrates/engagement/transport.js
// Engagement substrate transport: comments, conversations, messages.
//
// Owns: calling Instagram Graph API for direct user interaction endpoints.
// Does NOT own: DB writes, normalization, retry logic, orchestration.
//
// Decomposed from substrates/transport/instagram.js (former god module).

const {
  axios,
  GRAPH_API_BASE,
  resolveCreds,
  clampLimit,
  buildErrorResponse,
  extractUsage,
  logTelemetry,
} = require('../../../../substrates/transport/_shared');

// ═══════════════════════════════════════════════════════════════════════════════
// COMMENTS
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Fetch comments for a single media post. Meta caps at 50 per query.
 *
 * @param {string} accountId
 * @param {string} mediaId - Instagram media ID (numeric string)
 * @param {number} [limit=50]
 * @param {object} [credentials=null] - pre-resolved { pageToken, igUserId, userId }
 * @returns {Promise<object>} { success, records, count, paging, _usagePct, ...errorMeta }
 */
async function fetchComments(accountId, mediaId, limit = 50, credentials = null) {
  const startTime = Date.now();
  const fetchLimit = clampLimit(limit, 50, 50);

  try {
    const { pageToken, userId } = await resolveCreds(accountId, credentials);

    const res = await axios.get(`${GRAPH_API_BASE}/${mediaId}/comments`, {
      params: {
        fields: 'id,text,timestamp,username,like_count',
        limit: fetchLimit,
        access_token: pageToken,
      },
      timeout: 10000,
    });

    await logTelemetry('engagement', '/comments', accountId, userId, true, Date.now() - startTime);

    const records = res.data.data || [];
    return {
      success: true, records, count: records.length, paging: res.data.paging || {},
      _usagePct: extractUsage(res.headers),
    };
  } catch (error) {
    await logTelemetry('engagement', '/comments', accountId, null, false, Date.now() - startTime, {
      status_code: error.response?.status || null,
      error: error.response?.data?.error?.message || error.message,
      meta: buildErrorResponse(error),
    });
    return { success: false, records: [], count: 0, paging: {}, ...buildErrorResponse(error) };
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// CONVERSATIONS
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Fetch DM conversations. Uses messages.limit(2) so the caller can identify
 * the customer's last message even when the business replied most recently.
 *
 * @param {string} accountId
 * @param {number} [limit=20]
 * @param {object} [credentials=null] - pre-resolved { igUserId, pageToken, userId, pageId }
 * @returns {Promise<object>} { success, rawConversations, igUserId, pageId, count, _usagePct, ...errorMeta }
 */
async function fetchConversations(accountId, limit = 20, credentials = null) {
  const startTime = Date.now();
  const fetchLimit = clampLimit(limit, 20, 50);

  try {
    const { igUserId, pageToken, userId, pageId } = await resolveCreds(accountId, credentials);

    const conversationNode = pageId || igUserId;
    const res = await axios.get(`${GRAPH_API_BASE}/${conversationNode}/conversations`, {
      params: {
        fields: 'id,participants{id,username},updated_time,message_count,messages.limit(2){created_time,from{id}}',
        platform: 'INSTAGRAM',
        limit: fetchLimit,
        access_token: pageToken,
      },
      timeout: 10000,
    });

    await logTelemetry('engagement', '/conversations', accountId, userId, true, Date.now() - startTime);

    return {
      success: true,
      rawConversations: res.data.data || [],
      igUserId, pageId,
      count: (res.data.data || []).length,
      _usagePct: extractUsage(res.headers),
    };
  } catch (error) {
    await logTelemetry('engagement', '/conversations', accountId, null, false, Date.now() - startTime, {
      status_code: error.response?.status || null,
      error: error.response?.data?.error?.message || error.message,
      meta: buildErrorResponse(error),
    });
    return {
      success: false, rawConversations: [], igUserId: null, pageId: null,
      count: 0, ...buildErrorResponse(error),
    };
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// MESSAGES
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Fetch messages for a single DM conversation.
 *
 * @param {string} accountId
 * @param {string} conversationId - Instagram thread ID
 * @param {number} [limit=20]
 * @param {object} [credentials=null] - pre-resolved { igUserId, pageToken, userId, pageId }
 * @returns {Promise<object>} { success, rawMessages, igUserId, pageId, pageToken, count, _usagePct, ...errorMeta }
 */
async function fetchMessages(accountId, conversationId, limit = 20, credentials = null) {
  const startTime = Date.now();
  const fetchLimit = clampLimit(limit, 20, 100);

  try {
    const { igUserId, pageToken, userId, pageId } = await resolveCreds(accountId, credentials);

    const res = await axios.get(`${GRAPH_API_BASE}/${conversationId}/messages`, {
      params: {
        fields: 'id,message,from{id,username},to{id,username},created_time,' +
                'attachments{id,image_data{url,preview_url,render_as_sticker,animated_gif_url},file_url,name},' +
                'story,shares,is_unsupported',
        limit: fetchLimit,
        access_token: pageToken,
      },
      timeout: 10000,
    });

    await logTelemetry('engagement', '/messages', accountId, userId, true, Date.now() - startTime);

    return {
      success: true,
      rawMessages: res.data.data || [],
      igUserId, pageId, pageToken,
      count: (res.data.data || []).length,
      _usagePct: extractUsage(res.headers),
    };
  } catch (error) {
    await logTelemetry('engagement', '/messages', accountId, null, false, Date.now() - startTime, {
      status_code: error.response?.status || null,
      error: error.response?.data?.error?.message || error.message,
      meta: buildErrorResponse(error),
    });
    return {
      success: false, rawMessages: [], igUserId: null, pageId: null,
      count: 0, ...buildErrorResponse(error),
    };
  }
}

module.exports = { fetchComments, fetchConversations, fetchMessages };
