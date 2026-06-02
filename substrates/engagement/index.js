// substrates/engagement/index.js
// Engagement substrate: full pipeline for comments, conversations, messages.
//
// Owns: fetch → parse → normalize → persist for engagement domain.
// Does NOT own: retry logic, error classification, orchestration, credential resolution.

const transport = require('./transport');
const parser = require('./parser');
const { normalizeComment, transformMessage } = require('./normalizer');
const persistence = require('../persistence');
const { getRecentMedia } = require('../db/readers');

/**
 * Fetch raw data from Instagram API for an engagement domain.
 * Pure transport — no parsing, no persistence.
 *
 * @param {string} accountId
 * @param {object} params — { media_id?, conversation_id?, limit, maxPosts? }
 * @param {object} credentials — { igUserId, pageToken, userId, pageId }
 * @returns {Promise<object>} raw transport response
 */
async function fetch(accountId, params, credentials) {
  if (params.media_id) {
    return transport.fetchComments(accountId, params.media_id, params.limit, credentials);
  }
  if (params.conversation_id) {
    return transport.fetchMessages(accountId, params.conversation_id, params.limit, credentials);
  }
  // Conversations: fetch list
  if (params.convLimit || params.conversations) {
    return transport.fetchConversations(accountId, params.convLimit || params.limit, credentials);
  }
  // Comments broad scan: fetch recent media comments
  const maxPosts = params.maxPosts || 5;
  const recentMedia = await getRecentMedia(accountId);
  const postsToCheck = recentMedia.slice(0, maxPosts);
  if (postsToCheck.length === 0) {
    return { success: true, batches: [], count: 0 };
  }
  const { runConcurrent } = require('../../services/sync/helpers');
  const results = await runConcurrent(
    postsToCheck,
    (media) => transport.fetchComments(accountId, media.instagram_media_id, params.limit || 50, credentials),
    3
  );
  let maxUsagePct = null;
  const batches = [];
  for (let i = 0; i < results.length; i++) {
    const r = results[i];
    if (r.success && r.records?.length > 0) {
      batches.push({ mediaId: postsToCheck[i].instagram_media_id, comments: r.records });
    }
    if (r._usagePct != null && (maxUsagePct === null || r._usagePct > maxUsagePct)) {
      maxUsagePct = r._usagePct;
    }
  }
  const totalComments = batches.reduce((sum, b) => sum + b.comments.length, 0);
  return { success: true, batches, count: totalComments, _usagePct: maxUsagePct };
}

/**
 * Persist raw engagement data to Supabase.
 * Handles normalization internally.
 *
 * @param {string} accountId
 * @param {object} rawData — raw transport response
 * @param {object} [extra] — { igUserId, pageId } for messages
 * @returns {Promise<{count: number}>}
 */
async function persist(accountId, rawData, extra = {}) {
  // Comments
  if (rawData.batches) {
    return persistence.storeCommentBatches(accountId, rawData.batches);
  }
  if (rawData.records) {
    return persistence.storeCommentBatches(accountId, [{ mediaId: 'direct', comments: rawData.records }]);
  }
  // Messages
  if (rawData.rawMessages) {
    return persistence.storeMessageBatches(
      accountId,
      [{ conversationId: rawData.conversationId || 'direct', rawMessages: rawData.rawMessages }],
      rawData.igUserId || extra.igUserId,
      rawData.pageId || extra.pageId || null,
      extra.credentials || null
    );
  }
  // Conversations
  if (rawData.rawConversations) {
    return persistence.storeConversationBatches(
      accountId, rawData.rawConversations,
      rawData.igUserId || extra.igUserId,
      rawData.pageId || extra.pageId || null
    );
  }
  return { count: 0 };
}

module.exports = { fetch, persist };
