// substrates/engagement/index.js
// Engagement substrate: factory-creates workers → bounded IG API read.
//
// Owns: worker factory + transport bridge + broad scan orchestration.
// Does NOT own: retry, error classification, credential resolution.
//
// Workers: CommentsWorker, MessagesWorker, ConversationsWorker.
// Persist: routes to persistence substrate (called by parsing workers asynchronously).

const CommentsWorker = require('./workers/comments');
const MessagesWorker = require('./workers/messages');
const ConversationsWorker = require('./workers/conversations');
const transport = require('./transport');
const persistence = require('../persistence');
const { getRecentMedia } = require('../db/readers');

/**
 * Fetch raw data from Instagram API for engagement domain.
 * Factory-creates the appropriate worker based on params.
 *
 * @param {string} accountId
 * @param {object} params — { media_id?, conversation_id?, conversations?, limit?, maxPosts? }
 * @param {object} credentials — pre-resolved
 * @returns {Promise<object>} raw transport response
 */
async function fetch(accountId, params, credentials) {
  // Single media → comments
  if (params.media_id) {
    const worker = new CommentsWorker();
    return worker.execute(accountId, params, credentials);
  }
  // Single conversation → messages
  if (params.conversation_id) {
    const worker = new MessagesWorker();
    return worker.execute(accountId, params, credentials);
  }
  // Conversation list
  if (params.convLimit || params.conversations) {
    const worker = new ConversationsWorker();
    return worker.execute(accountId, params, credentials);
  }
  // Broad comment scan: recent media → concurrent per-post fetch
  const maxPosts = params.maxPosts || 5;
  const recentMedia = await getRecentMedia(accountId);
  const postsToCheck = recentMedia.slice(0, maxPosts);
  if (postsToCheck.length === 0) {
    return { success: true, batches: [], count: 0 };
  }
  const { runConcurrent } = require('../../services/sync/helpers');
  const limit = params.limit || 50;
  const results = await runConcurrent(
    postsToCheck,
    (media) => {
      const worker = new CommentsWorker();
      return worker.execute(accountId, { media_id: media.instagram_media_id, limit }, credentials);
    },
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
 * Persist raw engagement data to Supabase. Normalizes internally.
 * Called by parsing workers asynchronously.
 */
async function persist(accountId, rawData, extra = {}) {
  if (rawData.batches) {
    return persistence.storeCommentBatches(accountId, rawData.batches);
  }
  if (rawData.records) {
    return persistence.storeCommentBatches(accountId, [{ mediaId: 'direct', comments: rawData.records }]);
  }
  if (rawData.rawMessages) {
    return persistence.storeMessageBatches(
      accountId,
      [{ conversationId: rawData.conversationId || 'direct', rawMessages: rawData.rawMessages }],
      rawData.igUserId || extra.igUserId,
      rawData.pageId || extra.pageId || null,
      extra.credentials || null
    );
  }
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
