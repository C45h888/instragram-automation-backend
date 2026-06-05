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
const { getRecentMedia } = require('../../../substrates/db/readers');
const { normalizeComment, transformMessage } = require('./normalizer');
const dispatchWrite = require('../../../substrates/db/writers').dispatchWrite;

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
  const { runConcurrent } = require('../../../services/sync/helpers');
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
 * Persist engagement data to Supabase.
 * Routes through CK dispatch path: DB_WRITE_REQUESTED → persist-telemetry-fsm → db/writer.
 *
 * Normalization is done inline — callers receive raw API shapes and this function
 * canonicalizes them before dispatching.
 */
async function persist(accountId, rawData, extra = {}) {
  if (rawData.batches) {
    // Comments from media batches: { batches: [{ mediaId, comments }] }
    const allRecords = [];
    for (const { mediaId, comments } of rawData.batches) {
      for (const c of comments) {
        if (!c.id) continue;
        allRecords.push(normalizeComment(c, mediaId, accountId));
      }
    }
    if (allRecords.length === 0) return { count: 0 };
    dispatchWrite('batch_upsert_comments', {
      domain: 'comments', accountId, intentId: null, table: 'instagram_comments',
      rows: allRecords,
    });
    return { count: allRecords.length };
  }

  if (rawData.records) {
    // Direct comments (no media context): { records: [...] }
    const allRecords = [];
    for (const c of rawData.records) {
      if (!c.id) continue;
      allRecords.push(normalizeComment(c, 'direct', accountId));
    }
    if (allRecords.length === 0) return { count: 0 };
    dispatchWrite('batch_upsert_comments', {
      domain: 'comments', accountId, intentId: null, table: 'instagram_comments',
      rows: allRecords,
    });
    return { count: allRecords.length };
  }

  if (rawData.rawMessages) {
    // Messages: { rawMessages: [...], conversationId, igUserId?, pageId? }
    const igUserId = rawData.igUserId || extra.igUserId;
    const pageId = rawData.pageId || extra.pageId || null;
    const rows = [];
    for (const m of rawData.rawMessages) {
      if (!m || !m.id) continue;
      const fromBusiness = m.from?.id === igUserId || (pageId && m.from?.id === pageId);
      const att = m.attachments?.data?.[0] || null;
      const imgData = att?.image_data || null;
      const isSticker = imgData?.render_as_sticker === true;
      const mediaUrl = imgData?.url || imgData?.animated_gif_url || att?.file_url || m.story?.link || null;
      let messageType = 'text';
      if (isSticker) messageType = 'media';
      else if (att) messageType = 'media';
      else if (m.story) messageType = 'story_reply';
      else if (m.shares?.data?.length) messageType = 'post_share';
      const mediaType = imgData ? 'image' : att?.file_url ? 'file' : null;
      rows.push({
        instagram_message_id: m.id,
        message_text: m.message || null,
        message_type: messageType,
        media_url: mediaUrl,
        media_type: mediaType,
        conversation_id: rawData.conversationId || 'direct',
        business_account_id: accountId,
        is_from_business: fromBusiness,
        recipient_instagram_id: m.to?.data?.[0]?.id || (fromBusiness ? null : igUserId) || '',
        sender_username: m.from?.username || null,
        sent_at: m.created_time,
        send_status: fromBusiness ? 'sent' : 'delivered',
      });
    }
    if (rows.length === 0) return { count: 0 };
    dispatchWrite('batch_upsert_messages', {
      domain: 'messages', accountId, intentId: null, table: 'instagram_dm_messages',
      rows,
    });
    return { count: rows.length };
  }

  if (rawData.rawConversations) {
    // Conversations: { rawConversations: [...], igUserId?, pageId? }
    const igUserId = rawData.igUserId || extra.igUserId;
    const pageId = rawData.pageId || extra.pageId || null;
    const { parseConversations } = require('./parser');
    const { records } = parseConversations(rawData.rawConversations, igUserId, pageId);
    if (records.length === 0) return { count: 0 };
    const rows = records.map(r => ({
      instagram_thread_id: r.id,
      customer_instagram_id: r.customer_instagram_id,
      customer_username: r.customer_username,
      business_account_id: accountId,
      last_message_at: r.updated_time,
      last_user_message_at: r.last_customer_message_at,
      message_count: r.message_count,
      conversation_status: 'active',
    }));
    dispatchWrite('batch_upsert_conversations', {
      domain: 'messages', accountId, intentId: null, table: 'instagram_dm_conversations',
      rows,
    });
    return { count: rows.length };
  }

  return { count: 0 };
}

module.exports = { fetch, persist };
