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
const { getRecentMedia } = require('../../../postgres-telemetry-kernel/readers');
const { normalizeComment, transformMessage } = require('./normalizer');
const conversationHydrator = require('./hydrators/conversation-hydrator');
const mediaHydrator = require('./hydrators/media-hydrator');

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
 * Constitutional path: hydrate → normalize → CK(DB_WRITE_REQUESTED) → writer.
 *
 * Normalization is done via canonical normalizer — no inline field mapping.
 */
async function persist(accountId, rawData, extra = {}) {
  const governance = extra._governance;

  if (rawData.batches) {
    // ── Comments from media batches ────────────────────────────────────────
    // Hydrate: resolve Instagram media IDs → DB UUIDs
    const mediaIds = [...new Set(rawData.batches.map(b => b.mediaId).filter(Boolean))];
    let mediaUUIDMap = new Map();

    if (governance && mediaIds.length > 0) {
      const { resolved, missing } = await mediaHydrator.hydrate(accountId, mediaIds, governance);
      mediaUUIDMap = resolved;

      // Create stubs for missing media IDs via constitutional dispatch
      if (missing.size > 0) {
        const stubs = [...missing].map(igId => ({
          instagram_media_id: igId,
          business_account_id: accountId,
        }));
        governance.dispatch({
          type: 'DB_WRITE_REQUESTED',
          domain: 'media', accountId, intentId: null,
          table: 'instagram_media',
          operation: 'batch_upsert_media_stubs',
          rows: stubs,
        });
        // Assume stubs created; pass through Instagram ID as fallback
        for (const igId of missing) {
          mediaUUIDMap.set(igId, igId);
        }
      }
    }

    // Normalize via canonical normalizer
    const allRecords = [];
    for (const { mediaId, comments } of rawData.batches) {
      const uuid = mediaUUIDMap.get(mediaId) || mediaId;
      for (const c of comments) {
        if (!c.id) continue;
        allRecords.push(normalizeComment(c, uuid, accountId));
      }
    }
    if (allRecords.length === 0) return { count: 0 };

    governance?.dispatch({
      type: 'DB_WRITE_REQUESTED',
      domain: 'comments', accountId, intentId: null,
      table: 'instagram_comments',
      operation: 'batch_upsert_comments',
      rows: allRecords,
    });
    return { count: allRecords.length };
  }

  if (rawData.records) {
    // ── Direct comments (no media context) ─────────────────────────────────
    const allRecords = [];
    for (const c of rawData.records) {
      if (!c.id) continue;
      allRecords.push(normalizeComment(c, 'direct', accountId));
    }
    if (allRecords.length === 0) return { count: 0 };

    governance?.dispatch({
      type: 'DB_WRITE_REQUESTED',
      domain: 'comments', accountId, intentId: null,
      table: 'instagram_comments',
      operation: 'batch_upsert_comments',
      rows: allRecords,
    });
    return { count: allRecords.length };
  }

  if (rawData.rawMessages) {
    // ── Messages ───────────────────────────────────────────────────────────
    const igUserId = rawData.igUserId || extra.igUserId;
    const pageId = rawData.pageId || extra.pageId || null;

    // Hydrate: resolve conversation thread ID → DB UUID
    let conversationUUID = null;
    if (governance && rawData.conversationId) {
      const convResult = await governance.governedRead('db.accounts', {
        query: 'igThreadIdToUuid',
        threadIds: [rawData.conversationId],
      });
      if (convResult.success && Array.isArray(convResult.data) && convResult.data.length > 0) {
        conversationUUID = convResult.data[0].id;
      }
    }
    // Fallback: use thread ID directly if governedRead unavailable or conversation not found
    if (!conversationUUID) {
      conversationUUID = rawData.conversationId || 'direct';
    }

    const rows = [];
    for (const m of rawData.rawMessages) {
      if (!m || !m.id) continue;
      rows.push(transformMessage(m, conversationUUID, accountId, igUserId, pageId, null));
    }
    if (rows.length === 0) return { count: 0 };

    governance?.dispatch({
      type: 'DB_WRITE_REQUESTED',
      domain: 'messages', accountId, intentId: null,
      table: 'instagram_dm_messages',
      operation: 'batch_upsert_messages',
      rows,
    });
    return { count: rows.length };
  }

  if (rawData.rawConversations) {
    // ── Conversations ──────────────────────────────────────────────────────
    const igUserId = rawData.igUserId || extra.igUserId;
    const pageId = rawData.pageId || extra.pageId || null;
    const { parseConversations } = require('./parser');
    const { records } = parseConversations(rawData.rawConversations, igUserId, pageId);
    if (records.length === 0) return { count: 0 };

    // Hydrate: resolve customer_user_id via governedRead
    if (governance) {
      await conversationHydrator.hydrate(records, governance);
    }

    const rows = records.map(r => ({
      instagram_thread_id: r.id,
      customer_instagram_id: r.customer_instagram_id,
      customer_username: r.customer_username,
      business_account_id: accountId,
      customer_user_id: r.customer_user_id || null,
      last_message_at: r.updated_time,
      last_user_message_at: r.last_customer_message_at,
      message_count: r.message_count,
      conversation_status: 'active',
    }));

    governance?.dispatch({
      type: 'DB_WRITE_REQUESTED',
      domain: 'messages', accountId, intentId: null,
      table: 'instagram_dm_conversations',
      operation: 'batch_upsert_conversations',
      rows,
    });
    return { count: rows.length };
  }

  return { count: 0 };
}

module.exports = { fetch, persist };
