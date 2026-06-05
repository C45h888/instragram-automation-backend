// substrates/parsing/workers/messages-worker.js
// Messages parsing worker: parse → hydrate → normalize → CK(DB_WRITE_REQUESTED).
//
// Owns: sequencing the engagement pipeline for message + conversation data.
// Does NOT own: parsing logic (engagement parser), normalization (engagement
//               normalizer), hydration (conversation-hydrator), Supabase,
//               governance policy. Conversation UUID resolution deferred to Phase 5.
//
// Phase 4: canonical path — uses domain substrate tools, no inline normalization.
// Phase 5: conversation UUID resolution + repair dispatch for missing conversations.

const { parseMessages, parseConversations } = require('../../substrates/engagement/parser');
const { transformMessage } = require('../../substrates/engagement/normalizer');
const { hydrate: hydrateConversations } = require('../../substrates/engagement/hydrators/conversation-hydrator');

async function execute(rawData, accountId, intentId, extra = {}, governance) {
  const igUserId = rawData.igUserId || extra.igUserId;
  const pageId = rawData.pageId || extra.pageId || null;
  const pageToken = rawData.pageToken || extra.pageToken || null;

  // ═══════════════════════════════════════════════════════════════════════════
  // MESSAGES — single conversation
  // ═══════════════════════════════════════════════════════════════════════════
  if (rawData.rawMessages && rawData.rawMessages.length > 0) {
    const conversationId = rawData.conversationId;

    // ── Parse ──────────────────────────────────────────────────────────────
    const parsed = parseMessages(rawData.rawMessages);
    if (!parsed.length) return { count: 0 };

    // ── Resolve conversation UUID (Phase 5) ────────────────────────────────
    let conversationUUID = conversationId || 'direct'; // fallback: thread ID
    if (governance && conversationId && conversationId !== 'direct') {
      const result = await governance.governedRead('db.accounts', {
        query: 'igThreadIdToUuid',
        threadIds: [conversationId],
      });
      if (result.success && result.data?.length > 0) {
        conversationUUID = result.data[0].id; // resolved UUID
      } else {
        // Conversation missing — fire repair through CK, proceed with stub
        governance.dispatch({
          type: 'REPAIR_CONVERSATION',
          threadId: conversationId,
          accountId, igUserId, pageToken, pageId,
        });
      }
    }

    // ── Normalize → DB rows ────────────────────────────────────────────────
    const rows = parsed.map(m =>
      transformMessage(m, conversationUUID, accountId, igUserId, pageId, null)
    );

    if (!rows.length) return { count: 0 };

    // ── Constitutional dispatch ────────────────────────────────────────────
    if (governance) {
      governance.dispatch({
        type: 'DB_WRITE_REQUESTED',
        domain: 'messages',
        accountId, intentId,
        table: 'instagram_dm_messages',
        operation: 'batch_upsert_messages',
        rows,
        extra: { igUserId, pageId, pageToken, conversationId: conversationUUID },
      });
    }

    return { count: rows.length };
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // CONVERSATIONS — list
  // ═══════════════════════════════════════════════════════════════════════════
  if (rawData.rawConversations && rawData.rawConversations.length > 0) {
    const { records } = parseConversations(rawData.rawConversations, igUserId, pageId);
    if (!records.length) return { count: 0 };

    // ── Hydrate: resolve customer_user_id via governedRead ─────────────────
    if (governance) {
      await hydrateConversations(records, governance);
    }

    // ── Normalize → DB rows ────────────────────────────────────────────────
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

    if (!rows.length) return { count: 0 };

    // ── Constitutional dispatch ────────────────────────────────────────────
    if (governance) {
      governance.dispatch({
        type: 'DB_WRITE_REQUESTED',
        domain: 'messages',
        accountId, intentId,
        table: 'instagram_dm_conversations',
        operation: 'batch_upsert_conversations',
        rows,
        extra: { igUserId, pageId },
      });
    }

    return { count: rows.length };
  }

  return { count: 0 };
}

module.exports = { execute };
