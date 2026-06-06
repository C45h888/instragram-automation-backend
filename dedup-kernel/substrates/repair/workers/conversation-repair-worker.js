// dedup-kernel/substrates/repair/workers/conversation-repair-worker.js
// Conversation Repair Worker: executor plane — one bounded repair attempt.
//
// Owns: ONE Graph API call to fetch missing conversation + upsert via
//        dispatchWrite + governedRead for UUID + fix orphaned messages.
// Does NOT own: governance policy, subscription management, dedup.
//
// Operationally bounded to: one external I/O call (Graph API).
// DB operations route through dispatchWrite → CK → persist-telemetry-fsm.

const { GRAPH_API_BASE } = require('../../../../substrates/transport/_shared');
const { parseConversations } = require('../../../../acquisition-kernel/substrates/engagement-substrate/parser');
const { dispatchWrite } = require('../../../../postgres-telemetry-kernel/writers');
const axios = require('axios');

module.exports = class ConversationRepairWorker {
  /**
   * Execute one bounded repair attempt.
   *
   * @param {{ threadId: string, accountId: string, igUserId: string, pageToken: string, pageId: string|null }} input
   * @param {object} governance — CK module (governedRead)
   * @returns {Promise<{ recovered: number, uuid: string|null }>}
   */
  async execute(input, governance) {
    const { threadId, accountId, igUserId, pageToken, pageId } = input;

    // ═══════════════════════════════════════════════════════════════════════
    // 1. Graph API: fetch conversation data ────────────────────────────────
    // ═══════════════════════════════════════════════════════════════════════
    let apiConversation = null;
    try {
      const conversationNode = pageId || igUserId;
      const res = await axios.get(`${GRAPH_API_BASE}/${conversationNode}/conversations`, {
        params: {
          fields: 'id,participants{id,username},updated_time,message_count,messages.limit(2){created_time,from{id}}',
          platform: 'INSTAGRAM',
          limit: 50,
          access_token: pageToken,
        },
        timeout: 10000,
      });

      const conversations = res.data?.data || [];
      apiConversation = conversations.find(c => c.id === threadId);
    } catch (err) {
      console.warn(`[conversation-repair-worker] Graph API call failed for ${threadId}:`, err.message);
      return { recovered: 0, uuid: null };
    }

    if (!apiConversation) {
      console.warn(`[conversation-repair-worker] Conversation ${threadId} not found via API`);
      return { recovered: 0, uuid: null };
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 2. Parse → build conversation row ────────────────────────────────────
    // ═══════════════════════════════════════════════════════════════════════
    const { records } = parseConversations([apiConversation], igUserId, pageId);
    if (!records.length) return { recovered: 0, uuid: null };

    // Resolve customer_user_id if governance is available
    if (governance) {
      const { hydrate } = require('../../../../acquisition-kernel/substrates/engagement-substrate/hydrators/conversation-hydrator');
      await hydrate(records, governance);
    }

    const convRow = {
      instagram_thread_id: records[0].id,
      customer_instagram_id: records[0].customer_instagram_id,
      customer_username: records[0].customer_username,
      business_account_id: accountId,
      customer_user_id: records[0].customer_user_id || null,
      last_message_at: records[0].updated_time,
      last_user_message_at: records[0].last_customer_message_at,
      message_count: records[0].message_count,
      conversation_status: 'active',
    };

    // ═══════════════════════════════════════════════════════════════════════
    // 3. Upsert conversation via canonical writer ──────────────────────────
    // ═══════════════════════════════════════════════════════════════════════
    dispatchWrite('batch_upsert_conversations', {
      domain: 'messages',
      accountId,
      intentId: null,
      table: 'instagram_dm_conversations',
      rows: [convRow],
    });

    // ═══════════════════════════════════════════════════════════════════════
    // 4. Resolve UUID via governed read ────────────────────────────────────
    // ═══════════════════════════════════════════════════════════════════════
    let uuid = null;
    if (governance) {
      const result = await governance.governedRead('db.accounts', {
        query: 'igThreadIdToUuid',
        threadIds: [threadId],
      });
      if (result.success && result.data?.length > 0) {
        uuid = result.data[0].id;
      }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 5. Fix orphaned messages — update conversation_id from threadId → UUID
    // ═══════════════════════════════════════════════════════════════════════
    if (uuid) {
      dispatchWrite('batch_fix_message_conversation_ids', {
        domain: 'messages',
        accountId,
        intentId: null,
        table: 'instagram_dm_messages',
        rows: [{
          conversation_id: uuid,
          match_conversation_id: threadId,
          business_account_id: accountId,
        }],
      });
    }

    return { recovered: 1, uuid };
  }
};
