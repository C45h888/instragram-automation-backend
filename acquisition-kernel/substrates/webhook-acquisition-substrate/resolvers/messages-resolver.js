// substrates/webhook-acquisition-substrate/resolvers/messages-resolver.js
// Pure transform: canonical DM event → instagram_dm_messages row (+ optional
// instagram_dm_conversations row).
//
// Owns: shape mapping only. No I/O. No state. No governance calls.
// Does NOT own: conversation UUID resolution, account context, DB writes.
//
// Contract:
//   Input  — canonical event (eventType ∈ {dm_echo, dm_postback})
//            + resolved context { accountId, businessAccountId,
//                                  conversationId (internal UUID or null),
//                                  customerInstagramId, customerUsername }
//   Output — { table, operation, rows } | { writes: [...] } for DB_WRITE_REQUESTED
//
// The FSM supplies the resolved context. If conversationId is null, the
// resolver produces only a message row; conversation repair is a separate
// FSM action (not handled here).

const { EVENT_TYPES } = require('../normalizer');

const TABLE_MESSAGES = 'instagram_dm_messages';
const TABLE_CONVERSATIONS = 'instagram_dm_conversations';
const OPERATION_UPSERT_MESSAGES = 'batch_upsert_messages';
const OPERATION_UPSERT_CONVERSATIONS = 'batch_upsert_conversations';

function resolve(canonicalEvent, context) {
  if (!canonicalEvent) return null;
  const eventType = canonicalEvent.eventType;
  if (eventType !== EVENT_TYPES.DM_ECHO && eventType !== EVENT_TYPES.DM_POSTBACK) {
    return null;
  }
  const n = canonicalEvent.normalized || {};

  if (!n.messageId) return { error: 'missing_message_id' };
  if (!context || !context.accountId) return { error: 'missing_account_context' };
  if (!context.businessAccountId) return { error: 'missing_business_account_context' }

  // ── Message row (always produced) ────────────────────────────────────
  const messageRow = {
    instagram_message_id: n.messageId,
    business_account_id:  context.businessAccountId,
    sender_instagram_id:  n.senderId || null,
    recipient_instagram_id: n.recipientId || null,
    message_text:         n.text || null,
    is_from_business:     !!n.isSelf,
    is_echo:              !!n.isEcho,
    sent_at:              new Date(canonicalEvent.occurredAt || Date.now()).toISOString(),
    conversation_id:      context.conversationId || null, // internal UUID (nullable)
    instagram_thread_id:  n.recipientId || null,         // IG thread id (for repair)
    metadata: {
      source: canonicalEvent.source,
      priority: canonicalEvent.priority,
      raw_event_id: canonicalEvent.eventId,
      event_type: eventType,
      postback: n.postback || null,
    },
  };

  return {
    table: TABLE_MESSAGES,
    operation: OPERATION_UPSERT_MESSAGES,
    rows: [messageRow],
    // Side-effect: if the FSM supplied a conversationId and this is a new
    // thread, the FSM should dispatch an additional conversations write
    // separately. The resolver signals it but does not perform it.
    requiresConversationUpsert: !!context.conversationId && !!n.recipientId,
    conversationContext: context.conversationId ? {
      instagram_thread_id: n.recipientId,
      business_account_id: context.businessAccountId,
      customer_instagram_id: context.customerInstagramId || n.senderId || null,
      customer_username: context.customerUsername || null,
    } : null,
  };
}

module.exports = {
  resolve,
  TABLE_MESSAGES,
  TABLE_CONVERSATIONS,
  OPERATION_UPSERT_MESSAGES,
  OPERATION_UPSERT_CONVERSATIONS,
};
