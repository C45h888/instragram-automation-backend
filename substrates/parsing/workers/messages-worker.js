// substrates/parsing/workers/messages-worker.js
// Messages parsing worker: parse → normalize → persist for messages domain.
//
// Owns: transforming raw message batches into Supabase rows.
// Does NOT own: fetch, transport, orchestration, governance.

const persistence = require('../../persistence');

/**
 * Execute the message parsing pipeline.
 *
 * @param {object} rawData — raw transport response { rawMessages, rawConversations, igUserId, pageId, conversationId }
 * @param {string} accountId
 * @param {object} extra — { igUserId, pageId, pageToken, credentials }
 * @returns {Promise<{count: number, error?: string}>}
 */
async function execute(rawData, accountId, extra = {}) {
  const igUserId = rawData.igUserId || extra.igUserId;
  const pageId = rawData.pageId || extra.pageId || null;

  // Messages for a single conversation
  if (rawData.rawMessages && rawData.rawMessages.length > 0) {
    const result = await persistence.storeMessageBatches(
      accountId,
      [{ conversationId: rawData.conversationId || 'direct', rawMessages: rawData.rawMessages }],
      igUserId, pageId,
      extra.credentials || null
    );
    return { count: result.count || 0 };
  }

  // Conversations list (no messages to store, just upsert conversations)
  if (rawData.rawConversations && rawData.rawConversations.length > 0) {
    const result = await persistence.storeConversationBatches(
      accountId, rawData.rawConversations, igUserId, pageId
    );
    return { count: result.count || 0 };
  }

  return { count: 0 };
}

module.exports = { execute };
