// substrates/engagement/parser.js
// Engagement parser: validates and normalizes raw engagement API shapes
// into a uniform intermediate representation before normalization.
//
// Owns: extracting structured data from raw Instagram API responses.
// Does NOT own: API transport, DB writes, schema normalization, orchestration.
//
// Extracted from inline logic in persistence.js (storeConversationBatches L229-263)
// and previously non-existent for comments/messages.

/**
 * Parse raw comment records from IG API.
 *
 * @param {Array} records — raw records from fetchComments [{ id, text, username, timestamp, like_count }]
 * @returns {Array<{id: string, text: string, username: string, timestamp: string, like_count: number}>}
 */
function parseComments(records) {
  if (!Array.isArray(records)) return [];
  return records
    .filter(c => c && typeof c.id === 'string' && c.id.length > 0)
    .map(c => ({
      id: c.id,
      text: c.text || '',
      username: c.username || '',
      timestamp: c.timestamp || null,
      like_count: typeof c.like_count === 'number' ? c.like_count : 0,
    }));
}

/**
 * Parse raw conversation records from IG API.
 * Extracts customer participant, customer last message time, and shapes
 * the raw data into structured form for the normalizer and caller.
 *
 * @param {Array} rawConversations — [{ id, participants, updated_time, message_count, messages }]
 * @param {string} igUserId — business IG User ID
 * @param {string|null} pageId — Facebook Page ID
 * @returns {{ records: Array, shaped: Array }}
 */
function parseConversations(rawConversations, igUserId, pageId) {
  if (!Array.isArray(rawConversations)) return { records: [], shaped: [] };

  const records = [];
  const shaped = [];

  for (const conv of rawConversations) {
    const customerMsg = conv.messages?.data?.find(
      m => m.from?.id !== igUserId && m.from?.id !== pageId
    );
    const lastCustomerTime = customerMsg ? new Date(customerMsg.created_time) : null;

    const participants = conv.participants?.data || [];
    const customerParticipant = participants.find(
      p => p.id !== igUserId && p.id !== pageId
    ) || participants[0];

    if (!customerParticipant?.id) continue;

    records.push({
      id: conv.id,
      customer_instagram_id: customerParticipant.id,
      customer_username: customerParticipant.username || null,
      updated_time: conv.updated_time || null,
      last_customer_message_at: lastCustomerTime ? lastCustomerTime.toISOString() : null,
      message_count: conv.message_count || 0,
    });

    shaped.push({
      id: conv.id,
      participants,
      last_message_at: conv.updated_time,
      message_count: conv.message_count || 0,
      last_message: conv.messages?.data?.[0] || null,
      last_customer_message_at: lastCustomerTime ? lastCustomerTime.toISOString() : null,
    });
  }

  return { records, shaped };
}

/**
 * Parse raw message records from IG API.
 *
 * @param {Array} rawMessages — [{ id, message, from, to, created_time, attachments, story, shares, is_unsupported }]
 * @returns {Array}
 */
function parseMessages(rawMessages) {
  if (!Array.isArray(rawMessages)) return [];
  return rawMessages.filter(m => m && typeof m.id === 'string' && m.id.length > 0);
}

module.exports = { parseComments, parseConversations, parseMessages };
