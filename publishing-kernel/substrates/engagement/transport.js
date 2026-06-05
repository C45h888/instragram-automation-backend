// publishing-kernel/substrates/engagement/transport.js
// Engagement publishing transport: pure Instagram Graph API write operations.
//
// Owns: reply_comment, reply_dm, send_dm HTTP calls.
// Does NOT own: DB writes, credential resolution, retry logic, rate-limiting.
//
// Migrated from substrates/transport/publishing.js — engagement-specific actions only.

const axios = require('axios');
const { GRAPH_API_BASE } = require('../../../config/instagram');

// ═══════════════════════════════════════════════════════════════════════════════
// REPLY COMMENT
// ═══════════════════════════════════════════════════════════════════════════════

async function replyComment(commentId, pageToken, replyText) {
  const res = await axios.post(`${GRAPH_API_BASE}/${commentId}/replies`, null, {
    params: { message: replyText.trim(), access_token: pageToken },
    timeout: 10000,
  });
  return { success: true, id: res.data.id };
}

// ═══════════════════════════════════════════════════════════════════════════════
// REPLY DM
// ═══════════════════════════════════════════════════════════════════════════════

async function replyDm(conversationId, pageToken, messageText) {
  const res = await axios.post(`${GRAPH_API_BASE}/${conversationId}/messages`, null, {
    params: { message: messageText.trim(), access_token: pageToken },
    timeout: 10000,
  });
  return { success: true, id: res.data.id };
}

// ═══════════════════════════════════════════════════════════════════════════════
// SEND DM (new thread)
// ═══════════════════════════════════════════════════════════════════════════════

async function sendDm(pageId, igUserId, pageToken, recipientId, messageText) {
  const node = pageId || igUserId;
  const res = await axios.post(`${GRAPH_API_BASE}/${node}/messages`, {
    recipient: { id: String(recipientId) },
    message: { text: messageText.trim() },
  }, {
    params: { access_token: pageToken },
    timeout: 10000,
  });
  return { success: true, messageId: res.data.message_id || res.data.id };
}

module.exports = { replyComment, replyDm, sendDm };
