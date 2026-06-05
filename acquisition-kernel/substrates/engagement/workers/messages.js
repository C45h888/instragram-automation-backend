// substrates/engagement/workers/messages.js
// Engagement MessagesWorker: one bounded IG API read call per instantiation.
//
// Owns: ONE HTTP GET to fetch messages for a single conversation.
// Does NOT own: retry, credentials, error classification, state.

const transport = require('../transport');

module.exports = class MessagesWorker {
  async execute(accountId, params, credentials) {
    const conversationId = params?.conversation_id;
    const limit = params?.limit || 20;
    return transport.fetchMessages(accountId, conversationId, limit, credentials);
  }
};
