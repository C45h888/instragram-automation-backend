// substrates/engagement/workers/conversations.js
// Engagement ConversationsWorker: one bounded IG API read call per instantiation.
//
// Owns: ONE HTTP GET to fetch DM conversation list.
// Does NOT own: retry, credentials, error classification, state.

const transport = require('../transport');

module.exports = class ConversationsWorker {
  async execute(accountId, params, credentials) {
    const limit = params?.convLimit || params?.limit || 20;
    return transport.fetchConversations(accountId, limit, credentials);
  }
};
