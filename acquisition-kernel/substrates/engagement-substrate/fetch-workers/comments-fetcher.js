// substrates/engagement/workers/comments.js
// Engagement CommentsWorker: one bounded IG API read call per instantiation.
//
// Owns: ONE HTTP GET to fetch comments for a single media post.
// Does NOT own: retry, credentials, error classification, state.

const transport = require('../transport');

module.exports = class CommentsWorker {
  async execute(accountId, params, credentials) {
    const mediaId = params?.media_id;
    const limit = params?.limit || 50;
    return transport.fetchComments(accountId, mediaId, limit, credentials);
  }
};
