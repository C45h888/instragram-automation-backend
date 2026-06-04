// substrates/content/index.js
// Content substrate: factory-creates worker → bounded IG API read.
//
// Owns: worker factory + transport bridge. Pure delegation plane.
// Does NOT own: retry, error classification, orchestration, credential resolution.
//
// Worker: PostsWorker — one bounded fetchPosts() call.
// Persist: routes to persistence substrate (called by parsing workers asynchronously).

const PostsWorker = require('./workers/posts');
const persistence = require('../persistence');

/**
 * Fetch raw data from Instagram API for content domain.
 * Factory-creates a PostsWorker and delegates the bounded call.
 *
 * @param {string} accountId
 * @param {object} params — { limit?, since?, until?, maxPosts? }
 * @param {object} credentials — pre-resolved { igUserId, pageToken, userId }
 * @returns {Promise<object>} raw transport response
 */
async function fetch(accountId, params, credentials) {
  const worker = new PostsWorker();
  return worker.execute(accountId, params, credentials);
}

/**
 * Persist raw business post data to Supabase.
 * Called by parsing workers asynchronously — not by the retry worker.
 */
async function persist(accountId, rawData) {
  if (!rawData.posts || rawData.posts.length === 0) return { count: 0 };
  return persistence.storeBusinessPosts(accountId, rawData.posts);
}

module.exports = { fetch, persist };
