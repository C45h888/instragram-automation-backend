// substrates/content/index.js
// Content substrate: factory-creates worker → bounded IG API read.
//
// Owns: worker factory + transport bridge. Pure delegation plane.
// Does NOT own: retry, error classification, orchestration, credential resolution.
//
// Worker: PostsWorker — one bounded fetchPosts() call.
// Persist: routes to persistence substrate (called by parsing workers asynchronously).

const PostsWorker = require('./workers/posts');
const dispatchWrite = require('../../../postgres-telemetry-kernel/writers').dispatchWrite;

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
 * Persist business post data to Supabase.
 * Routes through CK dispatch path: DB_WRITE_REQUESTED → persist-telemetry-fsm → db/writer.
 * Note: syncHashtagsFromCaptions side-effect deferred to Phase 4 enrichment membrane.
 */
async function persist(accountId, rawData) {
  if (!rawData.posts || rawData.posts.length === 0) return { count: 0 };
  const rows = rawData.posts
    .filter(p => p && p.id)
    .map(p => ({
      instagram_media_id: p.id,
      business_account_id: accountId,
      media_type: p.media_type || null,
      caption: p.caption || null,
      media_url: p.media_url || null,
      thumbnail_url: p.thumbnail_url || null,
      permalink: p.permalink || null,
      like_count: p.like_count || 0,
      comments_count: p.comments_count || 0,
      published_at: p.timestamp || null,
    }));
  if (rows.length === 0) return { count: 0 };
  dispatchWrite('batch_upsert_posts', {
    domain: 'media', accountId, intentId: null, table: 'instagram_media',
    rows,
  });
  return { count: rows.length };
}

module.exports = { fetch, persist };
