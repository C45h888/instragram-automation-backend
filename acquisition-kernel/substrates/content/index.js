// substrates/content/index.js
// Content substrate: factory-creates worker → bounded IG API read.
//
// Owns: worker factory + transport bridge. Pure delegation plane.
// Does NOT own: retry, error classification, orchestration, credential resolution.
//
// Worker: PostsWorker — one bounded fetchPosts() call.
// Persist: routes to persistence substrate (called by parsing workers asynchronously).

const PostsWorker = require('./workers/posts');
const { normalizeBusinessPost } = require('./normalizer');
const { syncHashtagsFromCaptions } = require('../../../helpers/agent-helpers');
const { getSupabaseAdmin } = require('../../../config/supabase');

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
 * Constitutional path: normalize → CK(DB_WRITE_REQUESTED) → writer → hashtag sync.
 * Uses canonical normalizer — no inline field mapping.
 */
async function persist(accountId, rawData, extra = {}) {
  const governance = extra._governance;

  if (!rawData.posts || rawData.posts.length === 0) return { count: 0 };

  const rows = rawData.posts
    .filter(p => p && p.id)
    .map(p => normalizeBusinessPost(p, accountId));

  if (rows.length === 0) return { count: 0 };

  governance?.dispatch({
    type: 'DB_WRITE_REQUESTED',
    domain: 'media', accountId, intentId: null,
    table: 'instagram_media',
    operation: 'batch_upsert_posts',
    rows,
  });

  // Side-effect: extract hashtags from captions into ugc_monitored_hashtags
  const captions = rawData.posts.map(p => p.caption).filter(Boolean);
  if (captions.length > 0) {
    const supabase = getSupabaseAdmin();
    if (supabase) syncHashtagsFromCaptions(supabase, accountId, captions).catch(() => {});
  }

  return { count: rows.length };
}

module.exports = { fetch, persist };
