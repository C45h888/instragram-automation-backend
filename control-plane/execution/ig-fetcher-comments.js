// control-plane/execution/ig-fetcher-comments.js
// IG Fetcher: comments domain — pure transport only.
//
// Owns: calling Instagram Graph API for comment data.
// Does NOT own: DB writes, credential resolution, retry logic, orchestration.
//
// Pure transport. No DB writes. No persistence. No loops. No spawning.
// Called by orchestrator under HSM governance.

const transport = require('../../substrates/transport/instagram');
const { runConcurrent } = require('../../services/sync/helpers');

const COMMENT_MAX_POSTS = 5;

/**
 * Fetches comments for a single media post.
 *
 * @param {string} accountId
 * @param {string} mediaId - Instagram media ID
 * @param {number} [limit=50]
 * @param {{ igUserId: string, pageToken: string }} credentials
 * @returns {Promise<{success: boolean, records: Array, count: number, paging?: object, error?: string}>}
 */
async function fetchComments(accountId, mediaId, limit, credentials) {
  return transport.fetchComments(accountId, mediaId, limit, credentials);
}

/**
 * Fetches comments for recent media posts (broad scan).
 * Used by periodic acquisition, not by narrow agent requests.
 *
 * @param {string} accountId
 * @param {number} [maxPosts=5]
 * @param {number} [limit=50]
 * @param {{ igUserId: string, pageToken: string }} credentials
 * @returns {Promise<{success: boolean, batches: Array, count: number, _usagePct?: number, error?: string}>}
 */
async function fetchRecentMediaComments(accountId, maxPosts, limit, credentials) {
  const persistence = require('../../substrates/persistence');
  const recentMedia = await persistence.getRecentMedia(accountId);
  const postsToCheck = recentMedia.slice(0, maxPosts || COMMENT_MAX_POSTS);

  if (postsToCheck.length === 0) {
    return { success: true, batches: [], count: 0 };
  }

  const results = await runConcurrent(
    postsToCheck,
    (media) => transport.fetchComments(accountId, media.instagram_media_id, limit || 50, credentials),
    3
  );

  let maxUsagePct = null;
  const batches = [];

  for (let i = 0; i < results.length; i++) {
    const r = results[i];
    if (r.success && r.records?.length > 0) {
      batches.push({ mediaId: postsToCheck[i].instagram_media_id, comments: r.records });
    }
    if (r._usagePct != null && (maxUsagePct === null || r._usagePct > maxUsagePct)) {
      maxUsagePct = r._usagePct;
    }
  }

  const totalComments = batches.reduce((sum, b) => sum + b.comments.length, 0);
  return { success: true, batches, count: totalComments, _usagePct: maxUsagePct };
}

module.exports = { fetchComments, fetchRecentMediaComments };
