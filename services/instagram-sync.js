/**
 * Instagram Data Sync Service
 * Thin orchestration wrappers — all IG API calls go through transport layer,
 * all DB writes go through persistence layer.
 *
 * Jurisdiction contract:
 *   Transport = pure HTTP (axios call to IG API, returns raw response)
 *   Persistence = explicit DB write (upsert to Supabase)
 *   Wrapper = call transport → normalize → call persistence → return
 */

const transport = require('../substrates/transport/instagram');
const persistence = require('../substrates/persistence');
const { mapRawPostToUgcContent } = require('../substrates/normalization');

const GRAPH_API_VERSION = 'v23.0';

/**
 * ==========================================
 * WRAPPER FUNCTIONS (SYNCHRONIZATION)
 * ==========================================
 */

/**
 * Sync tagged posts (UGC) from Instagram to ugc_content table.
 * Thin wrapper: transport → normalize → persistence.
 *
 * @param {string} businessAccountId - UUID of the business account
 * @param {string} igUserId - Instagram Business Account ID
 * @param {string} pageToken - Page Access Token
 * @returns {Promise<{success: boolean, synced_count: number, errors_count: number, total_fetched: number}>}
 */
async function syncTaggedPosts(businessAccountId, igUserId, pageToken) {
  console.log('[Sync] Starting UGC sync for business account:', businessAccountId);

  // TRANSPORT: raw fetch from IG API
  const result = await transport.fetchTaggedMedia(businessAccountId, 50, { igUserId, pageToken });
  if (!result.success) {
    throw new Error(result.error || 'Failed to fetch tagged media from Instagram API');
  }

  const rawPosts = result.records || [];
  console.log(`[Sync] Fetched ${rawPosts.length} tagged posts from Instagram API`);

  if (rawPosts.length === 0) {
    return { success: true, synced_count: 0, errors_count: 0, total_fetched: 0 };
  }

  // TRANSPORT: resolve credentials for permission lookup
  const { resolveAccountCredentials } = require('../substrates/persistence');
  const credentials = await resolveAccountCredentials(businessAccountId);

  // PERSISTENCE: load existing permission flags for all posts in one round-trip
  const { getSupabaseAdmin } = require('../config/supabase');
  const supabase = getSupabaseAdmin();
  const postIds = rawPosts.map(p => p.id);

  const { data: existingRows } = await supabase
    .from('ugc_content')
    .select('visitor_post_id, repost_permission_granted, repost_permission_requested')
    .eq('business_account_id', businessAccountId)
    .in('visitor_post_id', postIds);

  const permissionMap = {};
  for (const row of existingRows || []) {
    permissionMap[row.visitor_post_id] = row;
  }

  // NORMALIZE: map raw posts to ugc_content shape, preserving permissions
  const ugcRecords = rawPosts.map(post => {
    const existing = permissionMap[post.id] || {};
    return {
      ...mapRawPostToUgcContent(post, businessAccountId, 'tagged', null),
      repost_permission_granted: existing.repost_permission_granted ?? null,
      repost_permission_requested: existing.repost_permission_requested ?? false,
    };
  });

  // PERSISTENCE: batch upsert to ugc_content
  const storeResult = await persistence.storeUgcContentBatch(ugcRecords);

  console.log(`[Sync] ✅ UGC sync complete: ${storeResult.count} synced`);

  return {
    success: true,
    synced_count: storeResult.count,
    errors_count: rawPosts.length - storeResult.count,
    total_fetched: rawPosts.length,
  };
}

/**
 * Sync business media posts to instagram_media table.
 * Thin wrapper: transport → normalize → persistence.
 *
 * @param {string} businessAccountId - UUID of the business account
 * @param {string} igUserId - Instagram Business Account ID
 * @param {string} pageToken - Page Access Token
 * @returns {Promise<{success: boolean, synced_count: number, total_fetched: number}>}
 */
async function syncBusinessPosts(businessAccountId, igUserId, pageToken) {
  console.log('[Sync] Starting business posts sync for:', businessAccountId);

  // TRANSPORT: raw fetch from IG API
  const result = await transport.fetchBusinessPosts(businessAccountId, 50);
  if (!result.success) {
    throw new Error(result.error || 'Failed to fetch business posts from Instagram API');
  }

  const posts = result.posts || [];
  console.log(`[Sync] Fetched ${posts.length} business posts`);

  if (posts.length === 0) {
    return { success: true, synced_count: 0, total_fetched: 0 };
  }

  // PERSISTENCE: batch upsert to instagram_media
  const storeResult = await persistence.storeBusinessPosts(businessAccountId, posts);

  console.log(`[Sync] ✅ Business posts sync complete: ${storeResult.count} synced`);

  return {
    success: true,
    synced_count: storeResult.count,
    total_fetched: posts.length,
  };
}

module.exports = {
  syncTaggedPosts,
  syncBusinessPosts
};
