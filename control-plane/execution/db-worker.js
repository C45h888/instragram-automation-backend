// control-plane/execution/db-worker.js
// Canonical DB Worker: single persistence authority for publishing operations.
//
// Owns: ALL Supabase reads/writes for the publishing pipeline.
// Does NOT own: IG API calls, transport, retry logic, orchestration, governance.
//
// Called by the orchestrator and runtime modules. No other module in the
// control plane or runtime should call getSupabaseAdmin() directly.
//
// Every function is a pure DB operation — no transport, no governance,
// no side effects beyond the target row.

const { getSupabaseAdmin } = require('../../config/supabase');

// ── Asset resolution ─────────────────────────────────────────────────────────

/**
 * Resolves an asset by ID, returning storage_path, media_type, and caption.
 * @param {string} assetId
 * @returns {Promise<{storage_path: string|null, media_type: string, caption: string}|null>}
 */
async function resolveAsset(assetId) {
  const supabase = getSupabaseAdmin();
  if (!supabase) return null;

  const { data: asset } = await supabase
    .from('instagram_assets')
    .select('storage_path, media_type, caption')
    .eq('id', assetId)
    .single();

  return asset || null;
}

// ── Scanning: scheduled_posts ───────────────────────────────────────────────

/**
 * Returns approved scheduled_posts for an account.
 * @param {string} accountId
 * @returns {Promise<Array<{id: string, business_account_id: string, asset_id: string}>>}
 */
async function getApprovedScheduledPosts(accountId) {
  const supabase = getSupabaseAdmin();
  if (!supabase) return [];

  const { data: posts, error } = await supabase
    .from('scheduled_posts')
    .select('id, business_account_id, asset_id')
    .eq('business_account_id', accountId)
    .eq('status', 'approved');

  return error ? [] : (posts || []);
}

// ── Scanning: post_queue ────────────────────────────────────────────────────

/**
 * Returns retryable post_queue rows for an account.
 * @param {string} accountId
 * @returns {Promise<Array>}
 */
async function getRetryablePostQueue(accountId) {
  const supabase = getSupabaseAdmin();
  if (!supabase) return [];

  const now = new Date().toISOString();

  const { data: rows, error } = await supabase
    .from('post_queue')
    .select('*')
    .eq('business_account_id', accountId)
    .in('status', ['pending', 'failed'])
    .or(`next_retry_at.is.null,next_retry_at.lte.${now}`)
    .order('created_at', { ascending: true })
    .limit(50);

  return error ? [] : (rows || []);
}

// ── Status transitions: scheduled_posts ─────────────────────────────────────

/**
 * Marks a scheduled_post as 'publishing'.
 * Idempotent — only transitions from 'approved'.
 */
async function markScheduledPostPublishing(postId) {
  const supabase = getSupabaseAdmin();
  if (!supabase) return;

  await supabase
    .from('scheduled_posts')
    .update({ status: 'publishing' })
    .eq('id', postId)
    .eq('status', 'approved');
}

/**
 * Marks a scheduled_post as 'failed' (missing asset, evaluation rejection, etc.).
 */
async function markScheduledPostFailed(postId) {
  const supabase = getSupabaseAdmin();
  if (!supabase) return;

  await supabase
    .from('scheduled_posts')
    .update({ status: 'failed' })
    .eq('id', postId);
}

/**
 * Marks a scheduled_post as 'published' with Instagram media ID.
 */
async function markScheduledPostPublished(postId, instagramMediaId) {
  const supabase = getSupabaseAdmin();
  if (!supabase) return;

  await supabase
    .from('scheduled_posts')
    .update({
      status: 'published',
      instagram_media_id: instagramMediaId,
      published_at: new Date().toISOString(),
    })
    .eq('id', postId);
}

// ── Status transitions: post_queue ──────────────────────────────────────────

/**
 * Marks a post_queue row as 'processing' to prevent concurrent pickup.
 * Idempotent — only transitions from the row's current status.
 */
async function markPostQueueProcessing(rowId, currentStatus) {
  const supabase = getSupabaseAdmin();
  if (!supabase) return;

  await supabase
    .from('post_queue')
    .update({ status: 'processing' })
    .eq('id', rowId)
    .eq('status', currentStatus);
}

/**
 * Marks a post_queue row as 'sent' with the Instagram result ID.
 */
async function markPostQueueSent(rowId, instagramId) {
  const supabase = getSupabaseAdmin();
  if (!supabase) return;

  await supabase
    .from('post_queue')
    .update({ status: 'sent', instagram_id: instagramId })
    .eq('id', rowId);
}

// ── Status transitions: ugc_permissions ─────────────────────────────────────

/**
 * Marks a UGC permission as 'reposted' with the Instagram media ID.
 */
async function markUgcPermissionReposted(permissionId, instagramId) {
  const supabase = getSupabaseAdmin();
  if (!supabase) return;

  await supabase
    .from('ugc_permissions')
    .update({
      status: 'reposted',
      instagram_media_id: instagramId,
      reposted_at: new Date().toISOString(),
    })
    .eq('id', permissionId);
}

// ── UGC content resolution ──────────────────────────────────────────────────

/**
 * Resolves UGC content by permission_id, joining ugc_permissions → ugc_content.
 * Returns { media_url, caption, media_type } for transport pre-resolution.
 * @param {string} permissionId
 * @returns {Promise<{media_url: string, caption: string, media_type: string}|null>}
 */
async function resolveUgcContent(permissionId) {
  const supabase = getSupabaseAdmin();
  if (!supabase) return null;

  const { data: perm } = await supabase
    .from('ugc_permissions')
    .select('ugc_content_id')
    .eq('id', permissionId)
    .single();

  if (!perm) return null;

  const { data: ugc } = await supabase
    .from('ugc_content')
    .select('media_url, message, author_username, media_type')
    .eq('id', perm.ugc_content_id)
    .single();

  if (!ugc || !ugc.media_url) return null;

  const caption = ugc.message
    ? `📸 @${ugc.author_username}: ${ugc.message}\n\n#repost`
    : `📸 @${ugc.author_username}\n\n#repost`;

  return {
    media_url: ugc.media_url,
    caption,
    media_type: ugc.media_type || 'IMAGE',
  };
}

module.exports = {
  resolveAsset,
  getApprovedScheduledPosts,
  getRetryablePostQueue,
  markScheduledPostPublishing,
  markScheduledPostFailed,
  markScheduledPostPublished,
  markPostQueueProcessing,
  markPostQueueSent,
  markUgcPermissionReposted,
  resolveUgcContent,
};
