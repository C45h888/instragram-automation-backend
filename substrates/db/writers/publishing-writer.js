// substrates/db/writers/publishing-writer.js
// Publishing writer: status marks for publish pipeline tables.
//
// Owns: Supabase UPDATE operations for post_queue, scheduled_posts,
//        ugc_permissions status transitions.
// Does NOT own: governance, fetch, orchestration, IG API publishing.

const { getSupabaseAdmin } = require('../../../config/supabase');

/**
 * Execute a publishing status mark operation.
 * Supports: mark_post_queue_sent, mark_scheduled_post_published,
 *           mark_ugc_permission_reposted, mark_account_disconnected.
 */
async function execute(params, governance) {
  const { domain, accountId, intentId, operation, table, rows, metadata } = params;
  const supabase = getSupabaseAdmin();
  if (!supabase) {
    governance?.dispatch({ type: 'DB_WRITE_COMPLETE', domain, accountId, intentId, table: table || 'publishing', count: 0, error: 'supabase_unavailable' });
    return;
  }

  try {
    let count = 0;

    switch (operation) {
      case 'mark_post_queue_sent': {
        const { rowId, instagramId } = metadata || {};
        if (rowId) {
          await supabase.from('post_queue').update({ status: 'sent', instagram_id: instagramId }).eq('id', rowId);
          count = 1;
        }
        break;
      }
      case 'mark_scheduled_post_published': {
        const { postId, instagramId } = metadata || {};
        if (postId) {
          await supabase.from('scheduled_posts').update({
            status: 'published', instagram_media_id: instagramId, published_at: new Date().toISOString(),
          }).eq('id', postId);
          count = 1;
        }
        break;
      }
      case 'mark_ugc_permission_reposted': {
        const { permissionId, instagramId } = metadata || {};
        if (permissionId) {
          await supabase.from('ugc_permissions').update({
            status: 'reposted', instagram_media_id: instagramId, reposted_at: new Date().toISOString(),
          }).eq('id', permissionId);
          count = 1;
        }
        break;
      }
      case 'mark_account_disconnected': {
        if (accountId) {
          await supabase.from('instagram_business_accounts').update({
            is_connected: false, connection_status: 'disconnected',
          }).eq('id', accountId);
          count = 1;
        }
        break;
      }
      default: {
        governance?.dispatch({ type: 'DB_WRITE_COMPLETE', domain, accountId, intentId, table: 'publishing', count: 0, error: `unknown_publishing_op: ${operation}` });
        return;
      }
    }

    governance?.dispatch({ type: 'DB_WRITE_COMPLETE', domain, accountId, intentId, table: table || 'publishing', count, error: null });
  } catch (err) {
    governance?.dispatch({ type: 'DB_WRITE_COMPLETE', domain, accountId, intentId, table: table || 'publishing', count: 0, error: err.message });
  }
}

module.exports = { execute };
