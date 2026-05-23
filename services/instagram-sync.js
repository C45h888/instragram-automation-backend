/**
 * Instagram Data Sync Service
 * Fetches fresh data from Instagram Graph API and syncs to database
 *
 * Purpose: Populate ugc_content and instagram_media tables with real data
 * Handles: CDN link rot, permission preservation, duplicate prevention
 */

const axios = require('axios');
const { createClient } = require('@supabase/supabase-js');

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

const GRAPH_API_VERSION = 'v23.0';

/**
 * ==========================================
 * LAYER A: ENGINE (WRITE)
 * Split-Brain Architecture - Data Fetching Layer
 * ==========================================
 *
 * Sync tagged posts (UGC) from Instagram to ugc_content table
 *
 * Responsibilities:
 *   - Fetches data from Instagram Tags API (/{ig-user-id}/tags)
 *   - Writes to Supabase database
 *   - Preserves human-set permission flags
 *   - Updates media URLs to prevent link rot
 *
 * @param {string} businessAccountId - UUID of the business account
 * @param {string} igUserId - Instagram Business Account ID
 * @param {string} pageToken - Page Access Token
 * @returns {Promise<{success: boolean, synced_count: number, errors_count: number, total_fetched: number}>}
 */
async function syncTaggedPosts(businessAccountId, igUserId, pageToken) {
  console.log('[Sync] Starting UGC sync for business account:', businessAccountId);

  try {
    // Include all necessary fields, especially thumbnail_url for videos
    const fields = 'id,media_type,media_url,thumbnail_url,caption,permalink,timestamp,username,like_count,comments_count';
    const graphUrl = `https://graph.facebook.com/${GRAPH_API_VERSION}/${igUserId}/tags`;

    const response = await axios.get(graphUrl, {
      params: {
        fields,
        access_token: pageToken,
        limit: 50
      },
      timeout: 15000
    });

    const posts = response.data.data || [];
    console.log(`[Sync] Fetched ${posts.length} tagged posts from Instagram API`);

    let syncedCount = 0;
    let errors = 0;

    for (const post of posts) {
      try {
        // STEP 1: Check existing record for permission preservation
        const { data: existing } = await supabase
          .from('ugc_content')
          .select('repost_permission_granted, repost_permission_requested')
          .eq('visitor_post_id', post.id)
          .single();

        // STEP 2: UPSERT with fresh media URLs (prevents CDN link rot)
        // Column names aligned to database schema (ugc_content table)
        const { error } = await supabase.from('ugc_content').upsert({
          visitor_post_id: post.id,                    // FIXED: was instagram_media_id
          business_account_id: businessAccountId,
          author_id: post.username,                    // Required NOT NULL field (using username as ID)
          author_username: post.username,
          media_url: post.media_url,                   // ✅ Always refresh (link rot fix)
          thumbnail_url: post.thumbnail_url,           // ✅ Required for VIDEO support
          message: post.caption || '',                 // FIXED: was caption
          created_time: post.timestamp,                // FIXED: was timestamp
          media_type: post.media_type,
          permalink_url: post.permalink,               // FIXED: was permalink
          like_count: post.like_count || 0,
          comment_count: post.comments_count || 0,     // FIXED: was comments_count (singular)
          // ✅ Preserve existing permission status if post exists
          repost_permission_granted: existing?.repost_permission_granted ?? null,
          repost_permission_requested: existing?.repost_permission_requested ?? false,
          sentiment: null,
          featured: false,
          updated_at: new Date().toISOString()
        }, { onConflict: 'business_account_id,visitor_post_id' });  // FIXED: matches database constraint

        if (error) {
          console.error(`[Sync] Failed to upsert post ${post.id}:`, error.message);
          errors++;
        } else {
          syncedCount++;
        }

      } catch (postError) {
        console.error(`[Sync] Error processing post ${post.id}:`, postError.message);
        errors++;
      }
    }

    console.log(`[Sync] ✅ UGC sync complete: ${syncedCount} synced, ${errors} errors`);

    return {
      success: true,
      synced_count: syncedCount,
      errors_count: errors,
      total_fetched: posts.length
    };

  } catch (error) {
    console.error('[Sync] ❌ UGC sync failed:', error.message);
    throw new Error(`UGC sync failed: ${error.message}`);
  }
}

/**
 * Sync business media posts to instagram_media table
 *
 * @param {string} businessAccountId - UUID of the business account
 * @param {string} igUserId - Instagram Business Account ID
 * @param {string} pageToken - Page Access Token
 * @returns {Promise<{success: boolean, synced_count: number, total_fetched: number}>}
 */
async function syncBusinessPosts(businessAccountId, igUserId, pageToken) {
  console.log('[Sync] Starting business posts sync for:', businessAccountId);

  try {
    const fields = 'id,media_type,media_url,thumbnail_url,caption,permalink,timestamp,like_count,comments_count';
    const graphUrl = `https://graph.facebook.com/${GRAPH_API_VERSION}/${igUserId}/media`;

    const response = await axios.get(graphUrl, {
      params: {
        fields,
        access_token: pageToken,
        limit: 50
      },
      timeout: 15000
    });

    const posts = response.data.data || [];
    console.log(`[Sync] Fetched ${posts.length} business posts`);

    let syncedCount = 0;

    for (const post of posts) {
      try {
        // UPSERT with fresh media URLs
        const { error } = await supabase.from('instagram_media').upsert({
          instagram_media_id: post.id,
          business_account_id: businessAccountId,
          media_url: post.media_url,
          thumbnail_url: post.thumbnail_url,
          caption: post.caption || '',
          media_type: post.media_type,
          permalink: post.permalink,
          like_count: post.like_count || 0,
          comments_count: post.comments_count || 0,
          status: 'published',
          published_at: post.timestamp,
          updated_at: new Date().toISOString()
        }, { onConflict: 'instagram_media_id' });

        if (error) {
          console.error(`[Sync] Failed to upsert post ${post.id}:`, error.message);
        } else {
          syncedCount++;
        }

      } catch (postError) {
        console.error(`[Sync] Error processing post ${post.id}:`, postError.message);
      }
    }

    console.log(`[Sync] ✅ Business posts sync complete: ${syncedCount} synced`);

    return {
      success: true,
      synced_count: syncedCount,
      total_fetched: posts.length
    };

  } catch (error) {
    console.error('[Sync] ❌ Business posts sync failed:', error.message);
    throw new Error(`Business posts sync failed: ${error.message}`);
  }
}

module.exports = {
  syncTaggedPosts,
  syncBusinessPosts
};
