// backend.api/routes/frontend/media.js
// Media and profile routes: media fetch, profile, create-post
const express = require('express');
const router = express.Router();
const axios = require('axios');
const { retrievePageToken } = require('../../services/tokens');
const { logAudit: logAuditService } = require('../../config/supabase');
const { resolveAccountCredentials } = require('../../helpers/agent-helpers');
const { getSupabaseAdmin } = require('../../config/supabase');

const logAudit = logAuditService;
const GRAPH_API_VERSION = 'v23.0';

// ==========================================
// HELPER FUNCTIONS
// ==========================================

/**
 * Validate image or video URL for Instagram posting
 */
function validateImageUrl(url) {
  try {
    const parsedUrl = new URL(url);

    if (parsedUrl.protocol !== 'https:') {
      return { valid: false, error: 'Media URL must use HTTPS protocol' };
    }

    const hostname = parsedUrl.hostname.toLowerCase();
    if (hostname === 'localhost' || hostname === '127.0.0.1' || hostname.startsWith('192.168.')) {
      return {
        valid: false,
        error: 'Media must be publicly accessible (not localhost or private IP)'
      };
    }

    const validExtensions = ['.jpg', '.jpeg', '.png', '.gif', '.mp4', '.mov', '.avi'];
    const pathLower = parsedUrl.pathname.toLowerCase();
    const hasValidExtension = validExtensions.some(ext => pathLower.endsWith(ext));

    const isCdnHostname = /\.(supabase\.co|cloudflare\.com|amazonaws\.com|googleusercontent\.com|cdn\.|storage\.|digitaloceanspaces\.com|backblazeb2\.com|cloud\.apple\.com)$/.test(hostname);

    if (!hasValidExtension && !isCdnHostname) {
      return {
        valid: false,
        error: 'Media URL must have a valid image/video extension or be from a known CDN'
      };
    }

    return { valid: true };
  } catch (error) {
    return { valid: false, error: 'Invalid URL format' };
  }
}

// ==========================================
// ROUTES
// ==========================================

/**
 * GET /api/instagram/media/:accountId
 * Fetches Instagram media from database (Sync & Store pattern)
 */
router.get('/media/:accountId', async (req, res) => {
  const requestStartTime = Date.now();

  try {
    const { accountId } = req.params;
    const { limit = 25, offset = 0 } = req.query;
    const businessAccountId = req.query.businessAccountId || req.query.business_account_id;

    console.log(`🖼️  Fetching media from database for account: ${accountId}`);

    if (!businessAccountId) {
      console.error('❌ Missing businessAccountId parameter');
      return res.status(400).json({
        success: false,
        error: 'businessAccountId is required',
        code: 'MISSING_PARAMETERS'
      });
    }

    const mediaLimit = Math.min(Math.max(parseInt(limit) || 25, 1), 100);
    const mediaOffset = Math.max(parseInt(offset) || 0, 0);

    const supabase = getSupabaseAdmin();

    const { data: posts, error, count } = await supabase
      .from('instagram_media')
      .select('*', { count: 'exact' })
      .eq('business_account_id', businessAccountId)
      .order('published_at', { ascending: false })
      .range(mediaOffset, mediaOffset + mediaLimit - 1);

    if (error) {
      console.error('[Media] Database query error:', error);

      await logAudit('media_fetch_error', null, {
        action: 'fetch_media',
        business_account_id: businessAccountId,
        error: error.message,
        source: 'database',
        response_time_ms: Date.now() - requestStartTime
      });

      return res.status(500).json({
        success: false,
        error: 'Failed to fetch media from database',
        code: 'DATABASE_ERROR'
      });
    }

    const responseTime = Date.now() - requestStartTime;

    await logAudit('instagram_media_fetched', null, {
      action: 'fetch_media',
      business_account_id: businessAccountId,
      media_count: posts?.length || 0,
      source: 'database',
      response_time_ms: responseTime
    });

    res.json({
      success: true,
      data: posts || [],
      pagination: {
        total: count,
        limit: mediaLimit,
        offset: mediaOffset,
        hasMore: (mediaOffset + mediaLimit) < (count || 0)
      },
      source: 'database',
      meta: {
        count: posts?.length || 0,
        response_time_ms: responseTime,
        note: 'Data synced from Instagram via /sync/posts endpoint'
      }
    });

    console.log(`[Media] ✅ Returned ${posts?.length || 0} posts from database (cached, ${responseTime}ms)`);

  } catch (error) {
    const responseTime = Date.now() - requestStartTime;

    console.error('❌ Media fetch error:', error.message);

    await logAudit('media_fetch_error', null, {
      action: 'fetch_media',
      error: error.message,
      response_time_ms: responseTime
    });

    res.status(500).json({
      success: false,
      error: 'Failed to fetch Instagram media',
      code: 'INTERNAL_ERROR',
      details: process.env.NODE_ENV === 'development' ? error.message : undefined
    });
  }
});

/**
 * GET /api/instagram/profile/:id
 * Fetches Instagram Business profile data
 */
router.get('/profile/:id', async (req, res) => {
  const requestStartTime = Date.now();

  try {
    const { id } = req.params;
    const { userId } = req.query;
    const businessAccountId = req.query.businessAccountId || req.query.business_account_id;

    console.log(`👤 Fetching profile for IG account: ${id}`);

    if (!userId || !businessAccountId) {
      console.error('❌ Missing required query parameters for profile fetch');
      return res.status(400).json({
        success: false,
        error: 'userId and businessAccountId are required',
        code: 'MISSING_PARAMETERS'
      });
    }

    let pageToken;
    try {
      pageToken = await retrievePageToken(userId, businessAccountId);
    } catch (tokenError) {
      console.error('❌ Token retrieval failed:', tokenError.message);

      await logAudit('token_retrieval_failed', userId, {
        action: 'fetch_profile',
        business_account_id: businessAccountId,
        error: tokenError.message
      });

      return res.status(401).json({
        success: false,
        error: 'Authentication failed. Please reconnect your Instagram account.',
        code: 'TOKEN_RETRIEVAL_FAILED'
      });
    }

    const fields = 'id,username,name,profile_picture_url,followers_count,follows_count,media_count,biography,website';
    const graphApiUrl = `https://graph.facebook.com/${GRAPH_API_VERSION}/${id}`;

    try {
      const response = await axios.get(graphApiUrl, {
        params: {
          fields,
          access_token: pageToken
        },
        timeout: 10000
      });

      const responseTime = Date.now() - requestStartTime;

      await logAudit('instagram_profile_fetched', userId, {
        action: 'fetch_profile',
        business_account_id: businessAccountId,
        username: response.data.username,
        response_time_ms: responseTime
      });

      res.json({
        success: true,
        data: response.data,
        rate_limit: {
          remaining: req.rateLimitRemaining || 'unknown',
          limit: 200,
          window: '1 hour'
        },
        meta: {
          response_time_ms: responseTime
        }
      });

    } catch (apiError) {
      if (apiError.response) {
        const { status, data } = apiError.response;
        console.error(`❌ Graph API error (${status}):`, data);

        await logAudit('instagram_api_error', userId, {
          action: 'fetch_profile',
          business_account_id: businessAccountId,
          status_code: status,
          error_message: data.error?.message
        });

        return res.status(status).json({
          success: false,
          error: data.error?.message || 'Instagram API error',
          code: 'GRAPH_API_ERROR'
        });
      }

      throw apiError;
    }

  } catch (error) {
    const responseTime = Date.now() - requestStartTime;
    console.error('❌ Profile fetch error:', error.message);

    await logAudit('profile_fetch_error', req.query.userId, {
      action: 'fetch_profile',
      error: error.message,
      response_time_ms: responseTime
    });

    res.status(500).json({
      success: false,
      error: 'Failed to fetch Instagram profile',
      code: 'INTERNAL_ERROR',
      details: process.env.NODE_ENV === 'development' ? error.message : undefined
    });
  }
});

/**
 * POST /api/instagram/create-post
 * Creates a draft OR publishes a post to Instagram
 *
 * BUG FIX: Added `const supabase = getSupabaseAdmin()` — was using bare `supabase` before
 */
router.post('/create-post', async (req, res) => {
  const requestStartTime = Date.now();

  try {
    const {
      userId,
      businessAccountId,
      caption,
      image_url,
      status = 'draft'
    } = req.body;

    if (!userId || !businessAccountId || !caption || !image_url) {
      return res.status(400).json({
        success: false,
        error: 'Missing required fields: userId, businessAccountId, caption, image_url',
        code: 'MISSING_FIELDS'
      });
    }

    if (!['draft', 'publish'].includes(status)) {
      return res.status(400).json({
        success: false,
        error: 'status must be either "draft" or "publish"',
        code: 'INVALID_STATUS'
      });
    }

    if (caption.length > 2200) {
      return res.status(400).json({
        success: false,
        error: 'Caption exceeds maximum length of 2200 characters',
        code: 'CAPTION_TOO_LONG'
      });
    }

    const urlValidation = validateImageUrl(image_url);
    if (!urlValidation.valid) {
      return res.status(400).json({
        success: false,
        error: urlValidation.error,
        code: 'INVALID_IMAGE_URL'
      });
    }

    // BUG FIX: Was using bare `supabase` without declaration
    const supabase = getSupabaseAdmin();

    // ===== BRANCH 1: SAVE AS DRAFT =====
    if (status === 'draft') {
      console.log('💾 Saving post as draft (not publishing to Instagram)...');

      const { data: draftRecord, error: draftError } = await supabase
        .from('instagram_media')
        .insert({
          business_account_id: businessAccountId,
          caption,
          media_url: image_url,
          status: 'draft',
          media_type: 'IMAGE',
          instagram_media_id: `draft_${Date.now()}`,
          created_at: new Date().toISOString()
        })
        .select()
        .single();

      if (draftError) {
        console.error('❌ Draft save failed:', draftError);
        return res.status(500).json({
          success: false,
          error: 'Failed to save draft',
          code: 'DRAFT_SAVE_FAILED',
          details: draftError.message
        });
      }

      await logAudit('instagram_draft_saved', userId, {
        action: 'save_draft',
        business_account_id: businessAccountId,
        draft_id: draftRecord.id,
        caption_length: caption.length
      });

      return res.json({
        success: true,
        message: 'Post saved as draft',
        data: {
          draft_id: draftRecord.id,
          status: 'draft',
          can_publish: true
        },
        meta: {
          response_time_ms: Date.now() - requestStartTime
        }
      });
    }

    // ===== BRANCH 2: PUBLISH TO INSTAGRAM =====
    console.log('🚀 Publishing post to Instagram (2-step flow)...');

    let pageToken, igUserId;
    try {
      ({ pageToken, igUserId } = await resolveAccountCredentials(businessAccountId));
    } catch (tokenError) {
      console.error('❌ Token retrieval failed:', tokenError.message);

      await logAudit('token_retrieval_failed', userId, {
        action: 'create_post',
        business_account_id: businessAccountId,
        error: tokenError.message
      });

      return res.status(401).json({
        success: false,
        error: 'Authentication failed. Please reconnect your Instagram account.',
        code: 'TOKEN_RETRIEVAL_FAILED'
      });
    }

    // STEP 1: Create Media Container
    console.log('   Step 1: Creating media container...');
    let creationId;

    try {
      const containerUrl = `https://graph.facebook.com/${GRAPH_API_VERSION}/${igUserId}/media`;
      const containerResponse = await axios.post(containerUrl, null, {
        params: {
          image_url: image_url,
          caption: caption,
          access_token: pageToken
        },
        timeout: 15000
      });

      creationId = containerResponse.data.id;
      console.log(`   ✅ Step 1 Success: creation_id = ${creationId}`);

      await logAudit('instagram_container_created', userId, {
        action: 'create_post_step_1',
        business_account_id: businessAccountId,
        creation_id: creationId
      });

    } catch (containerError) {
      console.error('❌ Container creation failed:', containerError.response?.data || containerError.message);

      await logAudit('instagram_container_error', userId, {
        action: 'create_post_step_1',
        business_account_id: businessAccountId,
        error: containerError.response?.data?.error?.message || containerError.message
      });

      if (containerError.response) {
        const { status, data } = containerError.response;
        return res.status(status).json({
          success: false,
          error: data.error?.message || 'Failed to create media container',
          code: 'CONTAINER_CREATION_FAILED',
          details: data.error
        });
      }

      throw containerError;
    }

    // STEP 2: Publish Media Container
    console.log('   Step 2: Publishing container...');
    let mediaId;

    try {
      const publishUrl = `https://graph.facebook.com/${GRAPH_API_VERSION}/${igUserId}/media_publish`;
      const publishResponse = await axios.post(publishUrl, null, {
        params: {
          creation_id: creationId,
          access_token: pageToken
        },
        timeout: 15000
      });

      mediaId = publishResponse.data.id;
      console.log(`   ✅ Step 2 Success: Post is live! media_id = ${mediaId}`);

      // STEP 3: Store in database with 'published' status
      const { data: publishedRecord, error: dbError } = await supabase
        .from('instagram_media')
        .insert({
          business_account_id: businessAccountId,
          instagram_media_id: mediaId,
          caption,
          media_url: image_url,
          status: 'published',
          media_type: 'IMAGE',
          published_at: new Date().toISOString(),
          created_at: new Date().toISOString()
        })
        .select()
        .single();

      if (dbError) {
        console.warn('⚠️  Post published to Instagram but failed to save to database:', dbError);
      }

      const responseTime = Date.now() - requestStartTime;

      await logAudit('instagram_post_published', userId, {
        action: 'publish_post',
        business_account_id: businessAccountId,
        media_id: mediaId,
        creation_id: creationId,
        response_time_ms: responseTime,
        caption_length: caption.length
      });

    } catch (publishError) {
      console.error('❌ Publishing failed:', publishError.response?.data || publishError.message);

      await logAudit('instagram_publish_error', userId, {
        action: 'create_post_step_2',
        business_account_id: businessAccountId,
        creation_id: creationId,
        error: publishError.response?.data?.error?.message || publishError.message
      });

      if (publishError.response) {
        const { status, data } = publishError.response;
        return res.status(status).json({
          success: false,
          error: data.error?.message || 'Failed to publish post',
          code: 'PUBLISH_FAILED',
          details: data.error,
          partial_success: {
            creation_id: creationId,
            status: 'Container created but not published'
          }
        });
      }

      throw publishError;
    }

    // ===== SUCCESS RESPONSE =====
    const totalTime = Date.now() - requestStartTime;

    res.json({
      success: true,
      message: 'Post published successfully!',
      data: {
        media_id: mediaId,
        creation_id: creationId,
        status: 'published',
        permalink: `https://www.instagram.com/p/${mediaId}/`
      },
      rate_limit: {
        remaining: req.rateLimitRemaining || 'unknown',
        limit: 200,
        window: '1 hour'
      },
      meta: {
        response_time_ms: totalTime
      }
    });

  } catch (error) {
    const responseTime = Date.now() - requestStartTime;

    console.error('❌ Post creation error:', error.message);

    await logAudit('post_creation_error', req.body.userId, {
      action: 'create_post',
      error: error.message,
      response_time_ms: responseTime
    });

    res.status(500).json({
      success: false,
      error: 'Failed to publish post.',
      code: 'INTERNAL_ERROR',
      details: process.env.NODE_ENV === 'development' ? error.message : undefined
    });
  }
});

module.exports = router;
