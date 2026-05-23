// backend.api/routes/frontend/ugc.js
// UGC management routes: visitor-posts, feature toggle, permission requests
const express = require('express');
const router = express.Router();
const { validateTokenScopes } = require('../../services/tokens');
const { logAudit: logAuditService } = require('../../config/supabase');
const { getSupabaseAdmin } = require('../../config/supabase');

const logAudit = logAuditService;

// ==========================================
// ROUTES
// ==========================================

/**
 * GET /api/instagram/visitor-posts
 * Fetches visitor posts (UGC) from database cache
 */
router.get('/visitor-posts', async (req, res) => {
  const requestStartTime = Date.now();

  try {
    const { userId, limit = 20, offset = 0 } = req.query;
    const businessAccountId = req.query.businessAccountId || req.query.business_account_id;

    console.log('[UGC] Fetching visitor posts from database');
    console.log('   User ID:', userId);
    console.log('   Business Account ID:', businessAccountId);

    if (!userId || !businessAccountId) {
      console.error('❌ Missing required parameters');
      return res.status(400).json({
        success: false,
        error: 'Missing required parameters: userId and businessAccountId',
        code: 'MISSING_PARAMETERS'
      });
    }

    const scopeCheck = await validateTokenScopes(userId, businessAccountId, [
      'instagram_basic',
      'pages_read_user_content'
    ]);

    if (!scopeCheck.valid) {
      await logAudit('scope_check_failed', userId, {
        endpoint: '/visitor-posts',
        missing: scopeCheck.missing,
        business_account_id: businessAccountId
      });

      return res.status(403).json({
        success: false,
        error: `Missing required permissions: ${scopeCheck.missing.join(', ')}`,
        code: 'MISSING_SCOPES',
        missing: scopeCheck.missing
      });
    }

    const postsLimit = Math.min(Math.max(parseInt(limit) || 20, 1), 100);
    const postsOffset = Math.max(parseInt(offset) || 0, 0);

    const supabase = getSupabaseAdmin();

    const { data: posts, error, count } = await supabase
      .from('ugc_content')
      .select('*', { count: 'exact' })
      .eq('business_account_id', businessAccountId)
      .order('created_time', { ascending: false })
      .range(postsOffset, postsOffset + postsLimit - 1);

    if (error) {
      console.error('[UGC] Database query error:', error);

      await logAudit('visitor_posts_error', userId, {
        action: 'fetch_visitor_posts',
        error: error.message,
        source: 'database',
        business_account_id: businessAccountId,
        response_time_ms: Date.now() - requestStartTime
      });

      return res.status(500).json({
        success: false,
        error: 'Failed to fetch UGC posts from database',
        code: 'DATABASE_ERROR'
      });
    }

    // Calculate stats from database
    const { data: statsData } = await supabase
      .from('ugc_content')
      .select('sentiment, featured, created_time')
      .eq('business_account_id', businessAccountId);

    const weekAgo = new Date();
    weekAgo.setDate(weekAgo.getDate() - 7);

    const stats = {
      totalPosts: count || 0,
      postsThisWeek: statsData?.filter(p => {
        return new Date(p.created_time) > weekAgo;
      }).length || 0,
      sentimentBreakdown: {
        positive: statsData?.filter(p => p.sentiment === 'positive').length || 0,
        neutral: statsData?.filter(p => p.sentiment === 'neutral').length || 0,
        negative: statsData?.filter(p => p.sentiment === 'negative').length || 0,
      },
      featuredCount: statsData?.filter(p => p.featured).length || 0,
    };

    const responseTime = Date.now() - requestStartTime;

    await logAudit('posts_fetched', userId, {
      count: posts?.length || 0,
      source: 'database',
      endpoint: '/visitor-posts',
      business_account_id: businessAccountId,
      response_time_ms: responseTime
    });

    res.json({
      success: true,
      data: posts || [],
      pagination: {
        total: count,
        limit: postsLimit,
        offset: postsOffset,
        hasMore: (postsOffset + postsLimit) < (count || 0)
      },
      stats,
      source: 'database',
      meta: {
        response_time_ms: responseTime,
        note: 'Data synced from Instagram via /sync/ugc endpoint'
      }
    });

    console.log(`[UGC] ✅ Returned ${posts?.length || 0} posts from database (cached, ${responseTime}ms)`);

  } catch (error) {
    const responseTime = Date.now() - requestStartTime;
    console.error('[UGC] Error fetching visitor posts:', error);

    await logAudit('visitor_posts_error', null, {
      action: 'fetch_visitor_posts',
      error: error.message,
      response_time_ms: responseTime
    });

    res.status(500).json({
      success: false,
      error: error.message,
      code: 'INTERNAL_ERROR'
    });
  }
});

/**
 * PATCH /api/instagram/ugc/:postId/feature
 * Toggle featured status of a visitor post
 *
 * BUG FIX: Added `const supabase = getSupabaseAdmin()` — was using bare `supabase` before
 */
router.patch('/ugc/:postId/feature', async (req, res) => {
  try {
    const { postId } = req.params;
    const { featured } = req.body;

    console.log(`[UGC] Updating featured status for post ${postId}: ${featured}`);

    if (typeof featured !== 'boolean') {
      return res.status(400).json({
        success: false,
        error: 'featured must be a boolean',
        code: 'INVALID_PARAMETER'
      });
    }

    // BUG FIX: Was using bare `supabase` without declaration
    const supabase = getSupabaseAdmin();

    const { data, error } = await supabase
      .from('ugc_content')
      .update({
        featured,
        featured_at: featured ? new Date().toISOString() : null,
        updated_at: new Date().toISOString()
      })
      .eq('id', postId)
      .select()
      .single();

    if (error) {
      console.error('[UGC] Error updating featured status:', error);
      return res.status(400).json({
        success: false,
        error: error.message,
        code: 'UPDATE_FAILED'
      });
    }

    await logAudit('ugc_featured_updated', null, {
      action: 'update_featured_status',
      post_id: postId,
      featured
    });

    res.json({
      success: true,
      data,
      rate_limit: {
        remaining: req.rateLimitRemaining || 'unknown',
        limit: 200,
        window: '1 hour'
      }
    });

  } catch (error) {
    console.error('[UGC] Error in feature toggle:', error);
    res.status(500).json({
      success: false,
      error: error.message,
      code: 'INTERNAL_ERROR'
    });
  }
});

/**
 * POST /api/instagram/ugc/request-permission
 * Create a permission request record
 *
 * BUG FIX: Added `const supabase = getSupabaseAdmin()` — was using bare `supabase` before
 */
router.post('/ugc/request-permission', async (req, res) => {
  try {
    const { ugcContentId, requestedVia, requestMessage, permissionType } = req.body;

    console.log('[UGC] Creating permission request for content:', ugcContentId);

    if (!ugcContentId || !requestedVia || !requestMessage || !permissionType) {
      return res.status(400).json({
        success: false,
        error: 'Missing required fields: ugcContentId, requestedVia, requestMessage, permissionType',
        code: 'MISSING_FIELDS'
      });
    }

    // BUG FIX: Was using bare `supabase` without declaration
    const supabase = getSupabaseAdmin();

    // STEP 1: Get the UGC content
    const { data: ugcContent, error: ugcError } = await supabase
      .from('ugc_content')
      .select('*')
      .eq('id', ugcContentId)
      .single();

    if (ugcError || !ugcContent) {
      console.error('[UGC] UGC content not found:', ugcError?.message);
      return res.status(404).json({
        success: false,
        error: 'UGC content not found',
        code: 'UGC_NOT_FOUND'
      });
    }

    // STEP 2: Create permission request
    const { data: permission, error: permError } = await supabase
      .from('ugc_permissions')
      .insert({
        ugc_content_id: ugcContentId,
        business_account_id: ugcContent.business_account_id,
        requested_via: requestedVia,
        request_message: requestMessage,
        permission_type: permissionType,
        status: 'pending',
        requested_at: new Date().toISOString()
      })
      .select()
      .single();

    if (permError) {
      console.error('[UGC] Error creating permission request:', permError);
      return res.status(400).json({
        success: false,
        error: permError.message,
        code: 'PERMISSION_CREATE_FAILED'
      });
    }

    // STEP 3: Update UGC content flag
    await supabase
      .from('ugc_content')
      .update({
        repost_permission_requested: true,
        updated_at: new Date().toISOString()
      })
      .eq('id', ugcContentId);

    await logAudit('ugc_permission_requested', null, {
      action: 'request_permission',
      ugc_content_id: ugcContentId,
      permission_type: permissionType,
      requested_via: requestedVia
    });

    res.json({
      success: true,
      permission,
      message: 'Permission request created successfully',
      rate_limit: {
        remaining: req.rateLimitRemaining || 'unknown',
        limit: 200,
        window: '1 hour'
      }
    });

  } catch (error) {
    console.error('[UGC] Error requesting permission:', error);
    res.status(500).json({
      success: false,
      error: error.message,
      code: 'INTERNAL_ERROR'
    });
  }
});

module.exports = router;
