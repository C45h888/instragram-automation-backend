// backend.api/routes/frontend/sync.js
// Data sync routes: sync UGC tagged posts, sync business posts.
//
// Both routes delegate to domain fetchers which handle:
//   - credential resolution (resolveAccountCredentials)
//   - Instagram Graph API calls
//   - Supabase write-through with domain-tagged logging

const express = require('express');
const router = express.Router();
const { fetchAndStoreTaggedMedia } = require('../../helpers/data-fetchers/ugc-fetchers');
const { fetchAndStoreBusinessPosts } = require('../../helpers/data-fetchers/media-fetchers');
const { logAudit: logAuditService } = require('../../config/supabase');

const logAudit = logAuditService;

// ==========================================
// ROUTES
// ==========================================

/**
 * POST /api/instagram/sync/ugc
 * Triggers background sync of tagged posts from Instagram to database.
 * Delegates to ugc-fetchers (fetchAndStoreTaggedMedia) — handles credentials,
 * Graph API call, and ugc_content upsert with domain='ugc' logging.
 */
router.post('/sync/ugc', async (req, res) => {
  try {
    const { businessAccountId } = req.body;

    if (!businessAccountId) {
      return res.status(400).json({ success: false, error: 'businessAccountId is required' });
    }

    const result = await fetchAndStoreTaggedMedia(businessAccountId, 50);

    if (!result.success) {
      return res.status(result.retryable === false ? 401 : 500).json({
        success: false,
        error: result.error,
        code: result.code,
        retryable: result.retryable,
        error_category: result.error_category,
      });
    }

    res.json({ success: true, synced_count: result.count });

    await logAudit('ugc_sync_completed', null, {
      business_account_id: businessAccountId,
      synced_count: result.count
    });

  } catch (error) {
    console.error('[sync/ugc] Error:', error.message);
    res.status(500).json({ success: false, error: error.message || 'Sync failed' });
  }
});

/**
 * POST /api/instagram/sync/posts
 * Triggers background sync of business media from Instagram to database.
 * Delegates to media-fetchers (fetchAndStoreBusinessPosts) — handles credentials,
 * Graph API call, and instagram_media upsert with domain='media' logging.
 */
router.post('/sync/posts', async (req, res) => {
  try {
    const { businessAccountId } = req.body;

    if (!businessAccountId) {
      return res.status(400).json({ success: false, error: 'businessAccountId is required' });
    }

    const result = await fetchAndStoreBusinessPosts(businessAccountId, 50);

    if (!result.success) {
      return res.status(result.retryable === false ? 401 : 500).json({
        success: false,
        error: result.error,
        code: result.code,
        retryable: result.retryable,
        error_category: result.error_category,
      });
    }

    res.json({ success: true, synced_count: result.count });

    await logAudit('business_posts_sync_completed', null, {
      business_account_id: businessAccountId,
      synced_count: result.count
    });

  } catch (error) {
    console.error('[sync/posts] Error:', error.message);
    res.status(500).json({ success: false, error: error.message || 'Sync failed' });
  }
});

module.exports = router;
