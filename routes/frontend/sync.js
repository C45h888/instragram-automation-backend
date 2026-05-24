// backend.api/routes/frontend/sync.js
// Data sync routes: sync UGC tagged posts, sync business posts.
// Frontend-facing — user-initiated, not governed by AcquisitionIntent contract.
// Uses IG fetcher modules (pure transport) + persistence substrate (pure DB write).

const express = require('express');
const router = express.Router();
const { logAudit: logAuditService } = require('../../config/supabase');
const persistence = require('../../substrates/persistence');
const { mapRawPostToUgcContent } = require('../../substrates/normalization');
const igFetcherUgc = require('../../control-plane/execution/ig-fetcher-ugc');
const igFetcherMedia = require('../../control-plane/execution/ig-fetcher-media');

const logAudit = logAuditService;

// ==========================================
// ROUTES
// ==========================================

/**
 * POST /api/instagram/sync/ugc
 * Triggers sync of tagged posts from Instagram to database.
 * User-initiated (frontend) — direct transport + persistence.
 */
router.post('/sync/ugc', async (req, res) => {
  try {
    const { businessAccountId } = req.body;

    if (!businessAccountId) {
      return res.status(400).json({ success: false, error: 'businessAccountId is required' });
    }

    const creds = await persistence.resolveAccountCredentials(businessAccountId);
    const result = await igFetcherUgc.fetchTaggedMedia(businessAccountId, 50, creds);

    if (!result.success) {
      return res.status(result.retryable === false ? 401 : 500).json({
        success: false,
        error: result.error,
        code: result.code,
        retryable: result.retryable,
        error_category: result.error_category,
      });
    }

    if (result.records?.length > 0) {
      const records = result.records
        .filter(p => p.id)
        .map(p => mapRawPostToUgcContent(p, businessAccountId, 'tagged', null));
      await persistence.storeUgcContentBatch(records);
    }

    res.json({ success: true, synced_count: result.count || 0 });

    await logAudit('ugc_sync_completed', null, {
      business_account_id: businessAccountId,
      synced_count: result.count || 0,
    });

  } catch (error) {
    console.error('[sync/ugc] Error:', error.message);
    res.status(500).json({ success: false, error: error.message || 'Sync failed' });
  }
});

/**
 * POST /api/instagram/sync/posts
 * Triggers sync of business media from Instagram to database.
 * User-initiated (frontend) — direct transport + persistence.
 */
router.post('/sync/posts', async (req, res) => {
  try {
    const { businessAccountId } = req.body;

    if (!businessAccountId) {
      return res.status(400).json({ success: false, error: 'businessAccountId is required' });
    }

    const result = await igFetcherMedia.fetchBusinessPosts(businessAccountId, 50);

    if (!result.success) {
      return res.status(result.retryable === false ? 401 : 500).json({
        success: false,
        error: result.error,
        code: result.code,
        retryable: result.retryable,
        error_category: result.error_category,
      });
    }

    if (result.posts?.length > 0) {
      await persistence.storeBusinessPosts(businessAccountId, result.posts);
    }

    res.json({ success: true, synced_count: result.count || 0 });

    await logAudit('business_posts_sync_completed', null, {
      business_account_id: businessAccountId,
      synced_count: result.count || 0,
    });

  } catch (error) {
    console.error('[sync/posts] Error:', error.message);
    res.status(500).json({ success: false, error: error.message || 'Sync failed' });
  }
});

module.exports = router;
