// backend.api/routes/frontend/sync.js
// Data sync routes: sync UGC tagged posts, sync business posts.
// Frontend-facing — user-initiated, not governed by AcquisitionIntent contract.
// Uses IG fetcher modules (pure transport) + persistence substrate (pure DB write).

const express = require('express');
const router = express.Router();
const { logAudit: logAuditService } = require('../../config/supabase');
const { resolveAccountCredentials } = require('../../helpers/agent-helpers');
const dispatchWrite = require('../../substrates/db/writers').dispatchWrite;
const { mapRawPostToUgcContent } = require('../../acquisition-kernel/substrates/ugc/normalizer');
const ugcTransport = require('../../acquisition-kernel/substrates/ugc/transport');
const contentTransport = require('../../acquisition-kernel/substrates/content/transport');

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

    const creds = await resolveAccountCredentials(businessAccountId);
    const result = await ugcTransport.fetchTaggedMedia(businessAccountId, 50, creds);

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
      const rows = result.records
        .filter(p => p.id)
        .map(p => mapRawPostToUgcContent(p, businessAccountId, 'tagged', null));
      if (rows.length > 0) {
        dispatchWrite('batch_upsert_ugc', {
          domain: 'ugc', accountId: businessAccountId, intentId: null, table: 'ugc_content',
          rows,
        });
      }
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

    const result = await contentTransport.fetchPosts(businessAccountId, 50);

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
      const rows = result.posts
        .filter(p => p && p.id)
        .map(p => ({
          instagram_media_id: p.id,
          business_account_id: businessAccountId,
          media_type: p.media_type || null,
          caption: p.caption || null,
          media_url: p.media_url || null,
          thumbnail_url: p.thumbnail_url || null,
          permalink: p.permalink || null,
          like_count: p.like_count || 0,
          comments_count: p.comments_count || 0,
          published_at: p.timestamp || null,
        }));
      if (rows.length > 0) {
        dispatchWrite('batch_upsert_posts', {
          domain: 'media', accountId: businessAccountId, intentId: null, table: 'instagram_media',
          rows,
        });
      }
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
