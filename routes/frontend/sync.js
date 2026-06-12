// backend.api/routes/frontend/sync.js
// ⚠️  DEPRECATED — ugc/content polling fetch logic removed (Phase A).
//     Both endpoints below will return 501 Not Implemented. UGC data is
//     now sourced from the webhook acquisition path. Frontend should be
//     updated to consume webhook-driven data instead of these polling
//     endpoints. This file is kept as a stub to preserve the route mount.

const express = require('express');
const router = express.Router();
const { logAudit: logAuditService } = require('../../config/supabase');

console.warn(
  '[routes/frontend/sync] DEPRECATED polling-sync endpoints loaded as 501 stubs. ' +
  'UGC + content data is now sourced from the webhook acquisition path. ' +
  'Update frontend callers before removing this file.'
);

const logAudit = logAuditService;

// ==========================================
// ROUTES — STUBS
// ==========================================

/**
 * POST /api/instagram/sync/ugc — REMOVED
 * Previously triggered sync of tagged posts via ugc-transport. Polling
 * fetch path retired. UGC data is delivered via webhooks.
 */
router.post('/sync/ugc', async (req, res) => {
  res.status(501).json({
    success: false,
    error: 'deprecated',
    message: 'UGC polling sync is deprecated. UGC data is now delivered via Meta webhooks.',
  });
  await logAudit('ugc_sync_deprecated_hit', null, { business_account_id: req.body?.businessAccountId || null });
});

/**
 * POST /api/instagram/sync/posts — REMOVED
 * Previously triggered sync of business media via content-transport.
 * Polling fetch path retired. Media data is delivered via webhooks.
 */
router.post('/sync/posts', async (req, res) => {
  res.status(501).json({
    success: false,
    error: 'deprecated',
    message: 'Content polling sync is deprecated. Media data is now delivered via Meta webhooks.',
  });
  await logAudit('posts_sync_deprecated_hit', null, { business_account_id: req.body?.businessAccountId || null });
});

module.exports = router;
