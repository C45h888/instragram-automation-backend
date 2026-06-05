// substrates/parsing/workers/comments-worker.js
// Comments parsing worker: parse → hydrate → normalize → CK(DB_WRITE_REQUESTED).
//
// Owns: sequencing the engagement pipeline for comment data.
// Does NOT own: parsing logic (engagement parser), normalization (engagement
//               normalizer), hydration (media-hydrator), Supabase, governance policy.
//
// Phase 4: canonical path — uses domain substrate tools, no inline normalization.

const { parseComments } = require('../../substrates/engagement/parser');
const { normalizeComment } = require('../../substrates/engagement/normalizer');
const { hydrate: hydrateMedia } = require('../../substrates/engagement/hydrators/media-hydrator');

async function execute(rawData, accountId, intentId, extra = {}, governance) {
  // ── Flatten: batches or direct records ───────────────────────────────────
  const allComments = rawData.batches
    ? rawData.batches.flatMap(b => (b.comments || []).map(c => ({ ...c, _mediaId: b.mediaId })))
    : (rawData.records || []).map(c => ({ ...c, _mediaId: 'direct' }));

  if (!allComments.length) return { count: 0 };

  // ── Parse ────────────────────────────────────────────────────────────────
  const parsed = parseComments(allComments);
  if (!parsed.length) return { count: 0 };

  // ── Hydrate: resolve Instagram media IDs → DB UUIDs ──────────────────────
  const mediaIds = [...new Set(allComments.map(c => c._mediaId).filter(id => id !== 'direct'))];
  const mediaUUIDMap = new Map();

  if (mediaIds.length > 0 && governance) {
    const { resolved, missing } = await hydrateMedia(accountId, mediaIds, governance);

    for (const [igId, uuid] of resolved) mediaUUIDMap.set(igId, uuid);

    // Create stubs for any media IDs not yet in the DB
    if (missing.size > 0) {
      governance.dispatch({
        type: 'DB_WRITE_REQUESTED',
        domain: 'media',
        accountId, intentId,
        table: 'instagram_media',
        operation: 'batch_upsert_media_stubs',
        rows: [...missing].map(igId => ({
          instagram_media_id: igId,
          business_account_id: accountId,
        })),
      });
    }
  }

  // ── Normalize → DB rows ──────────────────────────────────────────────────
  const rows = [];
  for (const c of parsed) {
    const rawComment = allComments.find(ac => ac.id === c.id);
    const igMediaId = rawComment?._mediaId || 'direct';
    const uuid = mediaUUIDMap.get(igMediaId) || igMediaId;
    rows.push(normalizeComment(c, uuid, accountId));
  }

  if (!rows.length) return { count: 0 };

  // ── Constitutional dispatch ──────────────────────────────────────────────
  if (governance) {
    governance.dispatch({
      type: 'DB_WRITE_REQUESTED',
      domain: 'comments',
      accountId, intentId,
      table: 'instagram_comments',
      operation: 'batch_upsert_comments',
      rows,
    });
  }

  return { count: rows.length };
}

module.exports = { execute };
