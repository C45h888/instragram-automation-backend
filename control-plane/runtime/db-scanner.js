// control-plane/runtime/db-scanner.js
// DB Scanner: governed DB polling for publishing intents.
//
// Owns: scanning scheduled_posts and post_queue for items ready to publish,
//        building canonical intents, LPUSHing to Redis domain queues.
// Does NOT own: DB reads/writes (delegates to db-worker), execution
//               (orchestrator handles via HSM), IG API calls, retry logic.
//
// Called by the orchestrator cadence loop (90s). Every scan LPUSHes intents
// to Redis queues already polled by governance.tick().
//
// Flow: db-worker reads → LPUSH Redis → governance.tick() discovers → HSM → execute

const { getRedisClient } = require('../../config/redis');
const { getActiveAccounts } = require('../../substrates/persistence');
const dbWorker = require('../execution/db-worker');

// ── Action → domain mapping ──────────────────────────────────────────────────

function domainForAction(actionType) {
  if (actionType === 'publish_post') return 'media';
  if (actionType === 'repost_ugc') return 'ugc';
  return 'messaging';
}

function fetchTypeForAction(actionType) {
  if (actionType === 'publish_post') return 'publish_media';
  if (actionType === 'repost_ugc') return 'publish_ugc';
  return 'publish_messaging';
}

// ── Intent builder ──────────────────────────────────────────────────────────

function buildIntent(accountId, actionType, payload, queueRowId, scheduledPostId) {
  return {
    intent_id: require('crypto').randomUUID(),
    account_id: accountId,
    fetch_type: fetchTypeForAction(actionType),
    action_type: actionType,
    payload,
    priority: 'normal',
    issued_at: new Date().toISOString(),
    queue_row_id: queueRowId || null,
    scheduled_post_id: scheduledPostId || null,
    intent_type: scheduledPostId ? 'scheduled_post' : 'post_queue',
  };
}

// ── Scan scheduled_posts ────────────────────────────────────────────────────

async function scanScheduledPosts(redis, accountId) {
  const posts = await dbWorker.getApprovedScheduledPosts(accountId);
  if (!posts.length) return 0;

  let emitted = 0;

  for (const post of posts) {
    const asset = await dbWorker.resolveAsset(post.asset_id);

    if (!asset?.storage_path) {
      console.warn(`[db-scanner] Scheduled post ${post.id} missing asset, marking failed`);
      await dbWorker.markScheduledPostFailed(post.id);
      continue;
    }

    const intent = buildIntent(
      accountId, 'publish_post',
      {
        image_url: asset.storage_path,
        caption: asset.caption || '',
        media_type: asset.media_type || 'IMAGE',
        scheduled_post_id: post.id,
      },
      null, post.id
    );

    const queueKey = `supervisor:acquisitions:publish:media:${accountId}`;
    await redis.lpush(queueKey, JSON.stringify(intent));

    await dbWorker.markScheduledPostPublishing(post.id);

    console.log(`[db-scanner] Emitted publish:media intent for scheduled_post ${post.id}`);
    emitted++;
  }

  return emitted;
}

// ── Scan post_queue ─────────────────────────────────────────────────────────

async function scanPostQueue(redis, accountId) {
  const rows = await dbWorker.getRetryablePostQueue(accountId);
  if (!rows.length) return 0;

  let emitted = 0;

  for (const row of rows) {
    const domain = domainForAction(row.action_type);
    const queueKey = `supervisor:acquisitions:publish:${domain}:${accountId}`;

    const intent = buildIntent(
      accountId, row.action_type, row.payload,
      row.id, row.payload?.scheduled_post_id || null
    );

    await redis.lpush(queueKey, JSON.stringify(intent));

    await dbWorker.markPostQueueProcessing(row.id, row.status);

    console.log(`[db-scanner] Emitted publish:${domain} intent for post_queue row ${row.id} (action: ${row.action_type})`);
    emitted++;
  }

  return emitted;
}

// ── Main scan (called by orchestrator cadence) ──────────────────────────────

async function runScan() {
  const redis = getRedisClient();
  if (!redis || redis.status !== 'ready') {
    return { ok: false, error: 'redis unavailable', totalEmitted: 0 };
  }

  const accounts = await getActiveAccounts();
  let totalEmitted = 0;

  for (const account of accounts) {
    try {
      const mediaCount = await scanScheduledPosts(redis, account.id);
      const queueCount = await scanPostQueue(redis, account.id);
      totalEmitted += mediaCount + queueCount;

      if (mediaCount > 0 || queueCount > 0) {
        console.log(`[db-scanner] Account ${account.id}: ${mediaCount} scheduled + ${queueCount} queue intents`);
      }
    } catch (err) {
      console.error(`[db-scanner] Error scanning account ${account.id}:`, err.message);
    }
  }

  return { ok: true, totalEmitted };
}

module.exports = { runScan };
