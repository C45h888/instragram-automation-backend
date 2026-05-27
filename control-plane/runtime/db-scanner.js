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
const { domainForAction } = require('../execution/domain-registry');

// ── Observability state tracking ────────────────────────────────────────────

let _scanState = 'IDLE';
let _lastScanAt = null;

// ── Intent builder ──────────────────────────────────────────────────────────

function buildIntent(accountId, actionType, payload, queueRowId, scheduledPostId) {
  const routingKey = domainForAction(actionType);  // e.g., 'publish:media'
  return {
    intent_id: require('crypto').randomUUID(),
    account_id: accountId,
    fetch_type: routingKey,
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
  if (!posts.length) return [];

  const emitted = [];

  for (const post of posts) {
    const asset = await dbWorker.resolveAsset(post.asset_id);

    if (!asset?.storage_path) {
      console.warn(`[db-scanner] Scheduled post ${post.id} missing asset, marking failed`);
      await dbWorker.markScheduledPostFailed(post.id);
      continue;
    }

    const routingKey = 'publish:media';
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

    const queueKey = `supervisor:acquisitions:${routingKey}:${accountId}`;
    await redis.lpush(queueKey, JSON.stringify(intent));

    emitted.push({ postId: post.id, accountId, routingKey, actionType: 'publish_post' });

    console.log(`[db-scanner] Emitted ${routingKey} intent for scheduled_post ${post.id}`);
  }

  return emitted;
}

// ── Scan post_queue ─────────────────────────────────────────────────────────

async function scanPostQueue(redis, accountId) {
  const rows = await dbWorker.getRetryablePostQueue(accountId);
  if (!rows.length) return [];

  const emitted = [];

  for (const row of rows) {
    const routingKey = domainForAction(row.action_type);  // e.g., 'publish:media'
    const queueKey = `supervisor:acquisitions:${routingKey}:${accountId}`;

    const intent = buildIntent(
      accountId, row.action_type, row.payload,
      row.id, row.payload?.scheduled_post_id || null
    );

    await redis.lpush(queueKey, JSON.stringify(intent));

    emitted.push({ rowId: row.id, accountId, routingKey, actionType: row.action_type, currentStatus: row.status });

    console.log(`[db-scanner] Emitted ${routingKey} intent for post_queue row ${row.id} (action: ${row.action_type})`);
  }

  return emitted;
}

// ── Main scan (called by orchestrator cadence) ──────────────────────────────

async function runScan(governance) {
  const previousState = _scanState;
  if (_scanState !== 'SCANNING') {
    _scanState = 'SCANNING';
    try {
      const observability = require('../observability/emitters/transition-emitter');
      observability.transition({
        domain: 'db-scanner',
        entity: 'db_scanner',
        entityId: 'system',
        previousState,
        nextState: 'SCANNING',
        authority: 'db-scanner',
        raw: { startedAt: Date.now() },
      });
    } catch (_) {}
  }

  const redis = getRedisClient();
  if (!redis || redis.status !== 'ready') {
    _scanState = 'IDLE';
    return { ok: false, error: 'redis unavailable', totalEmitted: 0 };
  }

  const accounts = await getActiveAccounts();
  let totalEmitted = 0;

  for (const account of accounts) {
    try {
      const scheduledEmitted = await scanScheduledPosts(redis, account.id);
      const queueEmitted = await scanPostQueue(redis, account.id);

      // Emit DB_SCAN_EMITTED events for each emitted intent (governance pathway)
      if (governance) {
        for (const item of scheduledEmitted) {
          governance.dispatch({
            type: 'DB_SCAN_EMITTED',
            target: 'scheduled_post',
            recordId: item.postId,
            accountId: item.accountId,
            actionType: item.actionType,
          });
        }
        for (const item of queueEmitted) {
          governance.dispatch({
            type: 'DB_SCAN_EMITTED',
            target: 'post_queue',
            recordId: item.rowId,
            accountId: item.accountId,
            actionType: item.actionType,
            currentStatus: item.currentStatus,
          });
        }
      }

      totalEmitted += scheduledEmitted.length + queueEmitted.length;

      if (scheduledEmitted.length > 0 || queueEmitted.length > 0) {
        console.log(`[db-scanner] Account ${account.id}: ${scheduledEmitted.length} scheduled + ${queueEmitted.length} queue intents`);
      }
    } catch (err) {
      console.error(`[db-scanner] Error scanning account ${account.id}:`, err.message);
    }
  }

  _scanState = 'IDLE';
  _lastScanAt = Date.now();
  try {
    const observability = require('../observability/emitters/transition-emitter');
    observability.transition({
      domain: 'db-scanner',
      entity: 'db_scanner',
      entityId: 'system',
      previousState: 'SCANNING',
      nextState: 'IDLE',
      authority: 'db-scanner',
      raw: { totalEmitted, accountsScanned: accounts.length, completedAt: Date.now() },
    });
  } catch (_) {}

  return { ok: true, totalEmitted };
}

module.exports = { runScan };
