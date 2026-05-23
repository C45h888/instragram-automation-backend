// DEPRECATED — superseded by evaluator + realtime. Not started by lifecycle.
//
// workers/db-worker.js
// Database Scanner Worker: scans scheduled_posts and post_queue, emits Redis intents.
//
// Owns: DB polling, intent construction, Redis queue emission.
// Does NOT own: IG API calls, retry logic, persistence writes.
//
// Polls every DB_WORKER_INTERVAL_MS (default: 30s):
//   1. scheduled_posts WHERE status = 'approved' → emit publish intent
//   2. post_queue WHERE status IN ('pending','failed') AND next_retry_at <= now → emit intent
//      (only for rows that aren't already covered by scheduled_posts)
//
// Emits to domain-bucketed Redis queues:
//   publish:media     → supervisor:acquisitions:publish:media:{account_id}
//   publish:ugc       → supervisor:acquisitions:publish:ugc:{account_id}
//   publish:messaging → supervisor:acquisitions:publish:messaging:{account_id}

const { getRedisClient } = require('../config/redis');
const { getSupabaseAdmin } = require('../config/supabase');
const { getActiveAccounts } = require('../substrates/persistence');
const { buildIdempotencyKey } = require('../helpers/agent-helpers');

const DB_WORKER_INTERVAL_MS = parseInt(process.env.DB_WORKER_INTERVAL_MS || '30000', 10);
const RESULT_TTL_SEC = 3600;

// ── Action type → domain queue mapping ───────────────────────────────────────

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

function buildIntent(accountId, actionType, payload, queueRowId = null, scheduledPostId = null) {
  const intentId = require('crypto').randomUUID();
  return {
    intent_id: intentId,
    account_id: accountId,
    fetch_type: fetchTypeForAction(actionType),
    action_type: actionType,
    payload,
    priority: 'normal',
    issued_at: new Date().toISOString(),
    queue_row_id: queueRowId,
    scheduled_post_id: scheduledPostId,
  };
}

// ── Scan scheduled_posts ─────────────────────────────────────────────────────

/**
 * Scans for approved scheduled posts and emits publish intents.
 * @returns {Promise<number>} count of intents emitted
 */
async function scanScheduledPosts(supabase, redis, accountId) {
  const { data: posts, error } = await supabase
    .from('scheduled_posts')
    .select('id, business_account_id, asset_id')
    .eq('business_account_id', accountId)
    .eq('status', 'approved');

  if (error || !posts?.length) return 0;

  let emitted = 0;

  for (const post of posts) {
    // Fetch asset for media_url
    const { data: asset } = await supabase
      .from('instagram_assets')
      .select('storage_path, media_type, caption')
      .eq('id', post.asset_id)
      .single();

    if (!asset?.storage_path) {
      console.warn(`[db-worker] Scheduled post ${post.id} missing asset, marking failed`);
      await supabase
        .from('scheduled_posts')
        .update({ status: 'failed' })
        .eq('id', post.id);
      continue;
    }

    const intent = buildIntent(
      accountId,
      'publish_post',
      {
        image_url: asset.storage_path,
        caption: asset.caption || '',
        media_type: asset.media_type || 'IMAGE',
        scheduled_post_id: post.id,
      },
      null,
      post.id
    );

    const domain = 'media';
    const queueKey = `supervisor:acquisitions:publish:${domain}:${accountId}`;
    await redis.lpush(queueKey, JSON.stringify(intent));

    // Mark as publishing
    await supabase
      .from('scheduled_posts')
      .update({ status: 'publishing' })
      .eq('id', post.id)
      .eq('status', 'approved');

    console.log(`[db-worker] Emitted publish:media intent for scheduled_post ${post.id}`);
    emitted++;
  }

  return emitted;
}

// ── Scan post_queue ──────────────────────────────────────────────────────────

/**
 * Scans for pending/failed post_queue rows ready for retry.
 * Only processes rows that belong to the given account.
 * @returns {Promise<number>} count of intents emitted
 */
async function scanPostQueue(supabase, redis, accountId) {
  const now = new Date().toISOString();

  const { data: rows, error } = await supabase
    .from('post_queue')
    .select('*')
    .eq('business_account_id', accountId)
    .in('status', ['pending', 'failed'])
    .or(`next_retry_at.is.null,next_retry_at.lte.${now}`)
    .order('created_at', { ascending: true })
    .limit(50);

  if (error || !rows?.length) return 0;

  let emitted = 0;

  for (const row of rows) {
    const domain = domainForAction(row.action_type);
    const queueKey = `supervisor:acquisitions:publish:${domain}:${accountId}`;

    const intent = buildIntent(
      accountId,
      row.action_type,
      row.payload,
      row.id,
      row.payload?.scheduled_post_id || null
    );

    // Idempotency: check if there's already a pending intent for this queue row
    const existingResult = await redis.get(`supervisor:acquisition_results:publish:${domain}:${accountId}:intent:${row.id}`);
    // We use the queue_row_id in intent to let publish-worker detect duplicates

    await redis.lpush(queueKey, JSON.stringify(intent));

    // Mark as processing to prevent re-emission
    await supabase
      .from('post_queue')
      .update({ status: 'processing' })
      .eq('id', row.id)
      .eq('status', row.status);

    console.log(`[db-worker] Emitted publish:${domain} intent for post_queue row ${row.id} (action: ${row.action_type})`);
    emitted++;
  }

  return emitted;
}

// ── Main poll tick ───────────────────────────────────────────────────────────

async function _pollTick() {
  const redis = getRedisClient();
  if (!redis || redis.status !== 'ready') {
    console.warn('[db-worker] Redis not ready — skipping poll tick');
    return;
  }

  const supabase = getSupabaseAdmin();
  if (!supabase) return;

  const accounts = await getActiveAccounts();

  for (const account of accounts) {
    try {
      const mediaCount = await scanScheduledPosts(supabase, redis, account.id);
      const queueCount = await scanPostQueue(supabase, redis, account.id);

      if (mediaCount > 0 || queueCount > 0) {
        console.log(`[db-worker] Account ${account.id}: emitted ${mediaCount} scheduled + ${queueCount} queue intents`);
      }
    } catch (err) {
      console.error(`[db-worker] Error scanning account ${account.id}:`, err.message);
    }
  }
}

// ── Lifecycle ────────────────────────────────────────────────────────────────

let _running = false;
let _stopRequested = false;
let _pollTimer = null;

function _sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function _dbWorkerLoop() {
  console.log(`[db-worker] Started — polling every ${DB_WORKER_INTERVAL_MS}ms`);

  while (!_stopRequested) {
    try {
      await _pollTick();
    } catch (err) {
      console.error('[db-worker] Unhandled error in poll tick:', err.message);
    }
    await _sleep(DB_WORKER_INTERVAL_MS);
  }

  console.log('[db-worker] Stopped');
}

// db-worker is a singleton — one instance across all accounts
async function startWorker(signal) {
  if (_running) {
    console.log('[db-worker] Already running');
    return;
  }

  _running = true;
  _stopRequested = false;

  _dbWorkerLoop().catch(err =>
    console.error('[db-worker] Loop crashed:', err.message)
  );

  signal.addEventListener('abort', () => {
    console.log('[db-worker] Abort signal received');
    _stopRequested = true;
    _running = false;
  }, { once: true });
}

module.exports = { startWorker };
