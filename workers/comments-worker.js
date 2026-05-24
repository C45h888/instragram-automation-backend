// workers/comments-worker.js
// Domain Worker: comments acquisition.
//
// Consumes: supervisor:acquisitions:comments:{account_id}
// Pipeline: resolveCredentials → getRecentMedia → runConcurrent(fetchComments)
//           → storeCommentBatches
// All wrapped in execution-bridge.executeWithRetry for retry + quota + telemetry.

const { getRedisClient } = require('../config/redis');
const { validateIntent } = require('../contracts/acquisition-intents');
const { executeWithRetry } = require('../control-plane/execution-bridge');
const transport = require('../substrates/transport/instagram');
const persistence = require('../substrates/persistence');
const { recordAcquisition } = require('../substrates/telemetry');
const { runConcurrent, delay } = require('../services/sync/helpers');

const QUEUE_PREFIX = 'supervisor:acquisitions:comments:';
const RESULT_PREFIX = 'supervisor:acquisition_results:';
const BRPOP_TIMEOUT_SEC = 30;
const RESULT_TTL_SEC = 3600;
const RECONNECT_DELAY_MS = 5000;
const COMMENT_MAX_POSTS = 5;

/**
 * Domain execution pipeline — called by execution-bridge.executeWithRetry.
 */
async function _execute(accountId, params = {}) {
  const maxPosts = params.maxPosts || COMMENT_MAX_POSTS;
  const limit = params.limit || 50;

  const credentials = await persistence.resolveAccountCredentials(accountId);
  const recentMedia = await persistence.getRecentMedia(accountId);
  const postsToCheck = recentMedia.slice(0, maxPosts);

  if (postsToCheck.length === 0) {
    return { success: true, count: 0 };
  }

  const results = await runConcurrent(
    postsToCheck,
    (media) => transport.fetchComments(accountId, media.instagram_media_id, limit, credentials),
    3
  );

  // Collect quota from all results
  let maxUsagePct = null;
  for (const r of results) {
    if (r._usagePct != null && (maxUsagePct === null || r._usagePct > maxUsagePct)) {
      maxUsagePct = r._usagePct;
    }
  }

  const successful = results.filter(r => r.success && r.records?.length > 0);
  if (successful.length > 0) {
    const batches = successful.map((r, i) => ({
      mediaId: postsToCheck[i].instagram_media_id,
      comments: r.records,
    }));
    await persistence.storeCommentBatches(accountId, batches);
  }

  const totalComments = successful.reduce((sum, r) => sum + r.records.length, 0);

  return {
    success: true,
    count: totalComments,
    _usagePct: maxUsagePct,
  };
}

// ── BRPOP Loop ───────────────────────────────────────────────────────────────

async function startWorker(accountId, signal) {
  const queueKey = `${QUEUE_PREFIX}${accountId}`;
  console.log(`[CommentsWorker] Starting for account ${accountId} on ${queueKey}`);

  while (!signal.aborted) {
    let redis;
    try {
      redis = getRedisClient();
      if (!redis || redis.status !== 'ready') {
        await _sleep(RECONNECT_DELAY_MS, signal);
        continue;
      }

      const result = await redis.brpop(queueKey, BRPOP_TIMEOUT_SEC);
      if (signal.aborted) break;
      if (!result) continue;

      const [, raw] = result;
      let intent;
      try {
        intent = JSON.parse(raw);
      } catch {
        continue;
      }

      const validation = validateIntent(intent);
      if (!validation.valid) {
        console.error(`[CommentsWorker] ${accountId}: invalid intent`, intent);
        continue;
      }

      const { intent_id } = intent;
      console.log(`[CommentsWorker] ${accountId}: intent ${intent_id}`);

      const outcome = await executeWithRetry(accountId, intent_id, 'comments', _execute, intent.parameters || {});

      // Write result to Redis
      const resultKey = `${RESULT_PREFIX}${accountId}:${intent_id}`;
      const payload = {
        intent_id, account_id: accountId, fetch_type: 'post_comments',
        status: outcome.status,
        result: { count: outcome.count },
        error: outcome.error,
        completed_at: new Date().toISOString(),
      };

      try {
        await redis.set(resultKey, JSON.stringify(payload), 'EX', RESULT_TTL_SEC);
      } catch (err) {
        console.error(`[CommentsWorker] ${accountId}: failed to write result:`, err.message);
      }

      console.log(`[CommentsWorker] ${accountId}: intent ${intent_id} ${outcome.status} (${outcome.count} items)`);
    } catch (err) {
      if (signal.aborted) break;
      console.error(`[CommentsWorker] ${accountId}: loop error:`, err.message);
      await _sleep(RECONNECT_DELAY_MS, signal);
    }
  }

  console.log(`[CommentsWorker] Stopped for account ${accountId}`);
}

async function _sleep(ms, signal) {
  return new Promise(resolve => {
    const timer = setTimeout(resolve, ms);
    const onAbort = () => { clearTimeout(timer); resolve(); };
    signal.addEventListener('abort', onAbort, { once: true });
  });
}

module.exports = { startWorker };
