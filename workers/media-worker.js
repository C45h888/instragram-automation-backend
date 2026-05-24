// workers/media-worker.js
// Domain Worker: media posts acquisition.
//
// Consumes: supervisor:acquisitions:media:{account_id}
// Pipeline: resolveCredentials → fetchBusinessPosts → normalize → storeBusinessPosts
// All wrapped in execution-bridge.executeWithRetry.

const { getRedisClient } = require('../config/redis');
const { validateIntent } = require('../contracts/acquisition-intents');
const { executeWithRetry } = require('../control-plane/execution-bridge');
const transport = require('../substrates/transport/instagram');
const persistence = require('../substrates/persistence');

const QUEUE_PREFIX = 'supervisor:acquisitions:media:';
const RESULT_PREFIX = 'supervisor:acquisition_results:';
const BRPOP_TIMEOUT_SEC = 30;
const RESULT_TTL_SEC = 3600;
const RECONNECT_DELAY_MS = 5000;

async function _execute(accountId, params = {}) {
  const limit = params.limit || 50;

  const result = await transport.fetchBusinessPosts(accountId, limit);
  if (!result.success) return result;

  if (result.posts.length > 0) {
    await persistence.storeBusinessPosts(accountId, result.posts);
    persistence.clearRecentMediaCache(accountId);
  }

  return {
    success: true,
    count: result.posts.length,
    _usagePct: result._usagePct,
  };
}

// ── BRPOP Loop ───────────────────────────────────────────────────────────────

async function startWorker(accountId, signal) {
  const queueKey = `${QUEUE_PREFIX}${accountId}`;
  console.log(`[MediaWorker] Starting for account ${accountId} on ${queueKey}`);

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
      try { intent = JSON.parse(raw); } catch { continue; }

      const validation = validateIntent(intent);
      if (!validation.valid) {
        console.error(`[MediaWorker] ${accountId}: invalid intent`, intent);
        continue;
      }

      const { intent_id } = intent;
      console.log(`[MediaWorker] ${accountId}: intent ${intent_id}`);

      const outcome = await executeWithRetry(accountId, intent_id, 'media', _execute, intent.parameters || {});

      const resultKey = `${RESULT_PREFIX}${accountId}:${intent_id}`;
      const payload = {
        intent_id, account_id: accountId, fetch_type: 'media_insights',
        status: outcome.status,
        result: { count: outcome.count },
        error: outcome.error,
        completed_at: new Date().toISOString(),
      };

      try {
        await redis.set(resultKey, JSON.stringify(payload), 'EX', RESULT_TTL_SEC);
      } catch (err) {
        console.error(`[MediaWorker] ${accountId}: failed to write result:`, err.message);
      }

      console.log(`[MediaWorker] ${accountId}: intent ${intent_id} ${outcome.status} (${outcome.count} items)`);
    } catch (err) {
      if (signal.aborted) break;
      console.error(`[MediaWorker] ${accountId}: loop error:`, err.message);
      await _sleep(RECONNECT_DELAY_MS, signal);
    }
  }

  console.log(`[MediaWorker] Stopped for account ${accountId}`);
}

async function _sleep(ms, signal) {
  return new Promise(resolve => {
    const timer = setTimeout(resolve, ms);
    const onAbort = () => { clearTimeout(timer); resolve(); };
    signal.addEventListener('abort', onAbort, { once: true });
  });
}

module.exports = { startWorker };
