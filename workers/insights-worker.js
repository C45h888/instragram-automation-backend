// workers/insights-worker.js
// Domain Worker: media insights acquisition.
//
// Consumes: supervisor:acquisitions:insights:{account_id}
// Pipeline: resolveCredentials → fetchMediaFeed → fetchMediaInsightsBatch
//           → normalize → storeMediaInsightsBatch
// All wrapped in governor.executeWithRetry.

const { getRedisClient } = require('../config/redis');
const { validateIntent } = require('../contracts/acquisition-intents');
const { executeWithRetry } = require('../control-plane/governor');
const transport = require('../substrates/transport/instagram');
const persistence = require('../substrates/persistence');

const QUEUE_PREFIX = 'supervisor:acquisitions:insights:';
const RESULT_PREFIX = 'supervisor:acquisition_results:';
const BRPOP_TIMEOUT_SEC = 30;
const RESULT_TTL_SEC = 3600;
const RECONNECT_DELAY_MS = 5000;

async function _execute(accountId, params = {}) {
  const sevenDaysAgo = params.since || Math.floor((Date.now() - 7 * 24 * 3600000) / 1000);
  const now          = params.until || Math.floor(Date.now() / 1000);

  const credentials = await persistence.resolveAccountCredentials(accountId);

  const feedResult = await transport.fetchMediaFeed(accountId, sevenDaysAgo, now, credentials);
  if (!feedResult.success) return feedResult;

  const mediaInsights = await transport.fetchMediaInsightsBatch(feedResult.mediaList, credentials.pageToken);

  if (mediaInsights.length > 0) {
    const captions = feedResult.mediaList.map(m => m.caption).filter(Boolean);
    await persistence.storeMediaInsightsBatch(accountId, mediaInsights, captions);
  }

  return {
    success: true,
    count: mediaInsights.length,
    _usagePct: feedResult._usagePct,
  };
}

// ── BRPOP Loop ───────────────────────────────────────────────────────────────

async function startWorker(accountId, signal) {
  const queueKey = `${QUEUE_PREFIX}${accountId}`;
  console.log(`[InsightsWorker] Starting for account ${accountId} on ${queueKey}`);

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
        console.error(`[InsightsWorker] ${accountId}: invalid intent`, intent);
        continue;
      }

      const { intent_id } = intent;
      console.log(`[InsightsWorker] ${accountId}: intent ${intent_id}`);

      const outcome = await executeWithRetry(accountId, intent_id, 'insights', _execute, intent.parameters || {});

      const resultKey = `${RESULT_PREFIX}${accountId}:${intent_id}`;
      const payload = {
        intent_id, account_id: accountId, fetch_type: 'account_insights',
        status: outcome.status,
        result: { count: outcome.count },
        error: outcome.error,
        completed_at: new Date().toISOString(),
      };

      try {
        await redis.set(resultKey, JSON.stringify(payload), 'EX', RESULT_TTL_SEC);
      } catch (err) {
        console.error(`[InsightsWorker] ${accountId}: failed to write result:`, err.message);
      }

      console.log(`[InsightsWorker] ${accountId}: intent ${intent_id} ${outcome.status} (${outcome.count} items)`);
    } catch (err) {
      if (signal.aborted) break;
      console.error(`[InsightsWorker] ${accountId}: loop error:`, err.message);
      await _sleep(RECONNECT_DELAY_MS, signal);
    }
  }

  console.log(`[InsightsWorker] Stopped for account ${accountId}`);
}

async function _sleep(ms, signal) {
  return new Promise(resolve => {
    const timer = setTimeout(resolve, ms);
    const onAbort = () => { clearTimeout(timer); resolve(); };
    signal.addEventListener('abort', onAbort, { once: true });
  });
}

module.exports = { startWorker };
