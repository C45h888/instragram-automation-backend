// workers/ugc-worker.js
// Domain Worker: UGC discovery acquisition.
//
// Consumes: supervisor:acquisitions:ugc:{account_id}
// Pipeline: resolveCredentials → fetchTaggedMedia → storeUgcContentBatch
//           → getMonitoredHashtags → runConcurrent(fetchHashtagMedia)
//           → normalize → storeUgcContentBatch
// All wrapped in governor.executeWithRetry.

const { getRedisClient } = require('../config/redis');
const { validateIntent } = require('../contracts/acquisition-intents');
const { executeWithRetry } = require('../control-plane/governor');
const transport = require('../substrates/transport/instagram');
const persistence = require('../substrates/persistence');
const { mapRawPostToUgcContent } = require('../substrates/normalization');
const { runConcurrent } = require('../services/sync/helpers');

const QUEUE_PREFIX = 'supervisor:acquisitions:ugc:';
const RESULT_PREFIX = 'supervisor:acquisition_results:';
const BRPOP_TIMEOUT_SEC = 30;
const RESULT_TTL_SEC = 3600;
const RECONNECT_DELAY_MS = 5000;
const UGC_MAX_HASHTAGS = 5;

async function _execute(accountId, params = {}) {
  const limit = params.limit || 50;

  const credentials = await persistence.resolveAccountCredentials(accountId);
  let totalCount = 0;

  // ── Tagged media ──────────────────────────────────────────────────────
  const tagResult = await transport.fetchTaggedMedia(accountId, limit, credentials);
  if (!tagResult.success) return tagResult;

  if (tagResult.records.length > 0) {
    const taggedRecords = tagResult.records
      .filter(p => p.id)
      .map(p => mapRawPostToUgcContent(p, accountId, 'tagged', null));
    await persistence.storeUgcContentBatch(taggedRecords);
    totalCount += taggedRecords.length;
  }

  // ── Hashtag media ────────────────────────────────────────────────────
  const hashtags = params.hashtags || await persistence.getMonitoredHashtags(accountId);
  const hashtagsToCheck = hashtags.slice(0, UGC_MAX_HASHTAGS);

  if (hashtagsToCheck.length > 0) {
    const hashResults = await runConcurrent(
      hashtagsToCheck,
      (hashtag) => transport.fetchHashtagMedia(accountId, hashtag, 25, credentials),
      3
    );

    let maxUsagePct = tagResult._usagePct;

    const allRecords = [];
    for (const r of hashResults) {
      if (r.success && r.records?.length > 0) {
        const shaped = r.records
          .filter(m => m.id)
          .map(m => mapRawPostToUgcContent(m, accountId, 'hashtag', r.cleanHashtag || null));
        allRecords.push(...shaped);
      }
      if (r._usagePct != null && (maxUsagePct === null || r._usagePct > maxUsagePct)) {
        maxUsagePct = r._usagePct;
      }
    }

    if (allRecords.length > 0) {
      await persistence.storeUgcContentBatch(allRecords);
      totalCount += allRecords.length;
    }

    return { success: true, count: totalCount, _usagePct: maxUsagePct };
  }

  return { success: true, count: totalCount, _usagePct: tagResult._usagePct };
}

// ── BRPOP Loop ───────────────────────────────────────────────────────────────

async function startWorker(accountId, signal) {
  const queueKey = `${QUEUE_PREFIX}${accountId}`;
  console.log(`[UgcWorker] Starting for account ${accountId} on ${queueKey}`);

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
        console.error(`[UgcWorker] ${accountId}: invalid intent`, intent);
        continue;
      }

      const { intent_id } = intent;
      console.log(`[UgcWorker] ${accountId}: intent ${intent_id}`);

      const outcome = await executeWithRetry(accountId, intent_id, 'ugc', _execute, intent.parameters || {});

      const resultKey = `${RESULT_PREFIX}${accountId}:${intent_id}`;
      const payload = {
        intent_id, account_id: accountId, fetch_type: 'ugc_discovery',
        status: outcome.status,
        result: { count: outcome.count },
        error: outcome.error,
        completed_at: new Date().toISOString(),
      };

      try {
        await redis.set(resultKey, JSON.stringify(payload), 'EX', RESULT_TTL_SEC);
      } catch (err) {
        console.error(`[UgcWorker] ${accountId}: failed to write result:`, err.message);
      }

      console.log(`[UgcWorker] ${accountId}: intent ${intent_id} ${outcome.status} (${outcome.count} items)`);
    } catch (err) {
      if (signal.aborted) break;
      console.error(`[UgcWorker] ${accountId}: loop error:`, err.message);
      await _sleep(RECONNECT_DELAY_MS, signal);
    }
  }

  console.log(`[UgcWorker] Stopped for account ${accountId}`);
}

async function _sleep(ms, signal) {
  return new Promise(resolve => {
    const timer = setTimeout(resolve, ms);
    const onAbort = () => { clearTimeout(timer); resolve(); };
    signal.addEventListener('abort', onAbort, { once: true });
  });
}

module.exports = { startWorker };
