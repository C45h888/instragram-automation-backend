// workers/messages-worker.js
// Domain Worker: DM messages acquisition.
//
// Consumes: supervisor:acquisitions:messages:{account_id}
// Pipeline: resolveCredentials → fetchConversations → storeConversationBatches
//           → filter open → runConcurrent(fetchMessages) → storeMessageBatches
// All wrapped in governor.executeWithRetry.

const { getRedisClient } = require('../config/redis');
const { validateIntent } = require('../contracts/acquisition-intents');
const { executeWithRetry } = require('../control-plane/governor');
const transport = require('../substrates/transport/instagram');
const persistence = require('../substrates/persistence');
const { runConcurrent } = require('../services/sync/helpers');

const QUEUE_PREFIX = 'supervisor:acquisitions:messages:';
const RESULT_PREFIX = 'supervisor:acquisition_results:';
const BRPOP_TIMEOUT_SEC = 30;
const RESULT_TTL_SEC = 3600;
const RECONNECT_DELAY_MS = 5000;
const MAX_CONVERSATIONS = 5;

async function _execute(accountId, params = {}) {
  const convLimit = params.convLimit || 20;
  const msgLimit  = params.msgLimit  || 20;
  const maxConvs  = params.maxConvs || MAX_CONVERSATIONS;

  const credentials = await persistence.resolveAccountCredentials(accountId);

  // ── Conversations ────────────────────────────────────────────────────
  const convResult = await transport.fetchConversations(accountId, convLimit, credentials);
  if (!convResult.success) return convResult;

  let convStoreResult;
  try {
    convStoreResult = await persistence.storeConversationBatches(
      accountId, convResult.rawConversations, convResult.igUserId, convResult.pageId
    );
  } catch (storeErr) {
    return { success: false, count: 0, error: storeErr.message, _usagePct: convResult._usagePct };
  }

  let totalCount = convStoreResult.count || 0;

  // ── Messages for open-window conversations ───────────────────────────
  if (convStoreResult.conversations?.length > 0) {
    const openConvs = convStoreResult.conversations
      .filter(c => c.within_window || c.messaging_window?.is_open)
      .slice(0, maxConvs);

    if (openConvs.length > 0) {
      const msgResults = await runConcurrent(
        openConvs,
        (conv) => transport.fetchMessages(accountId, conv.id, msgLimit, credentials),
        3
      );

      const successful = msgResults.filter(r => r.success && r.rawMessages?.length > 0);
      if (successful.length > 0) {
        const batches = successful.map((r, i) => ({
          conversationId: openConvs[i].id,
          rawMessages: r.rawMessages,
        }));
        await persistence.storeMessageBatches(
          accountId, batches, credentials.igUserId, credentials.pageId, credentials
        );
      }
    }
  }

  return {
    success: true,
    count: totalCount,
    _usagePct: convResult._usagePct,
  };
}

// ── BRPOP Loop ───────────────────────────────────────────────────────────────

async function startWorker(accountId, signal) {
  const queueKey = `${QUEUE_PREFIX}${accountId}`;
  console.log(`[MessagesWorker] Starting for account ${accountId} on ${queueKey}`);

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
        console.error(`[MessagesWorker] ${accountId}: invalid intent`, intent);
        continue;
      }

      const { intent_id } = intent;
      console.log(`[MessagesWorker] ${accountId}: intent ${intent_id}`);

      const outcome = await executeWithRetry(accountId, intent_id, 'messages', _execute, intent.parameters || {});

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
        console.error(`[MessagesWorker] ${accountId}: failed to write result:`, err.message);
      }

      console.log(`[MessagesWorker] ${accountId}: intent ${intent_id} ${outcome.status} (${outcome.count} items)`);
    } catch (err) {
      if (signal.aborted) break;
      console.error(`[MessagesWorker] ${accountId}: loop error:`, err.message);
      await _sleep(RECONNECT_DELAY_MS, signal);
    }
  }

  console.log(`[MessagesWorker] Stopped for account ${accountId}`);
}

async function _sleep(ms, signal) {
  return new Promise(resolve => {
    const timer = setTimeout(resolve, ms);
    const onAbort = () => { clearTimeout(timer); resolve(); };
    signal.addEventListener('abort', onAbort, { once: true });
  });
}

module.exports = { startWorker };
