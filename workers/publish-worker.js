// workers/publish-worker.js
// Domain Worker: governed publishing execution.
//
// Consumes: supervisor:acquisitions:publish:{media|ugc|messaging}:{account_id}
// Pipeline: resolveCredentials → publish-transport → update post_queue / scheduled_posts
// All wrapped in execution-bridge.executeWithRetry for retry + quota + telemetry.

const { getRedisClient } = require('../config/redis');
const { getSupabaseAdmin } = require('../config/supabase');
const { validateIntent } = require('../contracts/acquisition-intents');
const { executeWithRetry } = require('../control-plane/execution-bridge');
const publishTransport = require('../substrates/transport/publishing');
const persistence = require('../substrates/persistence');
const { recordAcquisition } = require('../substrates/telemetry');

const QUEUE_PREFIXES = {
  media:     'supervisor:acquisitions:publish:media:',
  ugc:       'supervisor:acquisitions:publish:ugc:',
  messaging:  'supervisor:acquisitions:publish:messaging:',
};
const RESULT_PREFIX = 'supervisor:acquisition_results:publish:';
const BRPOP_TIMEOUT_SEC = 30;
const RESULT_TTL_SEC = 3600;
const RECONNECT_DELAY_MS = 5000;

// ── Domain-to-action-type mapping ────────────────────────────────────────────

const DOMAIN_TO_ACTION = {
  media:     'publish_post',
  ugc:       'repost_ugc',
  messaging: null, // multi-action: reply_comment, reply_dm, send_dm
};

function actionTypeForIntent(intent) {
  const at = intent.action_type;
  if (['publish_post', 'repost_ugc', 'reply_comment', 'reply_dm', 'send_dm'].includes(at)) {
    return at;
  }
  return at;
}

function domainForAction(actionType) {
  if (actionType === 'publish_post') return 'media';
  if (actionType === 'repost_ugc') return 'ugc';
  return 'messaging';
}

// ── Execution pipeline ────────────────────────────────────────────────────────

/**
 * Main _execute — called by execution-bridge.executeWithRetry.
 * Resolves credentials, executes the publish action via transport, updates post_queue/scheduled_posts.
 *
 * For scheduled_post intents (asset_id in payload): fetches asset from instagram_assets
 * to resolve image_url/caption/media_type before calling transport.
 */
async function _execute(accountId, params = {}) {
  const { action_type: actionType, payload, queue_row_id, scheduled_post_id, intent_type } = params;

  const credentials = await persistence.resolveAccountCredentials(accountId);
  const supabase = getSupabaseAdmin();

  // Mark queue row as processing
  if (queue_row_id && supabase) {
    await supabase
      .from('post_queue')
      .update({ status: 'processing' })
      .eq('id', queue_row_id)
      .eq('status', 'pending');
  }

  // ── Resolve asset for scheduled_post intents ───────────────────────────
  // The evaluator only has asset_id; we resolve to the actual media_url here.
  let resolvedPayload = payload;

  if (actionType === 'publish_post' && intent_type === 'scheduled_post' && payload?.asset_id) {
    const { data: asset } = await supabase
      .from('instagram_assets')
      .select('storage_path, media_type, caption')
      .eq('id', payload.asset_id)
      .single();

    if (!asset?.storage_path) {
      // Mark scheduled_posts as failed if asset is missing
      if (scheduled_post_id && supabase) {
        await supabase
          .from('scheduled_posts')
          .update({ status: 'failed' })
          .eq('id', scheduled_post_id);
      }
      return {
        success: false, count: 0, error: 'Asset not found for scheduled post',
        retryable: false, error_category: 'permanent',
      };
    }

    resolvedPayload = {
      image_url: asset.storage_path,
      caption: asset.caption || '',
      media_type: asset.media_type || 'IMAGE',
      scheduled_post_id,
    };
  }

  // Execute the action
  const result = await publishTransport.executeAction(
    actionType,
    accountId,
    credentials,
    resolvedPayload,
    supabase
  );

  if (!result.success) {
    // Return error result — execution-bridge will handle retry classification
    return {
      success: false,
      count: 0,
      error: result.error,
      retryable: result.retryable,
      error_category: result.error_category,
      retry_after_seconds: result.retry_after_seconds,
    };
  }

  // ── SUCCESS: update post_queue and scheduled_posts ───────────────────────

  if (queue_row_id && supabase) {
    await supabase
      .from('post_queue')
      .update({ status: 'sent', instagram_id: result.instagram_id })
      .eq('id', queue_row_id);
  }

  if (scheduled_post_id && supabase) {
    await supabase
      .from('scheduled_posts')
      .update({
        status: 'published',
        instagram_media_id: result.instagram_id,
        published_at: new Date().toISOString(),
      })
      .eq('id', scheduled_post_id);
  }

  // For UGC repost: update ugc_permissions
  if (actionType === 'repost_ugc' && payload?.permission_id && supabase) {
    await supabase
      .from('ugc_permissions')
      .update({
        status: 'reposted',
        instagram_media_id: result.instagram_id,
        reposted_at: new Date().toISOString(),
      })
      .eq('id', payload.permission_id);
  }

  return { success: true, count: 1, instagram_id: result.instagram_id };
}

// ── BRPOP worker (per domain) ───────────────────────────────────────────────

async function _runDomainWorker(domain, queuePrefix, accountId, signal) {
  const queueKey = `${queuePrefix}${accountId}`;
  console.log(`[PublishWorker:${domain}] Starting for account ${accountId} on ${queueKey}`);

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
        console.error(`[PublishWorker:${domain}] ${accountId}: invalid intent`, intent);
        continue;
      }

      const { intent_id, action_type, payload } = intent;
      console.log(`[PublishWorker:${domain}] ${accountId}: intent ${intent_id} action=${action_type}`);

      const params = {
        action_type: action_type || actionTypeForIntent(intent),
        payload: payload || {},
        queue_row_id: intent.queue_row_id || null,
        scheduled_post_id: intent.scheduled_post_id || null,
        intent_type: intent.intent_type || 'post_queue',
      };

      const outcome = await executeWithRetry(accountId, intent_id, `publish:${domain}`, _execute, params);

      // Write result to Redis
      const resultKey = `${RESULT_PREFIX}${domain}:${accountId}:${intent_id}`;
      const resultPayload = {
        intent_id,
        account_id: accountId,
        action_type: params.action_type,
        domain,
        status: outcome.status,
        instagram_id: outcome.instagram_id || null,
        error: outcome.error,
        completed_at: new Date().toISOString(),
      };

      try {
        await redis.set(resultKey, JSON.stringify(resultPayload), 'EX', RESULT_TTL_SEC);
      } catch (err) {
        console.error(`[PublishWorker:${domain}] ${accountId}: failed to write result:`, err.message);
      }

      console.log(`[PublishWorker:${domain}] ${accountId}: intent ${intent_id} ${outcome.status}` +
        (outcome.instagram_id ? ` (id: ${outcome.instagram_id})` : ''));
    } catch (err) {
      if (signal.aborted) break;
      console.error(`[PublishWorker:${domain}] ${accountId}: loop error:`, err.message);
      await _sleep(RECONNECT_DELAY_MS, signal);
    }
  }

  console.log(`[PublishWorker:${domain}] Stopped for account ${accountId}`);
}

// ── Sleep ───────────────────────────────────────────────────────────────────

function _sleep(ms, signal) {
  return new Promise(resolve => {
    const timer = setTimeout(resolve, ms);
    const onAbort = () => { clearTimeout(timer); resolve(); };
    signal.addEventListener('abort', onAbort, { once: true });
  });
}

// ── Public API — spawns all 3 domain sub-workers for an account ─────────────

async function startWorker(accountId, signal) {
  // Spawn all 3 domain workers in parallel
  const domains = ['media', 'ugc', 'messaging'];
  const tasks = domains.map(domain =>
    _runDomainWorker(domain, QUEUE_PREFIXES[domain], accountId, signal)
  );

  console.log(`[PublishWorker] Spawned for account ${accountId} (media + ugc + messaging)`);

  await Promise.all(tasks);
}

module.exports = { startWorker };
