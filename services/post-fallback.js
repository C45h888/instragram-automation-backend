// backend.api/services/post-fallback.js
// Persistent retry worker: scans post_queue for pending/failed outgoing IG API
// calls and retries them. Runs as a continuous loop (no cron).
//
// Toggle:   POST_FALLBACK_ENABLED=true (default: false — safe off in dev)
// Poll interval: POST_FALLBACK_INTERVAL_MS env var (default: 30000ms)
// Max retries before DLQ: MAX_RETRIES = 5
// Batch size per tick: BATCH_SIZE = 20

const { getSupabaseAdmin, logAudit } = require('../config/supabase');
const { resolveAccountCredentials } = require('../helpers/agent-helpers');
const { isAccountRateLimited } = require('../substrates/retry');
const { executeWithRetry } = require('../control-plane/execution-bridge');
const publishTransport = require('../substrates/transport/publishing');

const MAX_RETRIES = 5;
const BATCH_SIZE = 20;
const DEFAULT_POLL_INTERVAL_MS = 30000; // 30 seconds

// ============================================
// ACTION DISPATCHER
// ============================================

/**
 * Executes a single post_queue row against the Instagram Graph API.
 * Marks the row as 'processing' before the attempt to prevent concurrent pickup.
 * All retry logic is delegated to execution-bridge's executeWithRetry.
 * Updates status to 'sent' or 'dlq' based on outcome.
 * @param {object} supabase - Supabase admin client
 * @param {object} row - post_queue row from Supabase
 */
async function dispatchAction(supabase, row) {
  const { id, business_account_id, action_type, payload, retry_count } = row;

  // Mark processing to prevent concurrent cron pickup
  await supabase
    .from('post_queue')
    .update({ status: 'processing' })
    .eq('id', id);

  // Resolve Instagram credentials for this account
  const { igUserId, pageToken, pageId } = await resolveAccountCredentials(business_account_id);

  /**
   * Thin execution wrapper — execution-bridge calls this function, handles all retries,
   * backoff, error classification, and telemetry.
   */
  async function executePublish(accountId, params) {
    const { credentials, payload: pubPayload } = params;
    const result = await publishTransport.executeAction(
      action_type,
      accountId,
      credentials,
      pubPayload
    );
    return result;
  }

  const outcome = await executeWithRetry(
    business_account_id,
    `fallback-${id}`,            // intentId
    'publish',                   // domain
    executePublish,
    { credentials: { igUserId, pageToken, pageId }, payload },
    { maxRetries: MAX_RETRIES }  // total attempts = 5
  );

  // ── Map outcome to post_queue status ────────────────────────────────────

  if (outcome.status === 'completed') {
    const instagram_id = outcome.instagram_id || null;

    // Keep scheduled_posts in sync
    if (action_type === 'publish_post' && payload.scheduled_post_id && instagram_id) {
      await supabase
        .from('scheduled_posts')
        .update({
          status: 'published',
          instagram_media_id: instagram_id,
          published_at: new Date().toISOString()
        })
        .eq('id', payload.scheduled_post_id);
    }

    // Mark permission as reposted
    if (action_type === 'repost_ugc' && payload.permission_id && instagram_id) {
      await supabase
        .from('ugc_permissions')
        .update({
          status: 'reposted',
          instagram_media_id: instagram_id,
          reposted_at: new Date().toISOString()
        })
        .eq('id', payload.permission_id);
    }

    await supabase
      .from('post_queue')
      .update({ status: 'sent', instagram_id, error: null, error_category: null })
      .eq('id', id);

    logAudit({
      event_type: 'post_queue_sent',
      action: 'post_queue_dispatch',
      resource_type: 'post_queue',
      resource_id: id,
      details: { action_type, instagram_id, retry_count, business_account_id },
      success: true,
    }).catch(() => {});

    console.log(`[PostFallback] ✅ ${action_type} row ${id} sent (instagram_id: ${instagram_id})`);
    return;
  }

  // ── Failed ───────────────────────────────────────────────────────────────
  const errorMessage = outcome.error || 'unknown';
  const newRetryCount = retry_count + MAX_RETRIES; // execution-bridge already consumed retries

  if (outcome.error === 'rate_limited') {
    // Rate-limited — execution-bridge already called markAccountRateLimited.
    // Leave status as 'failed' so next poll picks it up after cooldown.
    console.warn(`[PostFallback] ⚠️ ${action_type} row ${id} rate-limited, will retry on next poll`);
    return;
  }

  // Permanent failure or max retries exceeded → DLQ
  await supabase
    .from('post_queue')
    .update({ status: 'dlq', retry_count: newRetryCount, error: errorMessage, error_category: null })
    .eq('id', id);

  logAudit({
    event_type: outcome.error === 'max_retries_exceeded' ? 'post_failed_max_retries' : 'post_failed_permanent',
    action: 'post_queue_dlq',
    resource_type: 'post_queue',
    resource_id: id,
    details: {
      action_type,
      error: errorMessage,
      retry_count: newRetryCount,
      business_account_id
    },
    success: false
  }).catch(() => {});

  console.error(
    `[PostFallback] 💀 ${action_type} row ${id} → DLQ after ${MAX_RETRIES} attempts: ${errorMessage}`
  );
}

// ============================================
// CRON RUNNER
// ============================================

/**
 * One tick of the fallback cron:
 * 1. Fetch up to BATCH_SIZE pending/failed rows where next_retry_at <= now or is NULL.
 * 2. Skip rows for rate-limited accounts.
 * 3. Dispatch each eligible row sequentially (avoids IG API concurrency hammering).
 */
async function runPostFallback() {
  const supabase = getSupabaseAdmin();
  if (!supabase) {
    console.warn('[PostFallback] Supabase not available, skipping run');
    return;
  }

  const now = new Date().toISOString();

  const { data: rows, error } = await supabase
    .from('post_queue')
    .select('*')
    .in('status', ['pending', 'failed'])
    .or(`next_retry_at.is.null,next_retry_at.lte.${now}`)
    .order('created_at', { ascending: true })
    .limit(BATCH_SIZE);

  if (error) {
    console.error('[PostFallback] Queue scan failed:', error.message);
    return;
  }

  if (!rows || rows.length === 0) return;

  console.log(`[PostFallback] Scanning ${rows.length} retryable row(s)`);

  for (const row of rows) {
    if (isAccountRateLimited(row.business_account_id)) {
      console.log(
        `[PostFallback] Account ${row.business_account_id} rate-limited, skipping row ${row.id}`
      );
      logAudit({
        event_type: 'post_queue_rate_limited_skip',
        action: 'post_queue_dispatch',
        resource_type: 'post_queue',
        resource_id: row.id,
        details: { action_type: row.action_type, business_account_id: row.business_account_id },
        success: false,
      }).catch(() => {});
      continue;
    }
    await dispatchAction(supabase, row);
  }
}

// ============================================
// LIFECYCLE — persistent while-loop (no cron)
// ============================================

let _running = false;
let _stopRequested = false;

/**
 * Sleep for ms milliseconds.
 */
function _sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Persistent retry loop — polls post_queue every POLL_INTERVAL_MS.
 * Runs until _stopRequested is true.
 */
async function _postFallbackLoop() {
  const pollInterval = parseInt(process.env.POST_FALLBACK_INTERVAL_MS || DEFAULT_POLL_INTERVAL_MS, 10);

  console.log(`[PostFallback] Worker started — polling every ${pollInterval}ms, max_retries: ${MAX_RETRIES}, batch: ${BATCH_SIZE}`);

  while (!_stopRequested) {
    try {
      await runPostFallback();
    } catch (err) {
      console.error('[PostFallback] Unhandled error in run:', err.message);
    }
    await _sleep(pollInterval);
  }

  console.log('[PostFallback] Worker stopped');
}

/**
 * Starts the post-fallback worker as a background task.
 * No-op if POST_FALLBACK_ENABLED is not 'true'.
 *
 * @returns {Function} stop function for graceful shutdown
 */
function startPostFallbackWorker() {
  if (process.env.POST_FALLBACK_ENABLED !== 'true') {
    console.log('[PostFallback] Disabled (POST_FALLBACK_ENABLED !== "true")');
    return () => {};
  }

  if (_running) {
    console.log('[PostFallback] Already running');
    return () => stopPostFallbackWorker();
  }

  _running = true;
  _stopRequested = false;

  // Fire-and-forget — loop runs in background
  _postFallbackLoop().catch(err =>
    console.error('[PostFallback] Loop crashed:', err.message)
  );

  return function stopPostFallbackWorker() {
    console.log('[PostFallback] Stopping...');
    _stopRequested = true;
    _running = false;
  };
}

module.exports = { startPostFallbackWorker };
