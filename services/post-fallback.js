// backend.api/services/post-fallback.js
// Fallback cron: scans post_queue for pending/failed outgoing IG API calls and retries them.
//
// Toggle:   POST_FALLBACK_ENABLED=true (default: false — safe off in dev)
// Schedule: POST_FALLBACK_CRON env var (default: */5 * * * *)
// Max retries before DLQ: MAX_RETRIES = 5
// Batch size per tick: BATCH_SIZE = 20

const cron = require('node-cron');
const axios = require('axios');
const { getSupabaseAdmin, logAudit } = require('../config/supabase');
const {
  resolveAccountCredentials,
  categorizeIgError,
  GRAPH_API_BASE,
  pollMediaContainerStatus,
} = require('../helpers/agent-helpers');
const {
  isAccountRateLimited,
  markAccountRateLimited,
} = require('./sync/helpers');

const DEFAULT_CRON = '*/5 * * * *';
const MAX_RETRIES = 5;
const BATCH_SIZE = 20;

// ============================================
// BACKOFF
// ============================================

/**
 * Exponential backoff in milliseconds: 2^n minutes, capped at 60 minutes.
 * retry_count=1 → 2 min, =2 → 4 min, =3 → 8 min, =4 → 16 min, =5 → 32 min
 * @param {number} retryCount - current retry_count value (after increment)
 * @returns {number} milliseconds to wait
 */
function backoffMs(retryCount) {
  return Math.min(Math.pow(2, retryCount) * 60 * 1000, 60 * 60 * 1000);
}

// ============================================
// ACTION DISPATCHER
// ============================================

/**
 * Executes a single post_queue row against the Instagram Graph API.
 * Marks the row as 'processing' before the attempt to prevent concurrent pickup.
 * Updates status to 'sent', 'failed', or 'dlq' based on outcome.
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

  try {
    const { igUserId, pageToken, pageId } = await resolveAccountCredentials(business_account_id);
    let instagram_id;

    switch (action_type) {

      // ------------------------------------------
      case 'reply_comment': {
        const res = await axios.post(
          `${GRAPH_API_BASE}/${payload.comment_id}/replies`,
          null,
          {
            params: { message: payload.reply_text.trim(), access_token: pageToken },
            timeout: 10000
          }
        );
        instagram_id = res.data.id;
        break;
      }

      // ------------------------------------------
      case 'reply_dm': {
        const res = await axios.post(
          `${GRAPH_API_BASE}/${payload.conversation_id}/messages`,
          null,
          {
            params: { message: payload.message_text.trim(), access_token: pageToken },
            timeout: 10000
          }
        );
        instagram_id = res.data.id;
        break;
      }

      // ------------------------------------------
      case 'send_dm': {
        // Meta requires Facebook Page ID node for Instagram DM send; fall back to igUserId
        const dmNode = pageId || igUserId;
        const res = await axios.post(
          `${GRAPH_API_BASE}/${dmNode}/messages`,
          {
            recipient: { id: String(payload.recipient_id) },
            message: { text: payload.message_text.trim() }
          },
          {
            params: { access_token: pageToken },
            timeout: 10000
          }
        );
        instagram_id = res.data.message_id || res.data.id;
        break;
      }

      // ------------------------------------------
      case 'publish_post': {
        let creationId = payload.creation_id;

        if (!creationId) {
          // Step 1: create media container
          const type = (payload.media_type || 'IMAGE').toUpperCase();
          const createParams = { caption: payload.caption, access_token: pageToken };
          if (type === 'VIDEO' || type === 'REELS') {
            createParams.video_url = payload.image_url;
            createParams.media_type = type;
          } else {
            createParams.image_url = payload.image_url;
          }

          const createRes = await axios.post(
            `${GRAPH_API_BASE}/${igUserId}/media`,
            null,
            { params: createParams, timeout: 15000 }
          );
          creationId = createRes.data.id;

          // Persist creation_id so next retry skips Step 1
          await supabase
            .from('post_queue')
            .update({ payload: { ...payload, creation_id: creationId } })
            .eq('id', id);
        }

        // For VIDEO/REELS: poll until FINISHED before publishing
        const publishType = (payload.media_type || 'IMAGE').toUpperCase();
        if (publishType === 'VIDEO' || publishType === 'REELS') {
          await pollMediaContainerStatus(creationId, pageToken);
        }

        // Step 2: publish
        const publishRes = await axios.post(
          `${GRAPH_API_BASE}/${igUserId}/media_publish`,
          null,
          {
            params: { creation_id: creationId, access_token: pageToken },
            timeout: 15000
          }
        );
        instagram_id = publishRes.data.id;

        // Keep scheduled_posts in sync
        if (payload.scheduled_post_id && instagram_id) {
          await supabase
            .from('scheduled_posts')
            .update({
              status: 'published',
              instagram_media_id: instagram_id,
              published_at: new Date().toISOString()
            })
            .eq('id', payload.scheduled_post_id);
        }
        break;
      }

      // ------------------------------------------
      case 'repost_ugc': {
        let creationId = payload.creation_id;

        if (!creationId) {
          // Re-fetch media URL via ugc_permissions → ugc_content (unified schema, Feb 2026)
          const { data: perm, error: permErr } = await supabase
            .from('ugc_permissions')
            .select('ugc_content_id')
            .eq('id', payload.permission_id)
            .single();

          if (permErr || !perm) {
            throw new Error('Permission record not found for repost_ugc retry');
          }

          const { data: ugc, error: ugcErr } = await supabase
            .from('ugc_content')
            .select('media_url, message, author_username, media_type')
            .eq('id', perm.ugc_content_id)
            .single();

          if (ugcErr || !ugc || !ugc.media_url) {
            throw new Error('UGC media not found for repost_ugc retry');
          }

          const caption = ugc.message
            ? `📸 @${ugc.author_username}: ${ugc.message}\n\n#repost`
            : `📸 @${ugc.author_username}\n\n#repost`;

          const ugcMediaType = (ugc.media_type || 'IMAGE').toUpperCase();
          const ugcCreateParams = { caption, access_token: pageToken };
          if (ugcMediaType === 'VIDEO' || ugcMediaType === 'REELS') {
            ugcCreateParams.video_url = ugc.media_url;
            ugcCreateParams.media_type = ugcMediaType;
          } else {
            ugcCreateParams.image_url = ugc.media_url;
          }

          const createRes = await axios.post(
            `${GRAPH_API_BASE}/${igUserId}/media`,
            null,
            { params: ugcCreateParams, timeout: 15000 }
          );
          creationId = createRes.data.id;

          // Persist creation_id + ugc_media_type so retry can poll and branch correctly
          await supabase
            .from('post_queue')
            .update({ payload: { ...payload, creation_id: creationId, ugc_media_type: ugcMediaType } })
            .eq('id', id);
        }

        // For VIDEO/REELS: poll until FINISHED before publishing.
        // ugc_media_type is persisted in payload so retries (where creation_id is already set) can check.
        const repostMediaType = payload.ugc_media_type || 'IMAGE';
        if (repostMediaType === 'VIDEO' || repostMediaType === 'REELS') {
          await pollMediaContainerStatus(creationId, pageToken);
        }

        // Step 2: publish
        const publishRes = await axios.post(
          `${GRAPH_API_BASE}/${igUserId}/media_publish`,
          null,
          {
            params: { creation_id: creationId, access_token: pageToken },
            timeout: 15000
          }
        );
        instagram_id = publishRes.data.id;

        // Mark permission as reposted
        await supabase
          .from('ugc_permissions')
          .update({
            status: 'reposted',
            instagram_media_id: instagram_id,
            reposted_at: new Date().toISOString()
          })
          .eq('id', payload.permission_id);
        break;
      }

      // ------------------------------------------
      default:
        throw new Error(`Unknown action_type: ${action_type}`);
    }

    // ── SUCCESS ──────────────────────────────────
    await supabase
      .from('post_queue')
      .update({ status: 'sent', instagram_id })
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

  } catch (error) {
    // ── FAILURE ──────────────────────────────────
    const errorMessage = error.response?.data?.error?.message || error.message;
    const { retryable, error_category, retry_after_seconds } = categorizeIgError(error);
    const newRetryCount = retry_count + 1;

    // Feed rate-limit signal into shared circuit breaker
    if (error_category === 'rate_limit') {
      markAccountRateLimited(business_account_id, retry_after_seconds);
    }

    if (!retryable || newRetryCount >= MAX_RETRIES) {
      // ── DEAD LETTER QUEUE ──────────────────────
      await supabase
        .from('post_queue')
        .update({ status: 'dlq', retry_count: newRetryCount, error: errorMessage, error_category })
        .eq('id', id);

      await logAudit({
        event_type: 'post_failed_permanent',
        action: 'post_queue_dlq',
        resource_type: 'post_queue',
        resource_id: id,
        details: {
          action_type,
          error: errorMessage,
          error_category,
          retry_count: newRetryCount,
          business_account_id
        },
        success: false
      }).catch(() => {});

      console.error(
        `[PostFallback] 💀 ${action_type} row ${id} → DLQ after ${newRetryCount} attempts: ${errorMessage}`
      );

    } else {
      // ── RETRYABLE — exponential backoff ────────
      const nextRetryAt = new Date(Date.now() + backoffMs(newRetryCount)).toISOString();

      await supabase
        .from('post_queue')
        .update({
          status: 'failed',
          retry_count: newRetryCount,
          error: errorMessage,
          error_category,
          next_retry_at: nextRetryAt
        })
        .eq('id', id);

      logAudit({
        event_type: 'post_queue_retry_scheduled',
        action: 'post_queue_dispatch',
        resource_type: 'post_queue',
        resource_id: id,
        details: { action_type, retry_count: newRetryCount, next_retry_at: nextRetryAt, error_category, business_account_id },
        success: false,
      }).catch(() => {});

      console.warn(
        `[PostFallback] ⚠️ ${action_type} row ${id} failed (${newRetryCount}/${MAX_RETRIES}), ` +
        `retry at ${nextRetryAt}: ${errorMessage}`
      );
    }
  }
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
// LIFECYCLE
// ============================================

let _cronJob = null;
let _isRunning = false;

/**
 * Initialises the post-fallback cron job.
 * Mirrors the initScheduledJobs() pattern from proactive-sync.js.
 * Returns a stop function for use in graceful shutdown.
 * @returns {function} stopPostFallbackJob
 */
function initPostFallbackJob() {
  if (process.env.POST_FALLBACK_ENABLED !== 'true') {
    console.log('[PostFallback] Disabled (POST_FALLBACK_ENABLED !== "true")');
    return () => {};
  }

  const cronExpr = process.env.POST_FALLBACK_CRON || DEFAULT_CRON;

  if (!cron.validate(cronExpr)) {
    console.error(`[PostFallback] Invalid cron expression: "${cronExpr}" — job not started`);
    return () => {};
  }

  console.log(
    `[PostFallback] Initialized — cron: ${cronExpr}, ` +
    `max_retries: ${MAX_RETRIES}, batch: ${BATCH_SIZE}`
  );

  _cronJob = cron.schedule(cronExpr, async () => {
    if (_isRunning) {
      console.log('[PostFallback] Previous run still active, skipping tick');
      return;
    }
    _isRunning = true;
    try {
      await runPostFallback();
    } catch (err) {
      console.error('[PostFallback] Unhandled error in run:', err.message);
    } finally {
      _isRunning = false;
    }
  }, { scheduled: true, timezone: 'UTC' });

  return function stopPostFallbackJob() {
    console.log('[PostFallback] Stopping...');
    if (_cronJob) {
      _cronJob.stop();
      _cronJob = null;
    }
  };
}

module.exports = { initPostFallbackJob };
