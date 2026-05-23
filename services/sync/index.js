// backend.api/services/sync/index.js
// Cron scheduler for all proactive sync domains.
// Public entry point for services/sync/ — re-exports all domain functions
// so callers can do: require('./services/sync').
//
// Cron schedule (default → override via env):
//   engagement:  */3 * * * *   (PROACTIVE_COMMENTS_CRON)
//   ugc:         0 */3 * * *   (PROACTIVE_UGC_CRON)
//   media:       0 */6 * * *   (PROACTIVE_MEDIA_CRON)
//   insights:    0 2 * * *     (PROACTIVE_INSIGHTS_CRON)
//   tokenHealth: 0 3 * * *     (TOKEN_HEALTH_CRON)
//   heartbeat:   */5 * * * *   (POST_FALLBACK_CRON)
//
// External callers:
//   server.js         → require('./services/sync').initScheduledJobs
//   post-fallback.js  → require('./sync/helpers').isAccountRateLimited / markAccountRateLimited

const cron = require('node-cron');
const crypto = require('crypto');                                  // promoted from lazy require
const { getSupabaseAdmin, logAudit } = require('../../config/supabase');
const { insertQueueRow } = require('../../helpers/agent-helpers'); // promoted from lazy require

const { proactiveEngagementSync, proactiveCommentSync } = require('./engagement');
const { proactiveUgcSync }        = require('./ugc');
const { proactiveMediaSync }      = require('./media');
const { proactiveInsightsSync }   = require('./insights');
const { runTokenHealthCheck, runUATRefreshCheck } = require('./token-health');
const {
  isAccountRateLimited,
  markAccountRateLimited,
  handleFetchError,
  getActiveAccounts,
  getRecentMedia,
  getMonitoredHashtags,
  logSyncAudit,
  checkStaleDomains,
} = require('./helpers');

// ── Cron Defaults ────────────────────────────────────────────────────────────

const DEFAULT_SCHEDULES = {
  comments:    '0 */6 * * *',  // comment sync every 6h (aligned with media sync)
  engagement:  '*/3 * * * *',  // DM conversations + messages every 3 min
  ugc:         '0 */3 * * *',
  media:       '0 */6 * * *',
  insights:    '0 2 * * *',
  tokenHealth: '0 3 * * *',
};

// ── Heartbeat Failover ───────────────────────────────────────────────────────

/**
 * Heartbeat failover detector — runs every 5 min.
 * If agent is silent > HEARTBEAT_STALE_MINUTES, marks it 'down' and
 * moves approved scheduled_posts into post_queue so post-fallback.js
 * can publish them.
 *
 * @param {object} supabase - Supabase admin client
 * @param {number} HEARTBEAT_STALE_MINUTES - stale threshold in minutes
 */
async function proactiveHeartbeatFailover(supabase, HEARTBEAT_STALE_MINUTES) {
  const staleThreshold = new Date(Date.now() - HEARTBEAT_STALE_MINUTES * 60 * 1000).toISOString();

  // Mark stale agents as down
  const { data: downAgents } = await supabase
    .from('agent_heartbeats')
    .update({ status: 'down' })
    .lt('last_beat_at', staleThreshold)
    .eq('status', 'alive')
    .select('agent_id, last_beat_at');

  if (!downAgents?.length) return;

  console.warn(`[Failover] Agent(s) down: ${downAgents.map(a => a.agent_id).join(', ')}`);

  for (const agent of downAgents) {
    const missCount = Math.floor(
      (Date.now() - new Date(agent.last_beat_at).getTime()) / (20 * 60 * 1000)  // 20-min heartbeat interval
    );
    if (missCount >= 3) {
      await logAudit({
        event_type:    'agent_down_alert',
        action:        'heartbeat_missed',
        resource_type: 'agent_heartbeats',
        resource_id:   agent.agent_id,
        details:       { missed_beats: missCount, last_beat_at: agent.last_beat_at },
        success:       false,
      }).catch(() => {});
      console.error(`[Failover] ⚠️ Agent ${agent.agent_id} missed ${missCount} heartbeats`);
    }
  }

  // Failover: enqueue approved scheduled_posts older than stale window
  const { data: stuck } = await supabase
    .from('scheduled_posts')
    .select('id, business_account_id, asset_id')
    .eq('status', 'approved')
    .lt('created_at', staleThreshold);

  for (const post of (stuck || [])) {
    const { data: asset } = await supabase
      .from('instagram_assets')
      .select('storage_path, media_type')
      .eq('id', post.asset_id)
      .single();

    if (!asset) continue;

    const idemKey = crypto.createHash('sha256')
      .update(`failover_publish:${post.id}`)
      .digest('hex');

    await insertQueueRow(supabase, {
      business_account_id: post.business_account_id,
      action_type:         'publish_post',
      payload: {
        image_url:          asset.storage_path,
        media_type:         asset.media_type || 'IMAGE',
        scheduled_post_id:  post.id,
      },
      idempotency_key: idemKey,
    });

    // Mark as publishing so we don't re-insert on the next tick
    await supabase
      .from('scheduled_posts')
      .update({ status: 'publishing' })
      .eq('id', post.id);

    console.log(`[Failover] Enqueued publish_post for scheduled_post ${post.id}`);
  }
}

// ── Scheduler ────────────────────────────────────────────────────────────────

let scheduledJobs = [];

/**
 * Initializes all cron jobs.
 * Call after DB init in server.js.
 * Returns a cleanup function for graceful shutdown.
 *
 * @returns {Function} stopScheduledJobs
 */
function initScheduledJobs() {
  // Per-domain overlap guards — one object prevents one stalled job blocking another
  const running = {
    comments:    false,
    engagement:  false,
    ugc:         false,
    media:       false,
    insights:    false,
    tokenHealth: false,
    heartbeat:   false,
  };

  // ── Token health check (always active, regardless of PROACTIVE_SYNC_ENABLED) ──
  const tokenHealthSchedule = process.env.TOKEN_HEALTH_CRON || DEFAULT_SCHEDULES.tokenHealth;
  if (!cron.validate(tokenHealthSchedule)) {
    console.error(`[TokenHealth] Invalid cron expression: "${tokenHealthSchedule}"`);
  }

  const tokenHealthJob = cron.validate(tokenHealthSchedule)
    ? cron.schedule(tokenHealthSchedule, async () => {
        if (running.tokenHealth) {
          console.log('[TokenHealthCheck] Previous run still active, skipping');
          return;
        }
        running.tokenHealth = true;
        try {
          await runTokenHealthCheck();
          await runUATRefreshCheck();
        } catch (err) {
          console.error('[TokenHealthCheck] Unhandled error:', err.message);
        } finally {
          running.tokenHealth = false;
        }
      }, { scheduled: true, timezone: 'UTC' })
    : null;

  if (tokenHealthJob) {
    console.log(`[TokenHealth] Token health check scheduled: ${tokenHealthSchedule}`);
  }

  if (process.env.PROACTIVE_SYNC_ENABLED !== 'true') {
    console.log('[ProactiveSync] Disabled — only token health check is active');
    return function stopScheduledJobs() {
      if (tokenHealthJob) tokenHealthJob.stop();
    };
  }

  const schedules = {
    comments:   process.env.PROACTIVE_COMMENTS_CRON  || DEFAULT_SCHEDULES.comments,
    engagement: process.env.PROACTIVE_DM_CRON        || DEFAULT_SCHEDULES.engagement,
    ugc:        process.env.PROACTIVE_UGC_CRON        || DEFAULT_SCHEDULES.ugc,
    media:      process.env.PROACTIVE_MEDIA_CRON      || DEFAULT_SCHEDULES.media,
    insights:   process.env.PROACTIVE_INSIGHTS_CRON   || DEFAULT_SCHEDULES.insights,
  };

  // Validate cron expressions
  for (const [name, expr] of Object.entries(schedules)) {
    if (!cron.validate(expr)) {
      console.error(`[ProactiveSync] Invalid cron expression for ${name}: "${expr}"`);
      return function stopScheduledJobs() {
        if (tokenHealthJob) tokenHealthJob.stop();
      };
    }
  }

  console.log('[ProactiveSync] Initializing scheduled jobs:');
  console.log(`   Comment sync (most recent posts): ${schedules.comments}`);
  console.log(`   Engagement (DM conversations+messages): ${schedules.engagement}`);
  console.log(`   UGC discovery: ${schedules.ugc}`);
  console.log(`   Media posts feed: ${schedules.media}`);
  console.log(`   Media insights: ${schedules.insights}`);

  const commentJob = cron.schedule(schedules.comments, async () => {
    if (running.comments) {
      console.log('[Sync:comments] Previous run still active, skipping');
      return;
    }
    running.comments = true;
    try {
      await proactiveCommentSync();
    } catch (err) {
      console.error('[Sync:comments] Unhandled error:', err.message);
    } finally {
      running.comments = false;
    }
  }, { scheduled: true, timezone: 'UTC' });

  const engagementJob = cron.schedule(schedules.engagement, async () => {
    if (running.engagement) {
      console.log('[Sync:engagement] Previous run still active, skipping');
      return;
    }
    running.engagement = true;
    try {
      await proactiveEngagementSync();
    } catch (err) {
      console.error('[Sync:engagement] Unhandled error:', err.message);
    } finally {
      running.engagement = false;
    }
  }, { scheduled: true, timezone: 'UTC' });

  const ugcJob = cron.schedule(schedules.ugc, async () => {
    if (running.ugc) {
      console.log('[Sync:ugc] Previous run still active, skipping');
      return;
    }
    running.ugc = true;
    try {
      await proactiveUgcSync();
    } catch (err) {
      console.error('[Sync:ugc] Unhandled error:', err.message);
    } finally {
      running.ugc = false;
    }
  }, { scheduled: true, timezone: 'UTC' });

  const mediaJob = cron.schedule(schedules.media, async () => {
    if (running.media) {
      console.log('[Sync:media] Previous run still active, skipping');
      return;
    }
    running.media = true;
    try {
      await proactiveMediaSync();
    } catch (err) {
      console.error('[Sync:media] Unhandled error:', err.message);
    } finally {
      running.media = false;
    }
  }, { scheduled: true, timezone: 'UTC' });

  const insightsJob = cron.schedule(schedules.insights, async () => {
    if (running.insights) {
      console.log('[Sync:insights] Previous run still active, skipping');
      return;
    }
    running.insights = true;
    try {
      await proactiveInsightsSync();
    } catch (err) {
      console.error('[Sync:insights] Unhandled error:', err.message);
    } finally {
      running.insights = false;
    }
  }, { scheduled: true, timezone: 'UTC' });

  // ── Heartbeat failover detector — every 5 min ────────────────────────────
  const HEARTBEAT_STALE_MINUTES = parseInt(process.env.HEARTBEAT_STALE_MINUTES || '30', 10);

  const heartbeatJob = cron.schedule(process.env.POST_FALLBACK_CRON || '*/5 * * * *', async () => {
    if (running.heartbeat) return;
    running.heartbeat = true;
    try {
      const supabase = getSupabaseAdmin();
      if (!supabase) return;
      await proactiveHeartbeatFailover(supabase, HEARTBEAT_STALE_MINUTES);
      await checkStaleDomains().catch((err) => {
        console.warn('[Failover] checkStaleDomains error:', err.message);
      });
    } catch (err) {
      console.error('[Failover] Heartbeat cron error:', err.message);
    } finally {
      running.heartbeat = false;
    }
  }, { scheduled: true, timezone: 'UTC' });

  scheduledJobs = [
    commentJob,
    engagementJob,
    ugcJob,
    mediaJob,
    insightsJob,
    heartbeatJob,
    ...(tokenHealthJob ? [tokenHealthJob] : []),
  ];
  console.log(`[ProactiveSync] ${scheduledJobs.length} jobs scheduled`);

  return function stopScheduledJobs() {
    console.log('[ProactiveSync] Stopping all scheduled jobs...');
    for (const job of scheduledJobs) {
      job.stop();
    }
    scheduledJobs = [];
    console.log('[ProactiveSync] All jobs stopped');
  };
}

// ── Exports ──────────────────────────────────────────────────────────────────

module.exports = {
  // Scheduler entry point (consumed by server.js)
  initScheduledJobs,

  // Domain sync functions (for manual testing / agent triggering)
  proactiveCommentSync,
  proactiveEngagementSync,
  proactiveUgcSync,
  proactiveMediaSync,
  proactiveInsightsSync,

  // Token health (for manual testing)
  runTokenHealthCheck,
  runUATRefreshCheck,

  // DB helpers (for unit testing)
  getActiveAccounts,
  getRecentMedia,
  getMonitoredHashtags,

  // Circuit breaker — EXPLICIT re-export for post-fallback.js compatibility
  isAccountRateLimited,
  markAccountRateLimited,
};
