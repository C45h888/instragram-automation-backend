// backend.api/services/sync/index.js
// Pure function module — no timers, no cron, no scheduling.
// All data acquisition is Redis-driven (workers/).
// Operational checks are called directly by the acquisition worker's periodic loop
// or invoked once at server startup.
//
// External callers:
//   server.js         → require('./services/sync').runStartupHealthChecks
//   workers/          → require('./services/sync').syncCommentsForAccount etc.
//   workers/          → require('./services/sync').proactiveHeartbeatFailover
//   post-fallback.js  → require('./sync/helpers').isAccountRateLimited / markAccountRateLimited

const crypto = require('crypto');
const { getSupabaseAdmin, logAudit } = require('../../config/supabase');
const { insertQueueRow } = require('../../helpers/agent-helpers');

const { syncCommentsForAccount, syncEngagementForAccount } = require('./engagement');
const { syncUgcForAccount }        = require('./ugc');
const { syncMediaForAccount }      = require('./media');
const { syncInsightsForAccount }   = require('./insights');
const { runTokenHealthCheck, runUATRefreshCheck } = require('./token-health');
const {
  isAccountRateLimited,
  markAccountRateLimited,
} = require('./helpers');

// ── Heartbeat Failover ───────────────────────────────────────────────────────

/**
 * Heartbeat failover detector — runs every 5 min.
 * If agent is silent > HEARTBEAT_STALE_MINUTES, marks it 'down' and
 * moves approved scheduled_posts into post_queue so post-fallback.js
 * can publish them.
 */
async function proactiveHeartbeatFailover(supabase, HEARTBEAT_STALE_MINUTES) {
  const staleThreshold = new Date(Date.now() - HEARTBEAT_STALE_MINUTES * 60 * 1000).toISOString();

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
      (Date.now() - new Date(agent.last_beat_at).getTime()) / (20 * 60 * 1000)
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

    await supabase
      .from('scheduled_posts')
      .update({ status: 'publishing' })
      .eq('id', post.id);

    console.log(`[Failover] Enqueued publish_post for scheduled_post ${post.id}`);
  }
}

// ── Startup health checks ────────────────────────────────────────────────────

/**
 * Runs token health and UA token refresh checks once at startup.
 * Called by server.js after DB init. Non-fatal — errors are logged, not thrown.
 */
async function runStartupHealthChecks() {
  console.log('[Health] Running startup token health checks...');
  try {
    await runTokenHealthCheck();
    await runUATRefreshCheck();
    console.log('[Health] Token health checks complete');
  } catch (err) {
    console.error('[Health] Token health check failed:', err.message);
  }
}

// ── Exports ──────────────────────────────────────────────────────────────────

module.exports = {
  // Startup (called by server.js)
  runStartupHealthChecks,

  // Heartbeat failover (called by acquisition worker periodic loop)
  proactiveHeartbeatFailover,

  // Scoped (single-account) sync functions — consumed by Redis AcquisitionWorker
  syncCommentsForAccount,
  syncEngagementForAccount,
  syncUgcForAccount,
  syncMediaForAccount,
  syncInsightsForAccount,

  // Token health (for manual invocation)
  runTokenHealthCheck,
  runUATRefreshCheck,

  // Circuit breaker — re-export for post-fallback.js compatibility
  isAccountRateLimited,
  markAccountRateLimited,
};
