// control-plane/runtime/operational-safety.js
// Operational Safety: bounded operational health checks.
//
// Owns: heartbeat failover (agent liveness check → move stuck posts).
// Does NOT own: evaluation, emission, worker lifecycle, signal intake.
//
// TODO: extract proactiveHeartbeatFailover to a dedicated governance safety-net
// module or the agent repo's supervisor. Heartbeat failover is governance, not
// operational — it decides to move posts to queue when agent is down.
//
// Contract:
//   safety.runChecks()  → run heartbeat failover

const { getSupabaseAdmin } = require('../../config/supabase');
const { proactiveHeartbeatFailover } = require('../../services/sync');

const HEARTBEAT_STALE_MINUTES = parseInt(process.env.HEARTBEAT_STALE_MINUTES || '30', 10);

/**
 * Run operational safety checks. Currently only heartbeat failover.
 */
async function runChecks() {
  const supabase = getSupabaseAdmin();
  if (!supabase) return;

  try {
    await proactiveHeartbeatFailover(supabase, HEARTBEAT_STALE_MINUTES);
  } catch (err) {
    console.error('[safety] Heartbeat failover error:', err.message);
  }
}

module.exports = { runChecks };
