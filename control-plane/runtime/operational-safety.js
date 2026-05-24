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
 * Returns live runtime state. Checks Supabase connection health.
 * @returns {{ supabase: 'connected'|'disconnected' }}
 */
function status() {
  const supabase = getSupabaseAdmin();
  return {
    supabase: supabase ? 'connected' : 'disconnected',
  };
}

/**
 * Run operational safety checks. Currently only heartbeat failover.
 * Errors are caught and logged — this function never throws.
 *
 * @returns {Promise<{ok: boolean, error?: string}>}
 *   ok=false when Supabase is unavailable or failover throws.
 */
async function runChecks() {
  const supabase = getSupabaseAdmin();
  if (!supabase) {
    return { ok: false, error: 'supabase unavailable' };
  }

  try {
    await proactiveHeartbeatFailover(supabase, HEARTBEAT_STALE_MINUTES);
    return { ok: true };
  } catch (err) {
    console.error('[safety] Heartbeat failover error:', err.message);
    return { ok: false, error: err.message };
  }
}

module.exports = { status, runChecks };
