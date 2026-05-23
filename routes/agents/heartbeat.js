// backend.api/routes/agents/heartbeat.js
// Receives heartbeat pings from the Python agent. Upserts agent_heartbeats row.
// Route: POST /agent/heartbeat
// Auth: X-API-Key (inherited from agent-proxy.js via validateAgentApiKey)

const express = require('express');
const router = express.Router();
const { getSupabaseAdmin, logAudit } = require('../../config/supabase');

// POST /agent/heartbeat
// Body: { agent_id: UUID, timestamp: ISO string }
router.post('/agent/heartbeat', async (req, res) => {
  const { agent_id, timestamp } = req.body;
  if (!agent_id) return res.status(400).json({ error: 'Missing agent_id' });

  const supabase = getSupabaseAdmin();
  if (!supabase) return res.status(503).json({ error: 'Database unavailable' });

  try {
    // Use upsert: UPDATE silently does nothing if no row exists for agent_id,
    // causing the agent to appear "down" to proactiveHeartbeatFailover even though
    // heartbeat pings are succeeding.
    const { error } = await supabase
      .from('agent_heartbeats')
      .upsert(
        {
          agent_id,
          last_beat_at: timestamp || new Date().toISOString(),
          status: 'alive',
        },
        { onConflict: 'agent_id' }
      );

    if (error) throw error;

    logAudit({
      event_type: 'agent_heartbeat_received',
      action: 'heartbeat',
      resource_type: 'agent',
      details: { agent_id, status: 'alive', last_beat_at: timestamp || new Date().toISOString() },
      success: true,
    }).catch(() => {});

    res.json({ success: true, agent_id, received_at: new Date().toISOString() });
  } catch (err) {
    console.error('[Heartbeat] upsert failed:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /agent/status
// Returns agent liveness computed server-side using LIVENESS_THRESHOLD_MS.
// Frontend routes through this (like all other agent data) instead of hitting Supabase directly.
router.get('/agent/status', async (req, res) => {
  const supabase = getSupabaseAdmin();
  if (!supabase) return res.status(503).json({ error: 'Database unavailable' });

  const LIVENESS_THRESHOLD_MS = 25 * 60 * 1000; // must exceed 20-min agent heartbeat interval

  try {
    const { data: heartbeats, error } = await supabase
      .from('agent_heartbeats')
      .select('agent_id, status, last_beat_at')
      .order('last_beat_at', { ascending: false })
      .limit(1);

    if (error) throw error;

    const newestBeat = heartbeats?.[0];
    const isAlive = newestBeat
      ? Date.now() - new Date(newestBeat.last_beat_at).getTime() <= LIVENESS_THRESHOLD_MS
      : false;

    logAudit({
      event_type: 'agent_status_check',
      action: 'read',
      resource_type: 'agent',
      details: { agent_id: newestBeat?.agent_id, status: isAlive ? 'alive' : 'down' },
      success: true,
    }).catch(() => {});

    res.json({
      success: true,
      data: {
        status: isAlive ? 'alive' : 'down',
        last_beat_at: newestBeat?.last_beat_at ?? null,
        agent_id: newestBeat?.agent_id ?? null,
      },
    });
  } catch (err) {
    console.error('[Heartbeat] /agent/status failed:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// GET /sync/health
// Returns latest run_completed row per domain + unresolved alert count.
router.get('/sync/health', async (_req, res) => {
  const supabase = getSupabaseAdmin();
  if (!supabase) return res.status(503).json({ error: 'Database unavailable' });

  const domains = ['engagement', 'ugc', 'media', 'insights', 'token_health', 'comments'];
  const results = {};

  for (const domain of domains) {
    const { data } = await supabase
      .from('sync_run_log')
      .select('status, completed_at, duration_ms, success_count, error_count, items_fetched, error_message')
      .eq('domain', domain)
      .eq('status', 'run_completed')
      .order('completed_at', { ascending: false })
      .limit(1)
      .maybeSingle();
    results[domain] = data || { status: 'never_run' };
  }

  const { count: alertCount } = await supabase
    .from('system_alerts')
    .select('*', { count: 'exact', head: true })
    .eq('resolved', false);

  res.json({ domains: results, unresolved_alerts: alertCount || 0 });
});

module.exports = router;
