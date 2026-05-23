const express = require('express');
const router = express.Router();
const { getSupabaseAdmin, getConnectionInfo } = require('../config/supabase');

router.get('/', async (req, res) => {
  const health = {
    status: 'OK',
    timestamp: new Date().toISOString(),
    services: {
      api: 'running',
      database: 'unknown',
      tunnel_a: process.env.CLOUDFLARE_TUNNEL_TOKEN ? 'configured' : 'not configured',
      tunnel_b: process.env.CLOUDFLARE_SUPABASE_TUNNEL_TOKEN ? 'configured' : 'not configured'
    },
    environment: process.env.NODE_ENV,
    connection: getConnectionInfo()
  };

  try {
    const supabaseAdmin = getSupabaseAdmin();
    const { error } = await supabaseAdmin.from('user_profiles').select('count', { count: 'exact', head: true });
    
    if (!error) {
      health.services.database = 'connected';
      health.database_connection_type = getConnectionInfo()?.type || 'unknown';
    } else {
      health.services.database = 'error';
      health.database_error = error.message;
      health.status = 'DEGRADED';
    }
  } catch (error) {
    health.services.database = 'not initialized';
    health.status = 'DEGRADED';
  }

  res.status(health.status === 'OK' ? 200 : 503).json(health);
});

module.exports = router;