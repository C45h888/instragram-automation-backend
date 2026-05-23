// backend/middleware/agent-auth.js - Agent API Key Authentication
// Secures agent proxy endpoints with X-API-Key header validation

const crypto = require('crypto');

/**
 * Validates X-API-Key header for agent proxy endpoints
 * Uses timing-safe comparison to prevent timing attacks
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 * @param {Function} next - Express next middleware function
 */
function validateAgentApiKey(req, res, next) {
  const apiKey = req.headers['x-api-key'];
  const expectedKey = process.env.AGENT_API_KEY;

  // Check if AGENT_API_KEY is configured
  if (!expectedKey) {
    console.error('❌ AGENT_API_KEY environment variable not configured');
    return res.status(500).json({
      error: 'Server configuration error',
      code: 'AGENT_API_KEY_NOT_CONFIGURED',
      message: 'Agent API authentication is not configured on the server'
    });
  }

  // Check if API key is provided
  if (!apiKey) {
    console.warn('⚠️ Agent request received without X-API-Key header');
    return res.status(401).json({
      error: 'Missing API key',
      code: 'MISSING_API_KEY',
      message: 'X-API-Key header is required for agent endpoints'
    });
  }

  // Timing-safe comparison to prevent timing attacks
  try {
    const apiKeyBuffer = Buffer.from(apiKey);
    const expectedKeyBuffer = Buffer.from(expectedKey);

    // Ensure buffers are same length before comparison
    if (apiKeyBuffer.length !== expectedKeyBuffer.length) {
      console.warn('⚠️ Invalid agent API key (length mismatch)');
      return res.status(401).json({
        error: 'Invalid API key',
        code: 'INVALID_API_KEY',
        message: 'The provided API key is invalid'
      });
    }

    if (!crypto.timingSafeEqual(apiKeyBuffer, expectedKeyBuffer)) {
      console.warn('⚠️ Invalid agent API key');
      return res.status(401).json({
        error: 'Invalid API key',
        code: 'INVALID_API_KEY',
        message: 'The provided API key is invalid'
      });
    }
  } catch (error) {
    console.error('❌ API key validation error:', error.message);
    return res.status(401).json({
      error: 'Invalid API key',
      code: 'INVALID_API_KEY',
      message: 'The provided API key is invalid'
    });
  }

  // API key is valid
  console.log('✅ Agent API key validated');
  next();
}

/**
 * Dual-auth middleware: accepts X-API-Key (agent path) OR Supabase JWT (frontend path).
 * JWT path verifies the requesting user owns the business_account_id in the request.
 */
async function validateAgentOrUserAuth(req, res, next) {
  // Path 1: Agent — X-API-Key header (timing-safe, same logic as validateAgentApiKey)
  const apiKey = req.headers['x-api-key'];
  if (apiKey) {
    const expectedKey = process.env.AGENT_API_KEY;
    if (!expectedKey) {
      return res.status(500).json({ error: 'Server misconfiguration', code: 'AGENT_API_KEY_NOT_CONFIGURED' });
    }
    try {
      const keyBuffer = Buffer.from(apiKey);
      const secretBuffer = Buffer.from(expectedKey);
      if (keyBuffer.length !== secretBuffer.length) {
        return res.status(401).json({ error: 'Invalid API key', code: 'INVALID_API_KEY' });
      }
      if (!crypto.timingSafeEqual(keyBuffer, secretBuffer)) {
        return res.status(401).json({ error: 'Invalid API key', code: 'INVALID_API_KEY' });
      }
    } catch {
      return res.status(401).json({ error: 'Invalid API key', code: 'INVALID_API_KEY' });
    }
    return next();
  }

  // Path 2: Frontend — Supabase JWT
  const authHeader = req.headers['authorization'];
  if (authHeader?.startsWith('Bearer ')) {
    const token = authHeader.slice(7);
    const { getSupabaseAdmin } = require('../config/supabase');
    const supabase = getSupabaseAdmin();
    if (!supabase) return res.status(503).json({ error: 'Database unavailable' });

    const { data: { user }, error } = await supabase.auth.getUser(token);
    if (error || !user) {
      return res.status(401).json({ error: 'Invalid or expired session', code: 'INVALID_SESSION' });
    }

    // Ownership check — business_account_id may be in body (POST) or query params (GET)
    const business_account_id = req.body?.business_account_id || req.query?.business_account_id;
    if (business_account_id) {
      const { data: account } = await supabase
        .from('instagram_business_accounts')
        .select('id')
        .eq('id', business_account_id)
        .eq('user_id', user.id)
        .maybeSingle();
      if (!account) {
        return res.status(403).json({ error: 'Access denied to this account', code: 'FORBIDDEN' });
      }
    }

    req.frontendUser = user;
    return next();
  }

  return res.status(401).json({ error: 'Missing authentication', code: 'MISSING_AUTH' });
}

module.exports = { validateAgentApiKey, validateAgentOrUserAuth };
