// backend.api/lib/supabase/_client.js
/**
 * Supabase client lifecycle — singleton management for admin + anon clients.
 * Extracted from config/supabase.js module state + initializeSupabase().
 */

const { createClient } = require('@supabase/supabase-js');
const { getConfig, getEnv } = require('./_config');

// ── Module state ───────────────────────────────────────────────────────────────

let _admin       = null;   // the live admin client
let _client     = null;   // the anon client
let _connInfo    = null;
let _initialised = false;

// ── Client factory ─────────────────────────────────────────────────────────────

function _buildAdminOpts() {
  return {
    auth: { autoRefreshToken: false, persistSession: false, detectSessionInUrl: false },
    db: { schema: 'public' },
    global: {
      headers: { 'X-Client-Info': 'instagram-automation-backend', 'X-Client-Version': '2.0.0' }
    },
    realtime: { params: { eventsPerSecond: 10 } },
  };
}

function _buildAnonOpts(anonKey) {
  if (!anonKey) return null;
  return {
    auth: { autoRefreshToken: true, persistSession: true, detectSessionInUrl: true },
    db: { schema: 'public' },
    global: {
      headers: { 'X-Client-Info': 'instagram-automation-client', 'X-Client-Version': '2.0.0' }
    },
  };
}

// ── Internal connection test ──────────────────────────────────────────────────

async function _testConnection(url, key, timeout) {
  const testClient = createClient(url, key, {
    auth: { autoRefreshToken: false, persistSession: false },
    db: { schema: 'public' },
    global: { headers: { 'X-Client-Info': 'instagram-backend-test' } }
  });
  const timeoutPromise = new Promise((_, reject) =>
    setTimeout(() => reject(new Error('Connection timeout')), timeout)
  );
  const testPromise = testClient.from('user_profiles').select('count', { count: 'exact', head: true });
  const result = await Promise.race([testPromise, timeoutPromise]);
  if (result.error) throw result.error;
  return { success: true, url };
}

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Initialise (or reinitialise) the Supabase clients.
 * Idempotent: if already initialised and healthy, returns existing clients.
 * @param {object} [opts]  — override env-driven config
 */
async function initializeSupabase(opts) {
  opts = opts || {};
  if (_initialised && _admin) {
    const health = await checkHealth();
    if (health.healthy) return { supabaseAdmin: _admin, supabaseClient: _client, connectionInfo: _connInfo };
    console.log('⚠️  Existing connection unhealthy, reinitializing...');
  }

  const { url, serviceKey, anonKey, env } = getEnv();
  const config     = getConfig(opts.env || env);
  const maxRetries  = opts.retryAttempts || config.retryAttempts;
  const retryDelay = opts.retryDelay    || config.retryDelay;
  const timeout    = opts.timeout        || config.timeout;

  console.log('🔄 Initializing Supabase connection...');
  console.log(`   URL: ${url}`);
  console.log(`   Environment: ${env}`);
  console.log(`   Max retries: ${maxRetries}`);
  console.log(`   Timeout: ${timeout}ms`);

  let lastError = null;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    console.log(`\n🔍 Connection attempt ${attempt}/${maxRetries}...`);
    try {
      await _testConnection(url, serviceKey, timeout);

      _admin  = createClient(url, serviceKey, _buildAdminOpts());
      _client = anonKey ? createClient(url, anonKey, _buildAnonOpts(anonKey)) : null;
      _connInfo = { url, environment: env, timestamp: new Date().toISOString(), attempt, totalAttempts: maxRetries };
      _initialised = true;

      console.log('✅ Supabase connection established successfully');
      console.log(`   Connected on attempt: ${attempt}`);
      console.log(`   Database: ${url}`);
      console.log('   Security: Row Level Security (RLS) active');

      const { count, error } = await _admin
        .from('user_profiles')
        .select('*', { count: 'exact', head: true });
      if (!error) console.log(`   Verified: ${count || 0} user profiles accessible`);

      return { supabaseAdmin: _admin, supabaseClient: _client, connectionInfo: _connInfo };

    } catch (error) {
      lastError = error;
      console.error(`❌ Attempt ${attempt} failed: ${error.message}`);
      if (attempt < maxRetries) {
        console.log(`⏳ Retrying in ${retryDelay / 1000} seconds...`);
        await new Promise(resolve => setTimeout(resolve, retryDelay));
      }
    }
  }

  const errorMsg = `Failed to connect to Supabase after ${maxRetries} attempts. Last error: ${lastError?.message}`;
  console.error('❌ ' + errorMsg);

  if (process.env.NODE_ENV !== 'production') {
    console.warn('⚠️  Starting without database connection (development mode)');
    console.warn('   Database-dependent features will not work');
    return { supabaseAdmin: null, supabaseClient: null, connectionInfo: null };
  }

  throw new Error(errorMsg);
}

function getSupabaseAdmin() {
  if (!_admin) {
    if (process.env.NODE_ENV === 'production') {
      throw new Error('Supabase admin client not initialized. Server should not be running without database.');
    }
    console.warn('⚠️  Supabase admin client not available');
    return null;
  }
  return _admin;
}

function getSupabaseClient() {
  if (!_client) {
    console.warn('⚠️  Supabase client not available');
    return null;
  }
  return _client;
}

function getConnectionInfo() {
  return _connInfo;
}

async function checkHealth() {
  try {
    const admin = getSupabaseAdmin();
    if (!admin) {
      return { healthy: false, error: 'Admin client not initialized', timestamp: new Date().toISOString() };
    }
    const startTime = Date.now();
    const { error } = await admin
      .from('user_profiles')
      .select('count', { count: 'exact', head: true });
    const responseTime = Date.now() - startTime;
    return { healthy: !error, responseTime, connectionInfo: _connInfo, error: error?.message, timestamp: new Date().toISOString() };
  } catch (error) {
    return { healthy: false, error: error.message, timestamp: new Date().toISOString() };
  }
}

module.exports = {
  initializeSupabase,
  getSupabaseAdmin,
  getSupabaseClient,
  getConnectionInfo,
  checkHealth,
};
