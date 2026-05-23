// backend.api/lib/supabase/_config.js
/**
 * Environment configuration — single source of truth for all env defaults.
 * Extracted from config/supabase.js SUPABASE_CONFIG + getConfig().
 */

const dotenv = require('dotenv');
dotenv.config({ path: '../.env' });

const SUPABASE_URL_FALLBACK = 'https://uromexjprcrjfmhkmgxa.supabase.co';

const CONFIG = {
  development: {
    url: null,           // filled by getEnv() from process.env
    timeout: 10000,
    retryAttempts: 3,
    retryDelay: 5000,
  },
  production: {
    url: null,
    timeout: 5000,
    retryAttempts: 5,
    retryDelay: 3000,
  },
};

/**
 * Returns the config object for a given environment.
 * @param {'development'|'production'} [env]
 */
function getConfig(env) {
  const environment = env || process.env.NODE_ENV || 'development';
  return CONFIG[environment] || CONFIG.development;
}

/**
 * Resolves env vars with fallbacks. Throws if required keys are absent.
 * @returns {{ url: string, serviceKey: string, anonKey: string|null, env: string }}
 */
function getEnv() {
  const env = process.env.NODE_ENV || 'development';
  const url = process.env.SUPABASE_URL || SUPABASE_URL_FALLBACK;
  const serviceKey = process.env.SUPABASE_SERVICE_KEY;
  if (!serviceKey) {
    throw new Error('SUPABASE_SERVICE_KEY is required but not provided');
  }
  const anonKey = process.env.SUPABASE_ANON_KEY || null;
  return { url, serviceKey, anonKey, env };
}

module.exports = { getConfig, getEnv };
