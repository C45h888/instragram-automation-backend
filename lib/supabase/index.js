/**
 * Barrel — single re-export of all supabase library modules.
 * Imported by config/supabase.js to maintain 100% backward compatibility.
 * No new logic here — only re-exports.
 */

const {
  initializeSupabase,
  getSupabaseAdmin,
  getSupabaseClient,
  getConnectionInfo,
  checkHealth,
} = require('./_client');

const { logAudit, logApiRequest, shouldLog } = require('./_logging');

const { fireAndForgetInsert } = require('./_fire-forget');

const { supabaseHelpers } = require('./_helpers');

module.exports = {
  // Core initialisation and management
  initializeSupabase,
  getSupabaseAdmin,
  getSupabaseClient,
  getConnectionInfo,
  checkHealth,

  // Fire-and-forget query wrapper
  fireAndForgetInsert,

  // Logging functions
  logApiRequest,
  logAudit,
  shouldLog,

  // Helper functions
  supabaseHelpers,

  // Backward-compatibility aliases (dead code but kept for strict compat)
  supabaseAdmin: getSupabaseAdmin,
  supabaseClient: getSupabaseClient,
  supabaseAnon: getSupabaseClient,
};
