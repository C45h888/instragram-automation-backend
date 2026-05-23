// backend.api/services/tokens/scope.js
// validateTokenScopes — checks granted permissions against instagram_credentials.
// logAudit is NOT here — import from config/supabase directly.

const { getSupabaseAdmin } = require('../../config/supabase');

/**
 * Validate that a token has all required scopes.
 * Prefers scope_cache (live-validated) over scope (set at store time).
 * @param {string} userId
 * @param {string} businessAccountId
 * @param {string[]} requiredScopes
 * @returns {Promise<{valid: boolean, missing: string[]}>}
 */
async function validateTokenScopes(userId, businessAccountId, requiredScopes = []) {
  try {
    const supabase = getSupabaseAdmin();

    const { data: credentials, error } = await supabase
      .from('instagram_credentials')
      .select('scope, scope_cache, scope_cache_updated_at')
      .eq('user_id', userId)
      .eq('business_account_id', businessAccountId)
      .eq('token_type', 'page')
      .eq('is_active', true)
      .single();

    if (error || !credentials) return { valid: false, missing: requiredScopes };

    const grantedScopes = credentials.scope_cache || credentials.scope || [];
    const missingScopes = requiredScopes.filter(req => !grantedScopes.includes(req));

    if (missingScopes.length === 0) return { valid: true, missing: [] };
    console.warn('⚠️  Missing scopes:', missingScopes);
    return { valid: false, missing: missingScopes };
  } catch (err) {
    console.error('❌ Scope validation error:', err);
    return { valid: false, missing: requiredScopes };
  }
}

module.exports = { validateTokenScopes };
