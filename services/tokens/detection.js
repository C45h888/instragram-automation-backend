// backend.api/services/tokens/detection.js
// Token introspection via Meta /debug_token.
// detectTokenType: validates any token and returns type/scopes/expiry metadata.
// fetchDynamicScope: fetches live scopes from Meta with 7-day DB caching.

const { axios, GRAPH_API_BASE, PAT_SCOPE_DEFAULTS } = require('./base');

/**
 * Detect token type and metadata via Meta's /debug_token endpoint.
 * @param {string} token - Any Meta access token (UAT or PAT)
 * @returns {Promise<{isValid, type, scopes, expiresAt, issuedAt, userId, appId, dataAccessExpiresAt}|null>}
 */
async function detectTokenType(token) {
  try {
    const response = await axios.get(`${GRAPH_API_BASE}/debug_token`, {
      params: { input_token: token, access_token: token },
      timeout: 5000
    });
    const data = response.data.data;
    return {
      isValid: data.is_valid,
      type: data.type,
      appId: data.app_id,
      scopes: data.scopes || [],
      expiresAt: data.expires_at,
      issuedAt: data.issued_at,
      userId: data.user_id,
      dataAccessExpiresAt: data.data_access_expires_at || null
    };
  } catch (err) {
    console.warn('⚠️ Token type detection failed:', err.message);
    return null;
  }
}

/**
 * Fetches live scope from Meta /debug_token with 7-day DB caching.
 * @param {string} token - Access token to inspect
 * @param {object} supabase - Supabase client
 * @param {string|null} credentialId - Credential row ID for caching
 * @returns {Promise<string[]>}
 */
async function fetchDynamicScope(token, supabase, credentialId = null) {
  if (credentialId) {
    const { data: cached } = await supabase
      .from('instagram_credentials')
      .select('scope_cache, scope_cache_updated_at')
      .eq('id', credentialId)
      .single();

    if (cached?.scope_cache && cached?.scope_cache_updated_at) {
      const cacheAge = Date.now() - new Date(cached.scope_cache_updated_at).getTime();
      if (cacheAge < 7 * 24 * 60 * 60 * 1000) {
        console.log('✅ Using cached scope (age: ' + Math.floor(cacheAge / 1000 / 60 / 60) + 'h)');
        return cached.scope_cache;
      }
    }
  }

  try {
    const debugResponse = await axios.get(`${GRAPH_API_BASE}/debug_token`, {
      params: { input_token: token, access_token: token },
      timeout: 5000
    });

    const detectedScope = debugResponse.data.data?.scopes || [];
    console.log('✅ Detected scopes from Meta API:', detectedScope.join(', '));

    if (credentialId && detectedScope.length > 0) {
      await supabase
        .from('instagram_credentials')
        .update({ scope_cache: detectedScope, scope_cache_updated_at: new Date().toISOString() })
        .eq('id', credentialId);
    }

    return detectedScope;
  } catch (debugError) {
    console.warn('⚠️  Scope detection failed, using PAT defaults');
    return PAT_SCOPE_DEFAULTS;
  }
}

module.exports = { detectTokenType, fetchDynamicScope };
