// graph-capability-kernel/substrates/vault/scope-substrate/workers/detect-dynamic-worker.js
// Scope detect worker: one bounded /debug_token call + 7-day DB cache read/write.
// Migrated from substrates/vault/scope-substrate/workers/detect-dynamic-worker.js

const { axios, GRAPH_API_BASE } = require('../../api-surface');
const { PAT_SCOPE_DEFAULTS } = require('../../default-scopes');

class DetectDynamicWorker {
  /**
   * @param {{ token: string, supabase: object, credentialId?: string|null }} input
   * @returns {Promise<string[]>}
   */
  async execute({ token, supabase, credentialId = null }) {
    // 7-day cache check
    if (credentialId && supabase) {
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
        timeout: 5000,
      });

      const detectedScope = debugResponse.data.data?.scopes || [];
      console.log('✅ Detected scopes from Meta API:', detectedScope.join(', '));

      if (credentialId && supabase && detectedScope.length > 0) {
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
}

module.exports = DetectDynamicWorker;