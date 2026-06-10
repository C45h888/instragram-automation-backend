// retry-cadence-kernel/workers/cache-repair-worker.js
// Cache Repair Worker — bounded cache invalidation and rebuild.
//
// CONSTITUTIONAL CONTRACT:
//   Owns: invalidating stale cache entries, triggering cache rebuilds,
//         validating cache integrity after repair.
//
//   Does NOT own: cache TTL logic (reading workers own that),
//                 classification (persistence-failure-substrate),
//                 recommendation selection (FSM).
//
// Called by: maintenance-substrate.

/**
 * Invalidate and trigger a cache rebuild.
 *
 * The reading workers each have a clearCache(accountId) function.
 * This worker calls that, then signals that a rebuild is recommended.
 * The reading worker's natural cache-miss-on-next-read pattern will
 * repopulate the cache on the next governed read.
 *
 * @param {object} params — { domain, accountId, intentId, analysis }
 * @param {object} governance — CK reference
 * @returns {Promise<{ success: boolean, error?: string }>}
 */
async function execute(params, governance) {
  const { domain, accountId, intentId, analysis } = params;

  if (!governance || (typeof governance.dispatchGlobal !== 'function' && typeof governance.dispatch !== 'function')) {
    return { success: false, error: 'governance required for cache repair' };
  }

  // Signal the cache rebuild to the reading substrate.
  // The reading workers clear their per-worker caches on this signal.
  (governance.dispatchGlobal || governance.dispatch)({
    type: 'CACHE_INVALIDATED',
    domain: domain || 'persist-telemetry',
    accountId: accountId || '*',
    intentId,
    source: 'cache-repair-worker',
    reason: analysis?.category || 'storage_failure',
  });

  // The rebuild happens lazily: next read will cache-miss and re-fetch.
  // Immediate pre-warming would double the load on Supabase.
  return { success: true, error: null };
}

module.exports = { execute };
