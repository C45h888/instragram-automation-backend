// backend.api/services/sync/media.js
// Proactive media posts sync: business account's own media feed.
// Runs every 6 hours (cron: 0 */6 * * *) via services/sync/index.js.
//
// Data flow:
//   Redis AcquisitionWorker → syncMediaForAccount()
//     → fetchAndStoreBusinessPosts() → instagram_media (caption, media_url, permalink, published_at)

const {
  delay,
  isAccountRateLimited,
  handleFetchError,
  getActiveAccounts,
  logSyncAudit,
  updateQuotaUsage,
  getAdaptiveDelay,
  clearRecentMediaCache,
} = require('./helpers');

const {
  fetchAndStoreBusinessPosts,
} = require('../../helpers/data-fetchers/media-fetchers');

const INTER_ACCOUNT_DELAY_MS =
  parseInt(process.env.SYNC_MEDIA_DELAY_MS || '3000', 10);


/**
 * Scoped media sync for a single account — called by the Redis acquisition worker.
 *
 * @param {string} accountId - business account UUID
 * @param {object} [params] - optional { limit }
 * @returns {Promise<{success: boolean, count: number, error: string|null}>}
 */
async function syncMediaForAccount(accountId, params = {}) {
  const limit = params.limit || 50;

  if (isAccountRateLimited(accountId)) {
    return { success: false, count: 0, error: 'rate_limited' };
  }

  try {
    const result = await fetchAndStoreBusinessPosts(accountId, limit);
    updateQuotaUsage(accountId, result._usagePct);
    const { skip, break: brk, retryable, retryAfterMs } = handleFetchError(result, accountId);

    let finalResult = result;
    if (retryable) {
      console.warn(`[Sync:media] Account ${accountId} transient error, retrying in ${retryAfterMs}ms: ${result.error}`);
      await delay(retryAfterMs);
      finalResult = await fetchAndStoreBusinessPosts(accountId, limit);
      updateQuotaUsage(accountId, finalResult._usagePct);
    }

    await logSyncAudit('media_posts', accountId, {
      duration_ms:   0,
      items_fetched: finalResult.count || 0,
      errors_count:  (skip || brk) ? 1 : 0,
      success:       finalResult.success && !skip && !brk,
      status:        (skip || brk) ? 'error' : 'completed',
      count:         finalResult.count,
      error_message: finalResult.success ? undefined : finalResult.error,
    });

    if (finalResult.success && !skip && !brk) {
      clearRecentMediaCache(accountId);
    }

    return {
      success: finalResult.success && !skip && !brk,
      count: finalResult.count || 0,
      error: (skip || brk) ? (finalResult.error || 'fetch_failed') : null,
    };
  } catch (err) {
    console.error(`[Sync:media] Account ${accountId} failed:`, err.message);
    await logSyncAudit('media', accountId, {
      duration_ms:    0,
      items_fetched:  0,
      errors_count:   1,
      success:        false,
      status:         'error',
      error_message:  err.message,
    });
    return { success: false, count: 0, error: err.message };
  }
}

module.exports = { syncMediaForAccount };
