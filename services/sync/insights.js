// backend.api/services/sync/insights.js
// Proactive media insights sync: reach/impressions/saves for last 7 days.
// Runs daily at 02:00 UTC (cron: 0 2 * * *) via services/sync/index.js.
//
// Data flow:
//   Redis AcquisitionWorker → syncInsightsForAccount()
//     → fetchAndStoreMediaInsights() → instagram_media (reach, impressions, saved metrics)

const {
  delay,
  isAccountRateLimited,
  handleFetchError,
  getActiveAccounts,
  logSyncAudit,
  updateQuotaUsage,
  getAdaptiveDelay,
} = require('./helpers');

const {
  fetchAndStoreMediaInsights,
} = require('../../helpers/data-fetchers/media-fetchers');

const INTER_ACCOUNT_DELAY_MS =
  parseInt(process.env.SYNC_INSIGHTS_DELAY_MS || '3000', 10);

/**
  });

  console.log(`[Sync:insights] Run ${runId} complete — ok:${successCount} err:${errorCount} skip:${skippedCount}`);
}

/**
 * Scoped insights sync for a single account — called by the Redis acquisition worker.
 * Same logic as the per-account body of proactiveInsightsSync().
 *
 * @param {string} accountId - business account UUID
 * @param {object} [params] - optional { since, until } unix timestamps
 * @returns {Promise<{success: boolean, count: number, error: string|null}>}
 */
async function syncInsightsForAccount(accountId, params = {}) {
  const sevenDaysAgo = params.since || Math.floor((Date.now() - 7 * 24 * 3600000) / 1000);
  const now          = params.until || Math.floor(Date.now() / 1000);

  if (isAccountRateLimited(accountId)) {
    return { success: false, count: 0, error: 'rate_limited' };
  }

  try {
    const result = await fetchAndStoreMediaInsights(accountId, sevenDaysAgo, now);
    updateQuotaUsage(accountId, result._usagePct);
    const { skip, break: brk, retryable, retryAfterMs } = handleFetchError(result, accountId);

    let finalResult = result;
    if (retryable) {
      console.warn(`[Sync:insights] Account ${accountId} transient error, retrying in ${retryAfterMs}ms: ${result.error}`);
      await delay(retryAfterMs);
      finalResult = await fetchAndStoreMediaInsights(accountId, sevenDaysAgo, now);
      updateQuotaUsage(accountId, finalResult._usagePct);
    }

    await logSyncAudit('media_insights', accountId, {
      duration_ms:   0,
      items_fetched: finalResult.count || 0,
      errors_count:  (skip || brk) ? 1 : 0,
      success:       finalResult.success && !skip && !brk,
      status:        (skip || brk) ? 'error' : 'completed',
      count:         finalResult.count,
      error_message: finalResult.success ? undefined : finalResult.error,
    });

    return {
      success: finalResult.success && !skip && !brk,
      count: finalResult.count || 0,
      error: (skip || brk) ? (finalResult.error || 'fetch_failed') : null,
    };
  } catch (err) {
    console.error(`[Sync:insights] Account ${accountId} failed:`, err.message);
    await logSyncAudit('insights', accountId, {
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

module.exports = { syncInsightsForAccount };
