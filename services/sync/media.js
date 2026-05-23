// backend.api/services/sync/media.js
// Proactive media posts sync: business account's own media feed.
// Runs every 6 hours (cron: 0 */6 * * *) via services/sync/index.js.
//
// Data flow:
//   node-cron → proactiveMediaSync()
//     → fetchAndStoreBusinessPosts() → instagram_media (caption, media_url, permalink, published_at)

const {
  delay,
  generateRunId,
  writeSyncRunLog,
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
 * Proactive media posts sync: fetches the business account's own media feed
 * and writes full post data to instagram_media.
 * This populates the table read by GET /media/:accountId.
 */
async function proactiveMediaSync() {
  const runId    = generateRunId();
  const startTime = Date.now();
  const startMem = process.memoryUsage().heapUsed;
  const startCpu = process.cpuUsage();
  console.log(`[Sync:media] Starting run ${runId}`);

  const accounts = await getActiveAccounts();

  await writeSyncRunLog({
    domain: 'media', run_id: runId, status: 'run_started',
    total_accounts: accounts.length,
    cron_expr: process.env.PROACTIVE_MEDIA_CRON || '0 */6 * * *',
    node_env: process.env.NODE_ENV,
    started_at: new Date().toISOString(),
  });

  if (accounts.length === 0) {
    console.log('[Sync:media] No active accounts, skipping');
    return;
  }

  let successCount = 0;
  let errorCount   = 0;
  let skippedCount = 0;
  let itemsFetched = 0;
  let lastErrorMessage    = null;
  let lastErrorAccountId  = null;

  for (const account of accounts) {
    if (isAccountRateLimited(account.id)) {
      console.log(`[Sync:media] Account ${account.id} rate-limited, skipping`);
      skippedCount++;
      await logSyncAudit('media', account.id, {
        run_id:       runId,
        duration_ms:  Date.now() - startTime,
        items_fetched: 0,
        errors_count:  0,
        skipped:       true,
        success:       false,
        status:        'skipped',
        error_message: 'rate_limited',
      });
      // Note: delay is after the if/try block — falls through to bottom
      await delay(getAdaptiveDelay(account.id, INTER_ACCOUNT_DELAY_MS));
      continue;
    }

    try {
      const result = await fetchAndStoreBusinessPosts(account.id, 50);
      updateQuotaUsage(account.id, result._usagePct);
      const { skip, break: brk, retryable, retryAfterMs } = handleFetchError(result, account.id);

      // Retry once for transient errors using server-suggested delay
      let finalResult = result;
      if (retryable) {
        console.warn(`[Sync:media] Account ${account.id} transient error, retrying in ${retryAfterMs}ms: ${result.error}`);
        await delay(retryAfterMs);
        finalResult = await fetchAndStoreBusinessPosts(account.id, 50);
        updateQuotaUsage(account.id, finalResult._usagePct);
        const retryErr = handleFetchError(finalResult, account.id);
        if (retryErr.skip || retryErr.break) {
          console.error(`[Sync:media] Account ${account.id} retry also failed: ${finalResult.error}`);
        }
      }

      await logSyncAudit('media_posts', account.id, {
        run_id:        runId,
        duration_ms:   Date.now() - startTime,
        items_fetched: finalResult.count || 0,
        errors_count:  (skip || brk) ? 1 : 0,
        success:       finalResult.success && !skip,
        status:        (skip || brk) ? 'error' : 'completed',
        count:         finalResult.count,
        error_message: finalResult.success ? undefined : finalResult.error,
      });

      if (skip || brk) {
        errorCount++;
        lastErrorMessage   = finalResult.error || 'fetch_failed';
        lastErrorAccountId = account.id;
      } else {
        successCount++;
        itemsFetched += finalResult.count || 0;
        clearRecentMediaCache(account.id); // bust cache — comment sync picks up new posts immediately
      }

    } catch (accountError) {
      console.error(`[Sync:media] Account ${account.id} failed:`, accountError.message);
      errorCount++;
      lastErrorMessage   = accountError.message;
      lastErrorAccountId = account.id;
      await logSyncAudit('media', account.id, {
        run_id:          runId,
        duration_ms:     Date.now() - startTime,
        items_fetched:   0,
        errors_count:    1,
        success:         false,
        status:          'error',
        error_message:   accountError.message,
        skipped_accounts: 1,
      });
    }

    await delay(getAdaptiveDelay(account.id, INTER_ACCOUNT_DELAY_MS));
  }

  await writeSyncRunLog({
    domain: 'media', run_id: runId, status: 'run_completed',
    total_accounts: accounts.length,
    success_count: successCount, error_count: errorCount, skipped_count: skippedCount,
    items_fetched: itemsFetched,
    duration_ms: Date.now() - startTime,
    memory_delta_kb: Math.round((process.memoryUsage().heapUsed - startMem) / 1024),
    cpu_delta_ms: Math.round(process.cpuUsage(startCpu).user / 1000),
    error_message: lastErrorMessage,
    last_error_account: lastErrorAccountId,
    completed_at: new Date().toISOString(),
  });

  console.log(`[Sync:media] Run ${runId} complete — ok:${successCount} err:${errorCount} skip:${skippedCount}`);
}

module.exports = { proactiveMediaSync };
