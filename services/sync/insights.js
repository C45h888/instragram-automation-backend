// backend.api/services/sync/insights.js
// Proactive media insights sync: reach/impressions/saves for last 7 days.
// Runs daily at 02:00 UTC (cron: 0 2 * * *) via services/sync/index.js.
//
// Data flow:
//   node-cron → proactiveInsightsSync()
//     → fetchAndStoreMediaInsights() → instagram_media (reach, impressions, saved metrics)

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
} = require('./helpers');

const {
  fetchAndStoreMediaInsights,
} = require('../../helpers/data-fetchers/media-fetchers');

const INTER_ACCOUNT_DELAY_MS =
  parseInt(process.env.SYNC_INSIGHTS_DELAY_MS || '3000', 10);

/**
 * Proactive insights sync: media metrics for last 7 days.
 * For each active account: fetch and store media insights.
 */
async function proactiveInsightsSync() {
  const runId    = generateRunId();
  const startTime = Date.now();
  const startMem = process.memoryUsage().heapUsed;
  const startCpu = process.cpuUsage();
  console.log(`[Sync:insights] Starting run ${runId}`);

  const accounts = await getActiveAccounts();

  await writeSyncRunLog({
    domain: 'insights', run_id: runId, status: 'run_started',
    total_accounts: accounts.length,
    cron_expr: process.env.PROACTIVE_INSIGHTS_CRON || '0 2 * * *',
    node_env: process.env.NODE_ENV,
    started_at: new Date().toISOString(),
  });

  if (accounts.length === 0) {
    console.log('[Sync:insights] No active accounts, skipping');
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
      console.log(`[Sync:insights] Account ${account.id} rate-limited, skipping`);
      skippedCount++;
      await logSyncAudit('insights', account.id, {
        run_id:       runId,
        duration_ms:  Date.now() - startTime,
        items_fetched: 0,
        errors_count:  0,
        skipped:       true,
        success:       false,
        status:        'skipped',
        error_message: 'rate_limited',
      });
      continue;
    }

    try {
      const sevenDaysAgo = Math.floor((Date.now() - 7 * 24 * 3600000) / 1000);
      const now          = Math.floor(Date.now() / 1000);

      const result = await fetchAndStoreMediaInsights(account.id, sevenDaysAgo, now);
      updateQuotaUsage(account.id, result._usagePct);
      const { skip, break: brk, retryable, retryAfterMs } = handleFetchError(result, account.id);

      // Retry once for transient errors using server-suggested delay
      let finalResult = result;
      if (retryable) {
        console.warn(`[Sync:insights] Account ${account.id} transient error, retrying in ${retryAfterMs}ms: ${result.error}`);
        await delay(retryAfterMs);
        finalResult = await fetchAndStoreMediaInsights(account.id, sevenDaysAgo, now);
        updateQuotaUsage(account.id, finalResult._usagePct);
        const retryErr = handleFetchError(finalResult, account.id);
        if (retryErr.skip || retryErr.break) {
          // Retry also failed — treat as permanent error
          console.error(`[Sync:insights] Account ${account.id} retry also failed: ${finalResult.error}`);
        }
      }

      await logSyncAudit('media_insights', account.id, {
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
      }

      await delay(getAdaptiveDelay(account.id, INTER_ACCOUNT_DELAY_MS));

    } catch (accountError) {
      console.error(`[Sync:insights] Account ${account.id} failed:`, accountError.message);
      errorCount++;
      lastErrorMessage   = accountError.message;
      lastErrorAccountId = account.id;
      await logSyncAudit('insights', account.id, {
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
  }

  await writeSyncRunLog({
    domain: 'insights', run_id: runId, status: 'run_completed',
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

  console.log(`[Sync:insights] Run ${runId} complete — ok:${successCount} err:${errorCount} skip:${skippedCount}`);
}

module.exports = { proactiveInsightsSync };
