// backend.api/services/sync/ugc.js
// Proactive UGC discovery sync: tagged posts + hashtag media.
// Runs every 3 hours (cron: 0 */3 * * *) via services/sync/index.js.
//
// Data flow:
//   Redis AcquisitionWorker → syncUgcForAccount()
//     → fetchAndStoreTaggedMedia()              → ugc_content (source: 'tagged')
//     → fetchHashtagMedia() × hashtags parallel → storeUgcContentBatch() × 1

const {
  delay,
  runConcurrent,
  isAccountRateLimited,
  handleFetchError,
  getActiveAccounts,
  getMonitoredHashtags,
  logSyncAudit,
  updateQuotaUsage,
  getAdaptiveDelay,
} = require('./helpers');

const {
  fetchHashtagMedia,
  fetchAndStoreTaggedMedia,
} = require('../../helpers/data-fetchers/ugc-fetchers');

const {
  storeUgcContentBatch,
  resolveAccountCredentials,
} = require('../../helpers/data-fetchers/base');

/**
 * Scoped UGC sync for a single account — called by the Redis acquisition worker.
 *
 * @param {string} accountId - business account UUID
 * @param {object} [params] - optional { limit, hashtags }
 * @returns {Promise<{success: boolean, count: number, error: string|null}>}
 */
async function syncUgcForAccount(accountId, params = {}) {
  const limit = params.limit || 50;

  if (isAccountRateLimited(accountId)) {
    return { success: false, count: 0, error: 'rate_limited' };
  }

  let credentials;
  try {
    credentials = await resolveAccountCredentials(accountId);
  } catch (credErr) {
    console.warn(`[Sync:ugc] Account ${accountId} credential resolution failed: ${credErr.message}`);
    await logSyncAudit('ugc', accountId, {
      duration_ms: 0, items_fetched: 0, errors_count: 1,
      success: false, status: 'error', error_message: credErr.message,
    });
    return { success: false, count: 0, error: credErr.message };
  }

  let totalCount = 0;

  try {
    // ── Tagged media ───────────────────────────────────────────────────────
    const tagResult = await fetchAndStoreTaggedMedia(accountId, limit);
    updateQuotaUsage(accountId, tagResult._usagePct);
    const { skip: tagSkip, break: tagBrk, retryable: tagRetryable, retryAfterMs: tagRetryMs } = handleFetchError(tagResult, accountId);

    let finalTagResult = tagResult;
    if (tagRetryable) {
      console.warn(`[Sync:ugc] Account ${accountId} tagged media transient error, retrying in ${tagRetryMs}ms: ${tagResult.error}`);
      await delay(tagRetryMs);
      finalTagResult = await fetchAndStoreTaggedMedia(accountId, limit);
      updateQuotaUsage(accountId, finalTagResult._usagePct);
    }

    await logSyncAudit('ugc_tagged', accountId, {
      duration_ms:   0,
      items_fetched: finalTagResult.count || 0,
      errors_count:  (tagSkip || tagBrk) ? 1 : 0,
      success:       finalTagResult.success && !tagSkip && !tagBrk,
      status:        (tagSkip || tagBrk) ? 'error' : 'completed',
      count:         finalTagResult.count,
    });

    if (tagSkip || tagBrk) {
      return { success: false, count: totalCount, error: finalTagResult.error || 'tag_fetch_failed' };
    }

    totalCount += finalTagResult.count || 0;

    // ── Hashtag media ──────────────────────────────────────────────────────
    const hashtags = params.hashtags || await getMonitoredHashtags(accountId);
    const hashtagsToCheck = hashtags.slice(0, UGC_MAX_HASHTAGS);

    if (hashtagsToCheck.length > 0) {
      let hashFetchResults = await runConcurrent(
        hashtagsToCheck,
        (hashtag) => fetchHashtagMedia(accountId, hashtag, 25, credentials),
        3
      );

      for (const r of hashFetchResults) {
        if (r._usagePct != null) updateQuotaUsage(accountId, r._usagePct);
      }

      const hashTransientResults = hashFetchResults.filter(r => {
        const { retryable } = handleFetchError(r, accountId);
        return retryable;
      });

      if (hashTransientResults.length > 0) {
        const retryMs = hashTransientResults[0].retry_after_seconds
          ? Math.min(hashTransientResults[0].retry_after_seconds, 300) * 1000
          : 30000;
        console.warn(`[Sync:ugc] Account ${accountId} ${hashTransientResults.length} hashtag(s) transient error(s), retrying batch in ${retryMs}ms`);
        await delay(retryMs);
        hashFetchResults = await runConcurrent(
          hashtagsToCheck,
          (hashtag) => fetchHashtagMedia(accountId, hashtag, 25, credentials),
          3
        );
        for (const r of hashFetchResults) {
          if (r._usagePct != null) updateQuotaUsage(accountId, r._usagePct);
        }
      }

      let hashAuthFailed = false;
      let hashCount = 0;
      for (const hashResult of hashFetchResults) {
        if (hashResult.success) hashCount += hashResult.count;
        const { skip, break: brk } = handleFetchError(hashResult, accountId);
        if (skip) { hashAuthFailed = true; break; }
        if (brk) break;
      }

      if (!hashAuthFailed) {
        const allRecords = hashFetchResults
          .filter(r => r.success)
          .flatMap(r => r.records);
        if (allRecords.length > 0) {
          await storeUgcContentBatch(allRecords);
        }
      }

      await logSyncAudit('ugc_hashtags', accountId, {
        duration_ms:      0,
        items_fetched:    hashCount,
        errors_count:     hashAuthFailed ? 1 : 0,
        success:          !hashAuthFailed,
        status:           hashAuthFailed ? 'error' : 'completed',
        hashtags_checked: hashtagsToCheck.length,
        total_media:      hashCount,
      });

      totalCount += hashCount;
      if (hashAuthFailed) {
        return { success: false, count: totalCount, error: 'hashtag_auth_failed' };
      }
    }

    return { success: true, count: totalCount, error: null };

  } catch (err) {
    console.error(`[Sync:ugc] Account ${accountId} failed:`, err.message);
    await logSyncAudit('ugc', accountId, {
      duration_ms:    0,
      items_fetched:  0,
      errors_count:   1,
      success:        false,
      status:         'error',
      error_message:  err.message,
    });
    return { success: false, count: totalCount, error: err.message };
  }
}

module.exports = { syncUgcForAccount };
