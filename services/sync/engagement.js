// backend.api/services/sync/engagement.js
// Proactive engagement sync: comments (every 6h) + conversations/messages (every 3 min).
//
// Two separate exported functions — each wired to its own cron in services/sync/index.js:
//
//   proactiveCommentSync()     → comment fetching for N most recent posts (every 6h)
//   proactiveEngagementSync()  → DM conversations + messages (every 3 min)
//
// Data flow:
//   node-cron (6h)  → proactiveCommentSync()
//     → fetchComments() × posts in parallel  → storeCommentBatches() × 1
//
//   node-cron (3m)  → proactiveEngagementSync()
//     → fetchAndStoreConversations()          → instagram_dm_conversations
//     → fetchMessages() × convs in parallel  → storeMessageBatches() × 1

const {
  delay,
  generateRunId,
  writeSyncRunLog,
  runConcurrent,
  isAccountRateLimited,
  handleFetchError,
  getActiveAccounts,
  getRecentMedia,
  logSyncAudit,
  updateQuotaUsage,
  getAdaptiveDelay,
} = require('./helpers');

const {
  fetchComments,
  fetchMessages,
  fetchAndStoreConversations,
} = require('../../helpers/data-fetchers/messaging-fetchers');

const {
  storeCommentBatches,
  storeMessageBatches,
  resolveAccountCredentials,
} = require('../../helpers/data-fetchers/base');

const COMMENT_MAX_POSTS          = 5;
const ENGAGEMENT_MAX_CONVERSATIONS = 5;
const INTER_ACCOUNT_DELAY_MS       =
  parseInt(process.env.SYNC_ENGAGEMENT_DELAY_MS || '3000', 10);

// ── Comment Sync ──────────────────────────────────────────────────────────────

/**
 * Proactive comment sync — runs every 6 hours.
 * Fetches comments for the N most recent posts regardless of age.
 * Decoupled from DM sync so heavy Meta API calls don't block real-time DM pipeline.
 */
async function proactiveCommentSync() {
  const runId    = generateRunId();
  const startTime = Date.now();
  const startMem = process.memoryUsage().heapUsed;
  const startCpu = process.cpuUsage();
  console.log(`[Sync:comments] Starting run ${runId}`);

  const accounts = await getActiveAccounts();

  await writeSyncRunLog({
    domain: 'comments', run_id: runId, status: 'run_started',
    total_accounts: accounts.length,
    cron_expr: process.env.PROACTIVE_COMMENTS_CRON || '0 */6 * * *',
    node_env: process.env.NODE_ENV,
    started_at: new Date().toISOString(),
  });

  if (accounts.length === 0) {
    console.log('[Sync:comments] No active accounts, skipping');
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
      console.log(`[Sync:comments] Account ${account.id} rate-limited, skipping`);
      skippedCount++;
      await logSyncAudit('comments', account.id, {
        run_id:        runId,
        duration_ms:   Date.now() - startTime,
        items_fetched: 0,
        errors_count:  0,
        skipped:       true,
        success:       false,
        status:        'skipped',
        error_message: 'rate_limited',
      });
      continue;
    }

    // Pre-resolve credentials once — prevents N parallel cache-miss races inside runConcurrent
    let credentials;
    try {
      credentials = await resolveAccountCredentials(account.id);
    } catch (credErr) {
      console.warn(`[Sync:comments] Account ${account.id} credential resolution failed: ${credErr.message}`);
      errorCount++;
      lastErrorMessage   = credErr.message;
      lastErrorAccountId = account.id;
      await logSyncAudit('comments', account.id, {
        run_id: runId, duration_ms: Date.now() - startTime,
        items_fetched: 0, errors_count: 1,
        success: false, status: 'error', error_message: credErr.message,
      });
      await delay(getAdaptiveDelay(account.id, INTER_ACCOUNT_DELAY_MS));
      continue;
    }

    try {
      const recentMedia = await getRecentMedia(account.id);
      const postsToCheck = recentMedia.slice(0, COMMENT_MAX_POSTS);
      let totalComments   = 0;

      // PARALLEL FETCH — up to 3 posts in parallel per batch
      const commentResults = await runConcurrent(
        postsToCheck,
        (media) => fetchComments(account.id, media.instagram_media_id, 50, credentials),
        3
      );

      // Propagate quota readings from parallel results
      for (const r of commentResults) {
        if (r._usagePct != null) updateQuotaUsage(account.id, r._usagePct);
      }

      // Check for transient errors — retry entire batch once if any item was transient
      const transientResults = commentResults.filter(r => {
        const { retryable } = handleFetchError(r, account.id);
        return retryable;
      });

      let finalCommentResults = commentResults;
      if (transientResults.length > 0) {
        const retryMs = transientResults[0].retry_after_seconds
          ? Math.min(transientResults[0].retry_after_seconds, 300) * 1000
          : 30000;
        console.warn(`[Sync:comments] Account ${account.id} ${transientResults.length} transient error(s), retrying batch in ${retryMs}ms`);
        await delay(retryMs);
        finalCommentResults = await runConcurrent(
          postsToCheck,
          (media) => fetchComments(account.id, media.instagram_media_id, 50, credentials),
          3
        );
        // Propagate quota from retry results too
        for (const r of finalCommentResults) {
          if (r._usagePct != null) updateQuotaUsage(account.id, r._usagePct);
        }
      }

      // Post-batch error accounting (results in same order as postsToCheck)
      let commentAuthFailed = false;
      for (const result of finalCommentResults) {
        if (result.success) totalComments += result.count;
        const { skip, break: brk } = handleFetchError(result, account.id);
        if (skip) { commentAuthFailed = true; break; }
        if (brk) break;
      }

      // BATCH WRITE — one DB call covers all posts
      if (!commentAuthFailed) {
        const commentBatches = postsToCheck
          .map((media, i) => ({ mediaId: media.instagram_media_id, result: commentResults[i] }))
          .filter(({ result }) => result.success && result.records?.length > 0)
          .map(({ mediaId, result }) => ({ mediaId, comments: result.records }));
        if (commentBatches.length > 0) {
          await storeCommentBatches(account.id, commentBatches);
        }
      }

      itemsFetched += totalComments;
      if (commentAuthFailed) { errorCount++; } else { successCount++; }

      await logSyncAudit('comments', account.id, {
        run_id:         runId,
        duration_ms:    Date.now() - startTime,
        items_fetched:  totalComments,
        errors_count:   commentAuthFailed ? 1 : 0,
        success:        !commentAuthFailed,
        status:         commentAuthFailed ? 'error' : 'completed',
        posts_checked:  postsToCheck.length,
        total_comments: totalComments,
      });

      await delay(getAdaptiveDelay(account.id, INTER_ACCOUNT_DELAY_MS));

    } catch (accountError) {
      console.error(`[Sync:comments] Account ${account.id} failed:`, accountError.message);
      errorCount++;
      lastErrorMessage   = accountError.message;
      lastErrorAccountId = account.id;
      await logSyncAudit('comments', account.id, {
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
    domain: 'comments', run_id: runId, status: 'run_completed',
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

  console.log(`[Sync:comments] Run ${runId} complete — ok:${successCount} err:${errorCount} skip:${skippedCount}`);
}

// ── DM Sync (Conversations + Messages) ───────────────────────────────────────

/**
 * Proactive DM engagement sync — runs every 3 minutes.
 * Handles conversations and messages only. Comment fetching moved to proactiveCommentSync.
 */
async function proactiveEngagementSync() {
  const runId    = generateRunId();
  const startTime = Date.now();
  const startMem = process.memoryUsage().heapUsed;
  const startCpu = process.cpuUsage();
  console.log(`[Sync:engagement] Starting run ${runId}`);

  const accounts = await getActiveAccounts();

  await writeSyncRunLog({
    domain: 'engagement', run_id: runId, status: 'run_started',
    total_accounts: accounts.length,
    cron_expr: process.env.PROACTIVE_DM_CRON || '*/3 * * * *',
    node_env: process.env.NODE_ENV,
    started_at: new Date().toISOString(),
  });

  if (accounts.length === 0) {
    console.log('[Sync:engagement] No active accounts, skipping');
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
      console.log(`[Sync:engagement] Account ${account.id} rate-limited, skipping`);
      skippedCount++;
      await logSyncAudit('engagement', account.id, {
        run_id:        runId,
        duration_ms:   Date.now() - startTime,
        items_fetched: 0,
        errors_count:  0,
        skipped:       true,
        success:       false,
        status:        'skipped',
        error_message: 'rate_limited',
      });
      continue;
    }

    // Pre-resolve credentials once — prevents N parallel cache-miss races inside runConcurrent
    let credentials;
    try {
      credentials = await resolveAccountCredentials(account.id);
    } catch (credErr) {
      console.warn(`[Sync:engagement] Account ${account.id} credential resolution failed: ${credErr.message}`);
      errorCount++;
      lastErrorMessage   = credErr.message;
      lastErrorAccountId = account.id;
      await logSyncAudit('engagement', account.id, {
        run_id: runId, duration_ms: Date.now() - startTime,
        items_fetched: 0, errors_count: 1,
        success: false, status: 'error', error_message: credErr.message,
      });
      await delay(getAdaptiveDelay(account.id, INTER_ACCOUNT_DELAY_MS));
      continue;
    }

    try {
      // ── Conversations ────────────────────────────────────────────────────
      const convResult = await fetchAndStoreConversations(account.id, 20);
      updateQuotaUsage(account.id, convResult._usagePct);
      const { skip: convSkip, break: convBrk, retryable: convRetryable, retryAfterMs: convRetryMs } = handleFetchError(convResult, account.id);

      // Retry once for transient errors on conversation fetch
      let finalConvResult = convResult;
      if (convRetryable) {
        console.warn(`[Sync:engagement] Account ${account.id} conversations transient error, retrying in ${convRetryMs}ms: ${convResult.error}`);
        await delay(convRetryMs);
        finalConvResult = await fetchAndStoreConversations(account.id, 20);
        updateQuotaUsage(account.id, finalConvResult._usagePct);
        const retryErr = handleFetchError(finalConvResult, account.id);
        if (retryErr.skip || retryErr.break) {
          console.error(`[Sync:engagement] Account ${account.id} conversations retry also failed: ${finalConvResult.error}`);
        }
      }

      await logSyncAudit('conversations', account.id, {
        run_id:        runId,
        duration_ms:   Date.now() - startTime,
        items_fetched: finalConvResult.count || 0,
        errors_count:  (convSkip || convBrk) ? 1 : 0,
        success:       finalConvResult.success && !convSkip,
        status:        (convSkip || convBrk) ? 'error' : 'completed',
        count:         finalConvResult.count,
      });

      if (convSkip || convBrk) {
        errorCount++;
        lastErrorMessage   = finalConvResult.error || 'conv_fetch_failed';
        lastErrorAccountId = account.id;
        await delay(getAdaptiveDelay(account.id, INTER_ACCOUNT_DELAY_MS));
        continue;
      }

      itemsFetched += finalConvResult.count || 0;

      // ── Messages for open-window conversations ───────────────────────────
      if (finalConvResult.success && finalConvResult.conversations) {
        const openConvs = convResult.conversations
          .filter(c => c.within_window || c.messaging_window?.is_open)
          .slice(0, ENGAGEMENT_MAX_CONVERSATIONS);

        let msgAuthFailed = false;

        // PARALLEL FETCH — up to 3 conversations in parallel per batch
        let msgFetchResults = await runConcurrent(
          openConvs,
          (conv) => fetchMessages(account.id, conv.id, 20, credentials),
          3
        );

        // Propagate quota readings from parallel results
        for (const r of msgFetchResults) {
          if (r._usagePct != null) updateQuotaUsage(account.id, r._usagePct);
        }

        // Check for transient errors — retry entire message batch once if any item was transient
        const msgTransientResults = msgFetchResults.filter(r => {
          const { retryable } = handleFetchError(r, account.id);
          return retryable;
        });

        if (msgTransientResults.length > 0) {
          const retryMs = msgTransientResults[0].retry_after_seconds
            ? Math.min(msgTransientResults[0].retry_after_seconds, 300) * 1000
            : 30000;
          console.warn(`[Sync:engagement] Account ${account.id} ${msgTransientResults.length} message(s) transient error(s), retrying batch in ${retryMs}ms`);
          await delay(retryMs);
          msgFetchResults = await runConcurrent(
            openConvs,
            (conv) => fetchMessages(account.id, conv.id, 20, credentials),
            3
          );
          for (const r of msgFetchResults) {
            if (r._usagePct != null) updateQuotaUsage(account.id, r._usagePct);
          }
        }

        // Post-batch error accounting
        for (const result of msgFetchResults) {
          const { skip, break: brk } = handleFetchError(result, account.id);
          if (skip) { msgAuthFailed = true; break; }
          if (brk) break;
        }

        // BATCH WRITE — one DB call covers all open conversations
        if (!msgAuthFailed) {
          const messageBatches = openConvs
            .map((conv, i) => ({ conversationId: conv.id, result: msgFetchResults[i] }))
            .filter(({ result }) => result.success && result.rawMessages?.length > 0)
            .map(({ conversationId, result }) => ({ conversationId, rawMessages: result.rawMessages }));

          if (messageBatches.length > 0) {
            // Pass credentials so storeMessageBatches can call ensureConversationRows
            // if a new conversation arrived after the conversations fetch completed
            await storeMessageBatches(account.id, messageBatches, credentials.igUserId, credentials.pageId, credentials);
          }
        }

        await logSyncAudit('messages', account.id, {
          run_id:                runId,
          duration_ms:           Date.now() - startTime,
          items_fetched:         openConvs.length,
          errors_count:          msgAuthFailed ? 1 : 0,
          success:               !msgAuthFailed,
          status:                msgAuthFailed ? 'error' : 'completed',
          conversations_checked: openConvs.length,
        });

        if (msgAuthFailed) { errorCount++; } else { successCount++; }
      } else {
        successCount++;
      }

      await delay(getAdaptiveDelay(account.id, INTER_ACCOUNT_DELAY_MS));

    } catch (accountError) {
      console.error(`[Sync:engagement] Account ${account.id} failed:`, accountError.message);
      errorCount++;
      lastErrorMessage   = accountError.message;
      lastErrorAccountId = account.id;
      await logSyncAudit('engagement', account.id, {
        run_id:           runId,
        duration_ms:      Date.now() - startTime,
        items_fetched:    0,
        errors_count:     1,
        success:          false,
        status:           'error',
        error_message:    accountError.message,
        skipped_accounts: 1,
      });
    }
  }

  await writeSyncRunLog({
    domain: 'engagement', run_id: runId, status: 'run_completed',
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

  console.log(`[Sync:engagement] Run ${runId} complete — ok:${successCount} err:${errorCount} skip:${skippedCount}`);
}

module.exports = { proactiveCommentSync, proactiveEngagementSync };
