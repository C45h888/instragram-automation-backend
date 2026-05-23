// backend.api/services/sync/engagement.js
// Proactive engagement sync: comments (every 6h) + conversations/messages (every 3 min).
//
// Two separate exported functions — each wired to its own cron in services/sync/index.js:
//
//   proactiveCommentSync()     → comment fetching for N most recent posts (every 6h)
//   proactiveEngagementSync()  → DM conversations + messages (every 3 min)
//
// Data flow:
//   Redis AcquisitionWorker → syncCommentsForAccount()
//     → fetchComments() × posts in parallel  → storeCommentBatches() × 1
//
//   Redis AcquisitionWorker → syncEngagementForAccount()
//     → fetchAndStoreConversations()          → instagram_dm_conversations
//     → fetchMessages() × convs in parallel  → storeMessageBatches() × 1

const {
  delay,
  generateRunId,
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

/**
 * Scoped comment sync for a single account — called by the Redis acquisition worker.
 *
 * @param {string} accountId - business account UUID
 * @param {object} [params] - optional { maxPosts, limit }
 * @returns {Promise<{success: boolean, count: number, error: string|null}>}
 */
async function syncCommentsForAccount(accountId, params = {}) {
  const maxPosts  = params.maxPosts || COMMENT_MAX_POSTS;
  const limit     = params.limit || 50;

  if (isAccountRateLimited(accountId)) {
    return { success: false, count: 0, error: 'rate_limited' };
  }

  let credentials;
  try {
    credentials = await resolveAccountCredentials(accountId);
  } catch (credErr) {
    console.warn(`[Sync:comments] Account ${accountId} credential resolution failed: ${credErr.message}`);
    await logSyncAudit('comments', accountId, {
      duration_ms: 0, items_fetched: 0, errors_count: 1,
      success: false, status: 'error', error_message: credErr.message,
    });
    return { success: false, count: 0, error: credErr.message };
  }

  try {
    const recentMedia = await getRecentMedia(accountId);
    const postsToCheck = recentMedia.slice(0, maxPosts);

    if (postsToCheck.length === 0) {
      return { success: true, count: 0, error: null };
    }

    let commentResults = await runConcurrent(
      postsToCheck,
      (media) => fetchComments(accountId, media.instagram_media_id, limit, credentials),
      3
    );

    for (const r of commentResults) {
      if (r._usagePct != null) updateQuotaUsage(accountId, r._usagePct);
    }

    const transientResults = commentResults.filter(r => {
      const { retryable } = handleFetchError(r, accountId);
      return retryable;
    });

    let finalCommentResults = commentResults;
    if (transientResults.length > 0) {
      const retryMs = transientResults[0].retry_after_seconds
        ? Math.min(transientResults[0].retry_after_seconds, 300) * 1000
        : 30000;
      console.warn(`[Sync:comments] Account ${accountId} ${transientResults.length} transient error(s), retrying batch in ${retryMs}ms`);
      await delay(retryMs);
      finalCommentResults = await runConcurrent(
        postsToCheck,
        (media) => fetchComments(accountId, media.instagram_media_id, limit, credentials),
        3
      );
      for (const r of finalCommentResults) {
        if (r._usagePct != null) updateQuotaUsage(accountId, r._usagePct);
      }
    }

    let commentAuthFailed = false;
    let totalComments = 0;
    for (const result of finalCommentResults) {
      if (result.success) totalComments += result.count;
      const { skip, break: brk } = handleFetchError(result, accountId);
      if (skip) { commentAuthFailed = true; break; }
      if (brk) break;
    }

    if (!commentAuthFailed) {
      const commentBatches = postsToCheck
        .map((media, i) => ({ mediaId: media.instagram_media_id, result: finalCommentResults[i] }))
        .filter(({ result }) => result.success && result.records?.length > 0)
        .map(({ mediaId, result }) => ({ mediaId, comments: result.records }));
      if (commentBatches.length > 0) {
        await storeCommentBatches(accountId, commentBatches);
      }
    }

    await logSyncAudit('comments', accountId, {
      duration_ms:    0,
      items_fetched:  totalComments,
      errors_count:   commentAuthFailed ? 1 : 0,
      success:        !commentAuthFailed,
      status:         commentAuthFailed ? 'error' : 'completed',
      posts_checked:  postsToCheck.length,
      total_comments: totalComments,
    });

    return {
      success: !commentAuthFailed,
      count: totalComments,
      error: commentAuthFailed ? 'comment_auth_failed' : null,
    };
  } catch (err) {
    console.error(`[Sync:comments] Account ${accountId} failed:`, err.message);
    await logSyncAudit('comments', accountId, {
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

/**
 * Scoped engagement (DM conversations + messages) sync for a single account.
 *
 * @param {string} accountId - business account UUID
 * @param {object} [params] - optional { convLimit, msgLimit, maxConvs }
 * @returns {Promise<{success: boolean, count: number, error: string|null}>}
 */
async function syncEngagementForAccount(accountId, params = {}) {
  const convLimit = params.convLimit || 20;
  const msgLimit  = params.msgLimit  || 20;
  const maxConvs  = params.maxConvs || ENGAGEMENT_MAX_CONVERSATIONS;

  if (isAccountRateLimited(accountId)) {
    return { success: false, count: 0, error: 'rate_limited' };
  }

  let credentials;
  try {
    credentials = await resolveAccountCredentials(accountId);
  } catch (credErr) {
    console.warn(`[Sync:engagement] Account ${accountId} credential resolution failed: ${credErr.message}`);
    await logSyncAudit('engagement', accountId, {
      duration_ms: 0, items_fetched: 0, errors_count: 1,
      success: false, status: 'error', error_message: credErr.message,
    });
    return { success: false, count: 0, error: credErr.message };
  }

  let totalCount = 0;

  try {
    // ── Conversations ──────────────────────────────────────────────────────
    const convResult = await fetchAndStoreConversations(accountId, convLimit);
    updateQuotaUsage(accountId, convResult._usagePct);
    const { skip: convSkip, break: convBrk, retryable: convRetryable, retryAfterMs: convRetryMs } = handleFetchError(convResult, accountId);

    let finalConvResult = convResult;
    if (convRetryable) {
      console.warn(`[Sync:engagement] Account ${accountId} conversations transient error, retrying in ${convRetryMs}ms: ${convResult.error}`);
      await delay(convRetryMs);
      finalConvResult = await fetchAndStoreConversations(accountId, convLimit);
      updateQuotaUsage(accountId, finalConvResult._usagePct);
    }

    await logSyncAudit('conversations', accountId, {
      duration_ms:   0,
      items_fetched: finalConvResult.count || 0,
      errors_count:  (convSkip || convBrk) ? 1 : 0,
      success:       finalConvResult.success && !convSkip && !convBrk,
      status:        (convSkip || convBrk) ? 'error' : 'completed',
      count:         finalConvResult.count,
    });

    if (convSkip || convBrk) {
      return { success: false, count: 0, error: finalConvResult.error || 'conv_fetch_failed' };
    }

    totalCount += finalConvResult.count || 0;

    // ── Messages for open-window conversations ─────────────────────────────
    if (finalConvResult.success && finalConvResult.conversations) {
      const openConvs = finalConvResult.conversations
        .filter(c => c.within_window || c.messaging_window?.is_open)
        .slice(0, maxConvs);

      if (openConvs.length > 0) {
        let msgFetchResults = await runConcurrent(
          openConvs,
          (conv) => fetchMessages(accountId, conv.id, msgLimit, credentials),
          3
        );

        for (const r of msgFetchResults) {
          if (r._usagePct != null) updateQuotaUsage(accountId, r._usagePct);
        }

        const msgTransientResults = msgFetchResults.filter(r => {
          const { retryable } = handleFetchError(r, accountId);
          return retryable;
        });

        if (msgTransientResults.length > 0) {
          const retryMs = msgTransientResults[0].retry_after_seconds
            ? Math.min(msgTransientResults[0].retry_after_seconds, 300) * 1000
            : 30000;
          console.warn(`[Sync:engagement] Account ${accountId} ${msgTransientResults.length} message(s) transient error(s), retrying batch in ${retryMs}ms`);
          await delay(retryMs);
          msgFetchResults = await runConcurrent(
            openConvs,
            (conv) => fetchMessages(accountId, conv.id, msgLimit, credentials),
            3
          );
          for (const r of msgFetchResults) {
            if (r._usagePct != null) updateQuotaUsage(accountId, r._usagePct);
          }
        }

        let msgAuthFailed = false;
        for (const result of msgFetchResults) {
          const { skip, break: brk } = handleFetchError(result, accountId);
          if (skip) { msgAuthFailed = true; break; }
          if (brk) break;
        }

        if (!msgAuthFailed) {
          const messageBatches = openConvs
            .map((conv, i) => ({ conversationId: conv.id, result: msgFetchResults[i] }))
            .filter(({ result }) => result.success && result.rawMessages?.length > 0)
            .map(({ conversationId, result }) => ({ conversationId, rawMessages: result.rawMessages }));

          if (messageBatches.length > 0) {
            await storeMessageBatches(accountId, messageBatches, credentials.igUserId, credentials.pageId, credentials);
          }
        }

        await logSyncAudit('messages', accountId, {
          duration_ms:           0,
          items_fetched:         openConvs.length,
          errors_count:          msgAuthFailed ? 1 : 0,
          success:               !msgAuthFailed,
          status:                msgAuthFailed ? 'error' : 'completed',
          conversations_checked: openConvs.length,
        });

        if (msgAuthFailed) {
          return { success: false, count: totalCount, error: 'message_auth_failed' };
        }
      }
    }

    return { success: true, count: totalCount, error: null };

  } catch (err) {
    console.error(`[Sync:engagement] Account ${accountId} failed:`, err.message);
    await logSyncAudit('engagement', accountId, {
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

module.exports = { syncCommentsForAccount, syncEngagementForAccount };
