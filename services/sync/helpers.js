// backend.api/services/sync/helpers.js
// Pure infrastructure utilities for proactive sync.
//
// Jurisdiction: this module owns NO runtime state. All state lives in
// bounded substrates (retry, quota, persistence). This file provides:
//   - runConcurrent: parallel batch runner
//   - delay, generateRunId: basic infrastructure
//   - logSyncAudit: structured audit log writer
//
// Circuit breaker re-exports exist here only for post-fallback.js backward
// compatibility — the canonical home is substrates/retry.js.

const { randomUUID } = require('crypto');
const { logAudit } = require('../../config/supabase');

// ── Re-export circuit breaker from retry substrate ───────────────────────────
// DEPRECATED: import directly from substrates/retry.js in new code.
// These re-exports exist solely for post-fallback.js backward compatibility.
const {
  isAccountRateLimited,
  markAccountRateLimited,
} = require('../../substrates/retry');

// ── Infrastructure ──────────────────────────────────────────────────────────

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function generateRunId() {
  return randomUUID();
}

// ── Enhanced Audit Logging ───────────────────────────────────────────────────

/**
 * Writes a structured sync run entry to audit_log.
 *
 * Standard details shape (callers supply all fields):
 * {
 *   run_id:            number   — Date.now() captured at function entry
 *   status:            string   — 'started' | 'completed' | 'error'
 *   duration_ms:       number   — Date.now() - startTime (omit on 'started')
 *   items_fetched:     number
 *   errors_count:      number
 *   skipped:           boolean
 *   success:           boolean
 *   // domain-specific optional fields:
 *   posts_checked, total_comments, conversations_checked,
 *   hashtags_checked, total_media, count, error_message, skipped_accounts
 * }
 *
 * Queryable:
 *   SELECT action, details->>'run_id', details->>'duration_ms',
 *          details->>'items_fetched', details->>'errors_count', success, created_at
 *   FROM audit_log
 *   WHERE event_type = 'proactive_sync'
 *   ORDER BY created_at DESC LIMIT 20;
 */
async function logSyncAudit(syncType, accountId, details) {
  try {
    await logAudit({
      event_type:    'proactive_sync',
      action:        `sync_${syncType}`,
      resource_type: syncType,
      resource_id:   accountId,
      details: {
        sync_type:  syncType,
        timestamp:  new Date().toISOString(),
        ...details,
      },
      success: details.success !== false,
    });
  } catch (err) {
    console.warn(
      `[Sync:${syncType}] Audit log failed for account ${accountId}: ${err.message}`,
      { run_id: details?.run_id, status: details?.status }
    );
  }
}

// ── Parallel Batch Runner ─────────────────────────────────────────────────────

/**
 * Runs asyncFn over items in parallel batches of `concurrency`.
 * Uses Promise.allSettled — one failure never cancels other calls in the same batch.
 * Results are returned in INPUT ORDER (allSettled preserves order).
 * Rejected promises are normalised to { success: false, error: reason.message }.
 *
 * @param {Array}    items           - items to process
 * @param {Function} asyncFn         - (item) => Promise<result>
 * @param {number}   [concurrency=3] - max simultaneous calls per batch
 * @param {number}   [batchDelayMs=200] - courtesy pause between batches
 * @returns {Promise<Array>}         - flat result array, same order as input
 */
async function runConcurrent(items, asyncFn, concurrency = 3, batchDelayMs = 200) {
  const results = [];
  for (let i = 0; i < items.length; i += concurrency) {
    const batch = items.slice(i, i + concurrency);
    const settled = await Promise.allSettled(batch.map(asyncFn));
    for (const s of settled) {
      results.push(
        s.status === 'fulfilled'
          ? s.value
          : { success: false, error: s.reason?.message || 'unknown' }
      );
    }
    if (i + concurrency < items.length) await delay(batchDelayMs);
  }
  return results;
}

// ── Exports ──────────────────────────────────────────────────────────────────

module.exports = {
  // Infrastructure
  delay,
  generateRunId,
  runConcurrent,

  // Audit
  logSyncAudit,

  // DEPRECATED circuit breaker re-exports — use substrates/retry directly
  isAccountRateLimited,
  markAccountRateLimited,
};
