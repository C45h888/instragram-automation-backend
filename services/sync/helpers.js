// backend.api/services/sync/helpers.js
// Shared infrastructure for all proactive sync domains:
//   - Rate-limit circuit breaker (shared with post-fallback.js)
//   - Auth failure strike counter
//   - DB query helpers (getActiveAccounts, getRecentMedia, getMonitoredHashtags)
//   - Enhanced logSyncAudit (writes structured run metadata to audit_log)
//
// Module-level Maps are singletons via Node cache — all domain files
// and post-fallback.js share the same circuit breaker state.

const { randomUUID } = require('crypto');
const { getSupabaseAdmin, logAudit, fireAndForgetInsert } = require('../../config/supabase');
const { clearCredentialCache, logDataBusEvent } = require('../../helpers/agent-helpers');

// ── In-memory state ──────────────────────────────────────────────────────────

const _rateLimitedAccounts = new Map(); // accountId → unblocked_at ms
const _authFailureStrikes  = new Map(); // accountId → strike count
const AUTH_FAILURE_MAX_STRIKES = 3;

let _accountsCache = { data: [], expiresAt: 0 };
const ACCOUNTS_CACHE_TTL_MS = 30 * 1000; // 30s — covers inter-cron overlap window

const _quotaUsage = new Map(); // accountId → call_count pct (0–100), from X-Business-Use-Case-Usage

const RECENT_MEDIA_CACHE_TTL_MS = 60 * 1000;     // 60s — new posts visible within 1 min
const HASHTAGS_CACHE_TTL_MS     = 5 * 60 * 1000; // 5min — hashtags changed manually, stable
const QUOTA_USAGE_TTL_MS        = 60 * 60 * 1000; // 1h  — Meta call_count rolls over 1h window
const _recentMediaCache = new Map(); // accountId → { data: [], expiresAt: 0 }
const _hashtagsCache    = new Map(); // accountId → { data: [], expiresAt: 0 }

// ── Helpers ──────────────────────────────────────────────────────────────────

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/** Generates a UUID for correlating all log entries within one cron tick. */
function generateRunId() {
  return randomUUID();
}

/**
 * Writes a run-level aggregate row to sync_run_log.
 * Called twice per domain run: once at start (status='run_started'), once at end (status='run_completed').
 * This is the authoritative source for the /sync/health endpoint and stale-domain watchdog.
 * Do NOT use logSyncAudit for run-level markers — those go here instead.
 */
async function writeSyncRunLog(entry) {
  const supabase = getSupabaseAdmin();
  if (!supabase) return;
  const { error } = await fireAndForgetInsert(supabase.from('sync_run_log').insert(entry));
  if (error) console.warn('[Sync:helpers] writeSyncRunLog failed:', error.message);
  // Part B: auto-resolve stale-domain alert when a domain recovers (run completes successfully)
  if (entry.status === 'run_completed' && entry.domain) {
    const { error: resolveErr } = await fireAndForgetInsert(
      supabase
        .from('system_alerts')
        .update({ resolved: true, resolved_at: new Date().toISOString() })
        .eq('alert_type', 'sync_stale')
        .eq('details->>domain', entry.domain)
        .eq('resolved', false)
    );
    if (resolveErr) console.warn(`[Sync:helpers] Failed to resolve stale alert for domain ${entry.domain}:`, resolveErr.message);
  }
}

// ── Circuit Breaker ──────────────────────────────────────────────────────────

function isAccountRateLimited(accountId) {
  const unblocked = _rateLimitedAccounts.get(accountId);
  if (!unblocked) return false;
  if (Date.now() >= unblocked) {
    _rateLimitedAccounts.delete(accountId);
    return false;
  }
  return true;
}

function markAccountRateLimited(accountId, retryAfterSeconds) {
  const cooldown = (retryAfterSeconds || 3600) * 1000;
  _rateLimitedAccounts.set(accountId, Date.now() + cooldown);
  console.warn(`[Sync:helpers] Account ${accountId} rate-limited for ${retryAfterSeconds || 3600}s`);
  logAudit({
    event_type: 'rate_limit_triggered',
    action: 'circuit_breaker',
    resource_type: 'instagram_business_account',
    resource_id: null,
    details: { account_id: accountId, retry_after_seconds: retryAfterSeconds || 3600, source: 'proactive_sync' },
    success: false,
  }).catch(() => {});
}

// Async — called fire-and-forget from handleFetchError
async function markAccountDisconnectedOnAuthFailure(accountId, errorMessage) {
  const supabase = getSupabaseAdmin();
  if (!supabase) return;
  try {
    await supabase
      .from('instagram_business_accounts')
      .update({ is_connected: false, connection_status: 'disconnected' })
      .eq('id', accountId);

    // Dedup: only insert if no unresolved auth_failure alert already exists for this account
    const { data: existingAlert } = await supabase
      .from('system_alerts')
      .select('id')
      .eq('business_account_id', accountId)
      .eq('alert_type', 'auth_failure')
      .eq('resolved', false)
      .maybeSingle();

    if (!existingAlert) {
      await supabase
        .from('system_alerts')
        .insert({
          alert_type: 'auth_failure',
          business_account_id: accountId,
          message: `Proactive sync auth failure: ${errorMessage}`,
          details: { source: 'proactive_sync', error: errorMessage, occurred_at: new Date().toISOString() },
          resolved: false,
        });
    }

    clearCredentialCache(accountId);
    clearAccountsCache(); // disconnected account must be excluded from next cron tick immediately
    _quotaUsage.delete(accountId);
    _recentMediaCache.delete(accountId);
    _hashtagsCache.delete(accountId);
    console.error(`[Sync:helpers] Account ${accountId} disconnected due to auth_failure`);
  } catch (err) {
    console.warn(`[Sync:helpers] Failed to mark account ${accountId} disconnected:`, err.message);
  }
}

// Sync — callers use return value for flow control.
// Returns { skip, break, retryable, retryAfterMs }:
//   skip        → caller should skip this account (auth failure)
//   break       → caller should break loop (rate limit — circuit breaker set)
//   retryable   → transient error that can be retried once
//   retryAfterMs → server-suggested wait before retry (null = use default)
function handleFetchError(result, accountId) {
  if (!result || result.success) {
    _authFailureStrikes.delete(accountId);
    return { skip: false, break: false, retryable: false, retryAfterMs: null };
  }

  if (result.error_category === 'auth_failure') {
    const strikes = (_authFailureStrikes.get(accountId) || 0) + 1;
    _authFailureStrikes.set(accountId, strikes);
    console.warn(`[Sync:helpers] Account ${accountId} auth_failure strike ${strikes}/${AUTH_FAILURE_MAX_STRIKES}`);

    logAudit({
      event_type: 'auth_failure_strike',
      action: 'circuit_breaker',
      resource_type: 'instagram_business_account',
      resource_id: null,
      details: { account_id: accountId, strike: strikes, max: AUTH_FAILURE_MAX_STRIKES },
      success: false,
    }).catch(() => {});

    if (strikes >= AUTH_FAILURE_MAX_STRIKES) {
      _authFailureStrikes.delete(accountId);
      markAccountDisconnectedOnAuthFailure(accountId, result.error || 'auth_failure').catch(() => {});
      logDataBusEvent('sync', 'token_expired_mid_run', {
        account_id: accountId,
        error_code: result.code || null,
        success: false,
      }).catch(() => {});
    }
    return { skip: true, break: false, retryable: false, retryAfterMs: null };
  }

  if (result.error_category === 'rate_limit') {
    markAccountRateLimited(accountId, result.retry_after_seconds);
    return { skip: false, break: true, retryable: false, retryAfterMs: null };
  }

  // Transient error (5xx, timeout) — retryable with server-suggested or default delay
  if (result.error_category === 'transient') {
    const retryAfterSec = result.retry_after_seconds || 30;
    const cappedSec = Math.min(retryAfterSec, 300); // cap at 5 min to avoid runaway backoff
    return { skip: false, break: false, retryable: true, retryAfterMs: cappedSec * 1000 };
  }

  return { skip: false, break: false, retryable: false, retryAfterMs: null };
}

// ── DB Query Helpers ─────────────────────────────────────────────────────────

async function getActiveAccounts() {
  if (Date.now() < _accountsCache.expiresAt) return _accountsCache.data;

  const supabase = getSupabaseAdmin();
  if (!supabase) return [];

  const { data, error } = await supabase
    .from('instagram_business_accounts')
    .select('id, instagram_business_id, user_id')
    .eq('is_connected', true)
    .eq('connection_status', 'active');

  if (error) {
    console.error('[Sync:helpers] Failed to fetch active accounts:', error.message);
    return _accountsCache.data; // serve stale on DB error — blip shouldn't halt all cron processing
  }

  _accountsCache = { data: data || [], expiresAt: Date.now() + ACCOUNTS_CACHE_TTL_MS };
  return _accountsCache.data;
}

function clearAccountsCache() {
  _accountsCache = { data: [], expiresAt: 0 };
}

// ── Meta Quota Tracking (Cluster D) ──────────────────────────────────────────

/**
 * Parses X-Business-Use-Case-Usage header → max call_count across all instagram entries.
 * Returns null if header is absent or unparseable — callers treat null as "no data, assume healthy".
 * Per Meta docs: call_count = "percentage of allowed calls made by your app over a rolling one-hour period."
 * @param {string|undefined} headerValue - raw header string from axios response
 * @returns {number|null}
 */
function parseUsageHeader(headerValue) {
  if (!headerValue) return null;
  try {
    const parsed = typeof headerValue === 'string' ? JSON.parse(headerValue) : headerValue;
    let max = 0;
    for (const entries of Object.values(parsed)) {
      for (const entry of Array.isArray(entries) ? entries : [entries]) {
        if (entry.type === 'instagram' && typeof entry.call_count === 'number') {
          max = Math.max(max, entry.call_count);
        }
      }
    }
    return max;
  } catch {
    return null;
  }
}

/**
 * Stores latest quota reading for an account.
 * Called by domain loops after collecting fetcher results.
 * @param {string} accountId
 * @param {number|null|undefined} usagePct
 */
function updateQuotaUsage(accountId, usagePct) {
  if (usagePct != null) _quotaUsage.set(accountId, { pct: usagePct, recordedAt: Date.now() });
}

/**
 * Returns the inter-account delay scaled to current Meta quota pressure.
 * Tiers (per Meta docs: 100% = throttled; monitor proactively):
 *   call_count < 50%  → ~500ms  (healthy — green)
 *   call_count 50–79% → 1500ms  (moderate pressure — yellow)
 *   call_count ≥ 80%  → maxMs   (high pressure — red, use full configured delay)
 * Defaults to green tier when no quota data exists, or when the reading is
 * older than QUOTA_USAGE_TTL_MS (1h — Meta's rolling call_count window).
 * @param {string} accountId
 * @param {number} maxMs - INTER_ACCOUNT_DELAY_MS from domain config (default 3000)
 * @returns {number}
 */
function getAdaptiveDelay(accountId, maxMs) {
  const entry = _quotaUsage.get(accountId);
  const pct = (entry && Date.now() - entry.recordedAt < QUOTA_USAGE_TTL_MS) ? entry.pct : 0;
  if (pct >= 80) return maxMs;
  if (pct >= 50) return Math.round(maxMs * 0.5);
  return Math.round(maxMs * 0.17); // ~500ms for default 3000ms config
}

// ── Per-Account TTL Caches (Cluster E) ───────────────────────────────────────

async function getRecentMedia(accountId) {
  const cached = _recentMediaCache.get(accountId);
  if (cached && Date.now() < cached.expiresAt) return cached.data;

  const supabase = getSupabaseAdmin();
  if (!supabase) return [];

  // No time filter — always return the N most recent posts regardless of age.
  // Comment sync (proactiveCommentSync) slices to COMMENT_MAX_POSTS at call site.
  // Fetching 10 here gives the caller room to cap without a second DB query.
  const { data, error } = await supabase
    .from('instagram_media')
    .select('instagram_media_id')
    .eq('business_account_id', accountId)
    .order('published_at', { ascending: false })
    .limit(10);

  if (error) {
    console.warn('[Sync:helpers] Failed to fetch recent media:', error.message);
    return cached?.data || []; // serve stale on DB error
  }

  const result = data || [];
  _recentMediaCache.set(accountId, { data: result, expiresAt: Date.now() + RECENT_MEDIA_CACHE_TTL_MS });
  return result;
}

async function getMonitoredHashtags(accountId) {
  const cached = _hashtagsCache.get(accountId);
  if (cached && Date.now() < cached.expiresAt) return cached.data;

  const supabase = getSupabaseAdmin();
  if (!supabase) return [];

  const { data, error } = await supabase
    .from('ugc_monitored_hashtags')
    .select('hashtag')
    .eq('business_account_id', accountId)
    .eq('is_active', true);

  if (error) {
    console.warn('[Sync:helpers] Failed to fetch hashtags:', error.message);
    return cached?.data || []; // serve stale on DB error
  }

  const result = (data || []).map(h => h.hashtag);
  _hashtagsCache.set(accountId, { data: result, expiresAt: Date.now() + HASHTAGS_CACHE_TTL_MS });
  return result;
}

function clearRecentMediaCache(accountId) {
  if (accountId) _recentMediaCache.delete(accountId);
  else _recentMediaCache.clear();
}

function clearHashtagsCache(accountId) {
  if (accountId) _hashtagsCache.delete(accountId);
  else _hashtagsCache.clear();
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

// ── Stale Domain Watchdog ─────────────────────────────────────────────────────

/**
 * Checks whether each sync domain has run within its expected window.
 * Inserts a system_alert if a domain is overdue.
 * Called from index.js every 5 min via the heartbeat failover cron.
 */
async function checkStaleDomains() {
  const THRESHOLDS = {
    engagement:   9  * 60 * 1000,          // 9 min  (runs every 3 min)
    ugc:          9  * 60 * 60 * 1000,      // 9 h    (runs every 3 h)
    media:        18 * 60 * 60 * 1000,      // 18 h   (runs every 6 h)
    insights:     48 * 60 * 60 * 1000,      // 48 h   (runs daily)
    token_health: 48 * 60 * 60 * 1000,      // 48 h   (runs daily)
  };
  const supabase = getSupabaseAdmin();
  if (!supabase) return;

  for (const [domain, thresholdMs] of Object.entries(THRESHOLDS)) {
    const { data } = await supabase
      .from('sync_run_log')
      .select('completed_at')
      .eq('domain', domain)
      .eq('status', 'run_completed')
      .order('completed_at', { ascending: false })
      .limit(1)
      .maybeSingle();

    const lastRun = data?.completed_at ? new Date(data.completed_at).getTime() : 0;
    if (Date.now() - lastRun > thresholdMs) {
      // Part A: dedup — skip insert if an unresolved stale alert already exists for this domain
      let skipAlert = false;
      try {
        const { data: existingAlert } = await supabase
          .from('system_alerts')
          .select('id')
          .eq('alert_type', 'sync_stale')
          .eq('details->>domain', domain)
          .eq('resolved', false)
          .maybeSingle();
        skipAlert = !!existingAlert;
      } catch (dedupErr) {
        console.warn(`[Sync:helpers] checkStaleDomains dedup query failed for ${domain}:`, dedupErr.message);
      }

      if (!skipAlert) {
        const { error: alertErr } = await fireAndForgetInsert(
          supabase
            .from('system_alerts')
            .insert({
              alert_type: 'sync_stale',
              message: `${domain} sync has not completed in expected window`,
              details: {
                domain,
                last_completed_at: data?.completed_at || null,
                threshold_ms: thresholdMs,
                source: 'stale_domain_watchdog',
              },
              resolved: false,
            })
        );
        if (alertErr) console.warn(`[Sync:helpers] checkStaleDomains alert insert failed for ${domain}:`, alertErr.message);
        console.warn(`[Sync:helpers] Stale domain detected: ${domain} (last run: ${data?.completed_at || 'never'})`);
      } else {
        console.warn(`[Sync:helpers] Stale domain ${domain} already has an unresolved alert, skipping duplicate`);
      }
    }
  }
}

// ── Exports ──────────────────────────────────────────────────────────────────

module.exports = {
  delay,
  generateRunId,
  writeSyncRunLog,
  checkStaleDomains,
  runConcurrent,
  _rateLimitedAccounts,
  _authFailureStrikes,
  isAccountRateLimited,
  markAccountRateLimited,
  handleFetchError,
  getActiveAccounts,
  clearAccountsCache,
  parseUsageHeader,
  updateQuotaUsage,
  getAdaptiveDelay,
  getRecentMedia,
  clearRecentMediaCache,
  getMonitoredHashtags,
  clearHashtagsCache,
  logSyncAudit,
};
