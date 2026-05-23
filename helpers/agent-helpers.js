// backend.api/helpers/agent-helpers.js
// Shared helper functions used across agent proxy route modules.
// Extracted from routes/agent-proxy.js to keep route files lean.

const { getSupabaseAdmin, logApiRequest, logAudit, shouldLog } = require('../config/supabase');
const { retrievePageToken } = require('../services/tokens/pat');
const { clearCredentialCache: _clearCredentialCacheRaw, getFromCache, setInCache } = require('./credential-cache');

const GRAPH_API_BASE = 'https://graph.facebook.com/v25.0';

// ============================================
// HELPER: CATEGORIZE INSTAGRAM API ERROR
// ============================================

/**
 * Maps an Instagram Graph API error to structured retry metadata.
 * Called by every catch block that handles a Graph API axios failure.
 *
 * IG rate limits come as HTTP 400 with specific error codes (4, 17, 32, 613).
 * They do NOT reliably use HTTP 429, so we must inspect the IG error code directly.
 *
 * @param {Error} error - axios error from a Graph API call
 * @returns {{ retryable: boolean, error_category: string, retry_after_seconds: number|null }}
 */
function categorizeIgError(error) {
  const status = error.response?.status;
  const igCode = error.response?.data?.error?.code;
  const retryAfterHeader = error.response?.headers?.['retry-after'];

  // Auth failures — token expired/invalid, requires user re-auth, must not retry
  if ([190, 102, 104].includes(igCode)) {
    return { retryable: false, error_category: 'auth_failure', retry_after_seconds: null };
  }

  // Permanent errors — bad params, permission denied, action blocked
  // Checked BEFORE rate-limit block because they're also HTTP 400
  if (status === 400 && igCode && ![4, 17, 32, 613].includes(igCode)) {
    return { retryable: false, error_category: 'permanent', retry_after_seconds: null };
  }

  // Rate limits — IG sends these as HTTP 400, not 429, with specific codes
  if ([4, 17, 613].includes(igCode)) {
    const retryAfter = retryAfterHeader ? parseInt(retryAfterHeader, 10) : 3600;
    return { retryable: true, error_category: 'rate_limit', retry_after_seconds: retryAfter };
  }
  if (igCode === 32) {
    // Page-level throttling — shorter cooldown than app-level
    return { retryable: true, error_category: 'rate_limit', retry_after_seconds: 900 };
  }
  // Some IG endpoints do return HTTP 429 with Retry-After
  if (status === 429) {
    const retryAfter = retryAfterHeader ? parseInt(retryAfterHeader, 10) : 3600;
    return { retryable: true, error_category: 'rate_limit', retry_after_seconds: retryAfter };
  }

  // Transient — IG server errors
  if (status >= 500) {
    return { retryable: true, error_category: 'transient', retry_after_seconds: 30 };
  }

  // Network timeout — axios ETIMEDOUT / ECONNABORTED (no response object)
  if (!status && (error.code === 'ETIMEDOUT' || error.code === 'ECONNABORTED')) {
    return { retryable: true, error_category: 'transient', retry_after_seconds: 30 };
  }

  // Unknown — default safe to retry
  return { retryable: true, error_category: 'unknown', retry_after_seconds: 60 };
}

// ============================================
// UNIFIED DATA BUS EVENT LOGGER
// ============================================

/**
 * Single entry point for structured data bus events across all sync domains and fetchers.
 * Routes through logApiRequest → api_usage table with domain tag.
 * Prevents future console.warn drift — all bus events go through here.
 *
 * @param {string} domain  - e.g. 'messaging', 'ugc', 'media', 'sync', 'account'
 * @param {string} eventType - e.g. 'proxy_failure', 'paging_truncation', 'orphan_repair'
 * @param {object} details - any additional context (account_id, error, latency_ms, etc.)
 */
async function logDataBusEvent(domain, eventType, details = {}) {
  return logApiRequest({
    endpoint: `/${domain}/${eventType}`,
    method: 'SYSTEM',
    success: details.success !== false,
    error_message: details.error || null,
    business_account_id: details.account_id || null,
    domain,
    latency: details.latency_ms || 0,
    status_code: details.status_code || (details.success !== false ? 200 : 500),
    ...details,
  }).catch(() => {});
}

// ============================================
// CREDENTIAL CACHE
// ============================================
// Prevents redundant DB round trips (2-3 hits per call) across tight-loop callers
// like proactive-sync, which calls resolveAccountCredentials per-account per-fetcher.
// TTL is intentionally short (5 min) relative to 60-day token lifetime.
// Busted explicitly on token write events (exchange-token, refresh-token routes).

/**
 * Clears the credential cache for an account.
 * Wraps the raw cache clear with optional debug audit logging.
 * @param {string} businessAccountId
 * @param {string} [reason] - why the cache is being cleared (for debug logging)
 */
function clearCredentialCache(businessAccountId, reason = 'explicit') {
  _clearCredentialCacheRaw(businessAccountId);
  if (shouldLog('debug')) {
    logAudit({
      event_type: 'credential_cache_cleared_debug',
      action: 'cache_clear',
      resource_type: 'credential',
      details: { account_id: businessAccountId, reason },
    }).catch(() => {});
  }
}


// ============================================
// HELPER: ENSURE MEDIA RECORD
// ============================================

/**
 * Ensures an instagram_media record exists for the given Instagram media ID.
 * Returns the Supabase UUID for use as FK in instagram_comments.media_id.
 * Creates a minimal stub if the record doesn't exist yet.
 *
 * @param {Object} extraFields - Optional fields to enrich the row (e.g. caption, media_type).
 *   Only applied when the row is new or when the existing row has no caption (stub enrichment).
 */
async function ensureMediaRecord(supabase, instagramMediaId, businessAccountId, extraFields = {}) {
  // Only skip if existing row already has a caption — otherwise fall through to enrich the stub
  const { data: existing } = await supabase
    .from('instagram_media')
    .select('id, caption')
    .eq('instagram_media_id', instagramMediaId)
    .limit(1)
    .single();

  if (existing?.caption) return existing.id;

  const { data: created, error } = await supabase
    .from('instagram_media')
    .upsert(
      { instagram_media_id: instagramMediaId, business_account_id: businessAccountId, ...extraFields },
      { onConflict: 'instagram_media_id' }
    )
    .select('id')
    .single();

  if (error) {
    console.warn(`⚠️ ensureMediaRecord failed for ${instagramMediaId}:`, error.message);
    return null;
  }
  return created.id;
}

// ============================================
// HELPER: SYNC HASHTAGS FROM CAPTIONS
// ============================================

/**
 * Extracts hashtags from brand post captions and upserts into ugc_monitored_hashtags.
 * Auto-populates the table the agent reads at the start of every UGC discovery cycle.
 */
async function syncHashtagsFromCaptions(supabase, businessAccountId, captions) {
  const tagSet = new Set();
  const hashtagRegex = /#(\w+)/g;
  for (const caption of captions) {
    if (!caption) continue;
    let match;
    while ((match = hashtagRegex.exec(caption)) !== null) {
      tagSet.add(match[1].toLowerCase());
    }
  }
  if (tagSet.size === 0) return;
  const records = [...tagSet].map(tag => ({
    business_account_id: businessAccountId,
    hashtag: tag,
    is_active: true,
  }));
  const { error } = await supabase
    .from('ugc_monitored_hashtags')
    .upsert(records, { onConflict: 'business_account_id,hashtag', ignoreDuplicates: true });
  if (error) console.warn('⚠️ Hashtag sync failed:', error.message);
}

// ============================================
// HELPER: RESOLVE ACCOUNT CREDENTIALS
// ============================================

/**
 * Resolves business_account_id UUID to Instagram credentials.
 * @param {string} businessAccountId - UUID from instagram_business_accounts table
 * @returns {Promise<{igUserId: string, pageToken: string, userId: string}>}
 * @throws {Error} If account not found or token retrieval fails
 */
async function resolveAccountCredentials(businessAccountId) {
  const cached = getFromCache(businessAccountId);
  if (cached) return cached;

  try {
    const supabase = getSupabaseAdmin();
    if (!supabase) {
      throw new Error('Database not available');
    }

    const { data: account, error } = await supabase
      .from('instagram_business_accounts')
      .select('instagram_business_id, user_id, is_connected, username')
      .eq('id', businessAccountId)
      .single();

    if (error || !account) {
      throw new Error(`Business account not found: ${businessAccountId}`);
    }

    if (!account.is_connected) {
      throw new Error('Business account is disconnected');
    }

    const igUserId = account.instagram_business_id;
    const userId = account.user_id;

    const pageToken = await retrievePageToken(userId, businessAccountId);

    if (!pageToken) {
      throw new Error('Failed to retrieve access token');
    }

    // Fetch page_id from credential row — stored by storePageToken, needed for pages_* scoped ops
    let pageId = null;
    try {
      const { data: cred } = await supabase
        .from('instagram_credentials')
        .select('page_id')
        .eq('business_account_id', businessAccountId)
        .eq('token_type', 'page')
        .eq('is_active', true)
        .maybeSingle();
      pageId = cred?.page_id || null;
    } catch (pageIdErr) {
      console.warn('⚠️ page_id lookup failed (non-blocking):', pageIdErr.message);
    }

    const result = { igUserId, pageToken, userId, pageId, igUsername: account.username || null };
    setInCache(businessAccountId, result);
    return result;
  } catch (error) {
    console.error('❌ Credential resolution failed:', error.message);
    throw error;
  }
}

// ============================================
// HELPER: IDEMPOTENCY KEY
// ============================================

const crypto = require('crypto');

/**
 * Returns a deterministic SHA-256 hex digest from an action seed string.
 * Used as idempotency_key in post_queue to prevent duplicate in-flight rows.
 * @param {string} seed - e.g. 'reply_comment:123456789'
 * @returns {string} 64-char hex digest
 */
function buildIdempotencyKey(seed) {
  return crypto.createHash('sha256').update(seed).digest('hex');
}

// ============================================
// HELPER: POST QUEUE INSERT
// ============================================

/**
 * Inserts an intent row into post_queue before an outgoing IG API call.
 * On unique-constraint conflict (row already in-flight for this idempotency_key),
 * returns the existing row's id instead of throwing.
 * @param {object} supabase - Supabase admin client
 * @param {{ business_account_id: string, action_type: string, payload: object, idempotency_key: string }} params
 * @returns {Promise<string|null>} UUID of the queue row, or null on unexpected error
 */
async function insertQueueRow(supabase, { business_account_id, action_type, payload, idempotency_key }) {
  try {
    const { data, error } = await supabase
      .from('post_queue')
      .insert({ business_account_id, action_type, payload, idempotency_key, status: 'pending' })
      .select('id')
      .single();

    if (error) {
      // 23505 = unique_violation — row already active for this idempotency_key
      if (error.code === '23505') {
        const { data: existing } = await supabase
          .from('post_queue')
          .select('id')
          .eq('idempotency_key', idempotency_key)
          .not('status', 'in', '("sent","dlq")')
          .single();
        console.warn(`⚠️ Queue duplicate suppressed [${action_type}], existing row: ${existing?.id}`);
        return existing?.id || null;
      }
      console.warn(`⚠️ Queue insert failed [${action_type}]:`, error.message);
      return null;
    }
    return data.id;
  } catch (err) {
    console.warn(`⚠️ Queue insert error [${action_type}]:`, err.message);
    return null;
  }
}

// ============================================
// HELPER: POST QUEUE UPDATE
// ============================================

/**
 * Updates a post_queue row with the provided fields.
 * Silently no-ops if id is null (queue disabled or insert failed).
 * @param {object} supabase - Supabase admin client
 * @param {string|null} id - post_queue UUID
 * @param {object} fields - Columns to update (status, instagram_id, error, etc.)
 */
async function updateQueueRow(supabase, id, fields) {
  if (!id) return;
  try {
    const { error } = await supabase
      .from('post_queue')
      .update(fields)
      .eq('id', id);
    if (error) console.warn(`⚠️ Queue update failed [${id}]:`, error.message);
  } catch (err) {
    console.warn(`⚠️ Queue update error [${id}]:`, err.message);
  }
}

// ============================================
// MEDIA CONTAINER STATUS POLL (VIDEO / REELS)
// ============================================

const axios = require('axios');

/**
 * Polls GET /{creationId}?fields=status_code,status until the container reaches
 * FINISHED or a terminal error, before calling media_publish.
 *
 * Required by Meta for VIDEO and REELS; not needed for IMAGE.
 * Meta docs: https://developers.facebook.com/docs/instagram-api/reference/ig-media
 *
 * Status codes returned by Meta:
 *   EXPIRED     — container not published within 24h (non-retryable)
 *   ERROR       — container processing failed (non-retryable)
 *   FINISHED    — ready to publish
 *   IN_PROGRESS — still processing (keep polling)
 *   PUBLISHED   — already published (idempotent — treat as ready)
 *
 * @param {string} creationId   - Media container ID from POST /{igUserId}/media
 * @param {string} pageToken    - Page access token
 * @param {object} [opts]
 * @param {number} [opts.maxAttempts=12]    - Max poll iterations (default: 2 min at 10s interval)
 * @param {number} [opts.intervalMs=10000]  - Poll interval in ms
 * @throws {Error} if status is ERROR/EXPIRED or max attempts exceeded
 */
async function pollMediaContainerStatus(creationId, pageToken, opts = {}) {
  const { maxAttempts = 12, intervalMs = 10000 } = opts;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const { data } = await axios.get(`${GRAPH_API_BASE}/${creationId}`, {
      params: { fields: 'status_code,status', access_token: pageToken },
      timeout: 10000,
    });

    const statusCode = data.status_code;

    if (statusCode === 'FINISHED' || statusCode === 'PUBLISHED') return;

    if (statusCode === 'ERROR' || statusCode === 'EXPIRED') {
      throw new Error(`Media container ${creationId} failed with status: ${statusCode}`);
    }

    // IN_PROGRESS — wait before next attempt (skip wait on last attempt)
    if (attempt < maxAttempts) {
      await new Promise(resolve => setTimeout(resolve, intervalMs));
    }
  }

  throw new Error(`Media container ${creationId} not ready after ${maxAttempts} attempts`);
}

// ============================================
// HELPER: ENSURE CONVERSATION ROWS
// ============================================

/**
 * Guarantees conversation rows exist in instagram_dm_conversations for a set of thread IDs.
 * Called by storeMessageBatches() when a conversation batch has no matching DB row,
 * preventing silent message drop.
 *
 * Strategy:
 *   1. Fetch live conversation data from Meta API (up to 2 attempts).
 *   2. Upsert proper rows for any thread IDs found in the API response.
 *   3. For thread IDs still not found after both attempts: upsert minimal stub rows
 *      so messages can be stored against a valid FK. Stubs are never overwritten by
 *      subsequent proper rows (ignoreDuplicates: true on stub upsert only).
 *
 * @param {Object} supabase - Supabase admin client
 * @param {string} businessAccountId - UUID
 * @param {string[]} missingThreadIds - Thread IDs not currently in instagram_dm_conversations
 * @param {string} igUserId - Business IG User ID
 * @param {string} pageToken - Page access token
 * @param {string|null} pageId - Facebook Page ID (preferred node for conversations endpoint)
 */
async function ensureConversationRows(supabase, businessAccountId, missingThreadIds, igUserId, pageToken, pageId) {
  const MAX_ATTEMPTS = 2;
  const remaining = new Set(missingThreadIds);
  const now = new Date();

  for (let attempt = 1; attempt <= MAX_ATTEMPTS && remaining.size > 0; attempt++) {
    try {
      const conversationNode = pageId || igUserId;
      const convRes = await axios.get(`${GRAPH_API_BASE}/${conversationNode}/conversations`, {
        params: {
          fields: 'id,participants{id,username},updated_time,message_count,messages.limit(2){created_time,from{id}}',
          platform: 'INSTAGRAM',
          limit: 50,
          access_token: pageToken,
        },
        timeout: 10000,
      });

      const apiConversations = convRes.data?.data || [];
      const foundRecords = [];

      for (const conv of apiConversations) {
        if (!remaining.has(conv.id)) continue;

        // Calculate window from customer's last message only
        const customerMsg = conv.messages?.data?.find(
          m => m.from?.id !== igUserId && m.from?.id !== pageId
        );
        const lastCustomerTime = customerMsg ? new Date(customerMsg.created_time) : null;
        const hoursSince = lastCustomerTime ? (now - lastCustomerTime) / 3_600_000 : null;
        const isOpen = hoursSince !== null && hoursSince < 24;
        const hoursRemaining = hoursSince !== null ? Math.max(0, 24 - hoursSince) : null;

        const participants = conv.participants?.data || [];
        const customerParticipant = participants.find(
          p => p.id !== igUserId && p.id !== pageId
        ) || participants[0];

        foundRecords.push({
          instagram_thread_id: conv.id,
          customer_instagram_id: customerParticipant?.id || null,
          customer_username: customerParticipant?.username || null,
          business_account_id: businessAccountId,
          within_window: isOpen,
          window_expires_at: isOpen && hoursRemaining != null
            ? new Date(Date.now() + hoursRemaining * 3_600_000).toISOString()
            : null,
          last_message_at: conv.updated_time || null,
          last_user_message_at: lastCustomerTime ? lastCustomerTime.toISOString() : null,
          message_count: conv.message_count || 0,
          conversation_status: 'active',
        });

        remaining.delete(conv.id);
      }

      if (foundRecords.length > 0) {
        // Batch-resolve customer_user_id for platform accounts (same logic as storeConversationBatches)
        const igIds = foundRecords.map(r => r.customer_instagram_id).filter(Boolean);
        if (igIds.length > 0) {
          const { data: knownAccounts } = await supabase
            .from('instagram_business_accounts')
            .select('instagram_business_id, user_id')
            .in('instagram_business_id', igIds);
          const igIdToUserId = {};
          for (const a of knownAccounts || []) igIdToUserId[a.instagram_business_id] = a.user_id;
          for (const r of foundRecords) {
            if (r.customer_instagram_id) r.customer_user_id = igIdToUserId[r.customer_instagram_id] || null;
          }
        }
        await supabase
          .from('instagram_dm_conversations')
          .upsert(foundRecords, { onConflict: 'instagram_thread_id', ignoreDuplicates: false });
      }

    } catch (err) {
      console.warn(`[ensureConversationRows] API attempt ${attempt}/${MAX_ATTEMPTS} failed:`, err.message);
    }
  }

  // Any thread IDs still not found after all API attempts → insert stub rows
  // ignoreDuplicates: true ensures stubs never overwrite a proper row
  if (remaining.size > 0) {
    const stubs = [...remaining].map(threadId => ({
      instagram_thread_id: threadId,
      business_account_id: businessAccountId,
      within_window: false,
      conversation_status: 'active',
    }));
    const { error: stubErr } = await supabase
      .from('instagram_dm_conversations')
      .upsert(stubs, { onConflict: 'instagram_thread_id', ignoreDuplicates: true });
    if (stubErr) {
      console.warn('[ensureConversationRows] Stub upsert failed:', stubErr.message);
    } else {
      console.warn(
        `[ensureConversationRows] ${stubs.length} conversation(s) not found via API — ` +
        `stub row(s) inserted: ${[...remaining].join(', ')}`
      );
    }
  }
}

module.exports = {
  ensureMediaRecord,
  ensureConversationRows,
  syncHashtagsFromCaptions,
  resolveAccountCredentials,
  clearCredentialCache,
  categorizeIgError,
  logDataBusEvent,
  GRAPH_API_BASE,
  buildIdempotencyKey,
  insertQueueRow,
  updateQueueRow,
  pollMediaContainerStatus,
};
