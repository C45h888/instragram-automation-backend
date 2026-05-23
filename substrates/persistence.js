// substrates/persistence.js
// Bounded substrate: canonical Supabase persistence.
//
// Owns: batch upserts to Supabase domain tables, credential resolution,
//        account/media/hashtag discovery with TTL caching.
// Does NOT own: Instagram API calls, retry decisions, schema normalization,
//               orchestration, telemetry.

const { getSupabaseAdmin } = require('../config/supabase');
const { resolveAccountCredentials, ensureConversationRows, ensureMediaRecord, syncHashtagsFromCaptions } = require('../helpers/agent-helpers');
const { transformMessage } = require('./normalization');
const { logWithDomain } = require('./telemetry');
const { _setClearAccountsCache } = require('./retry');

// ── TTL Caches ───────────────────────────────────────────────────────────────

let _accountsCache = { data: [], expiresAt: 0 };
const ACCOUNTS_CACHE_TTL_MS = 30 * 1000;

const _recentMediaCache = new Map(); // accountId → { data: [], expiresAt: number }
const _hashtagsCache    = new Map(); // accountId → { data: [], expiresAt: number }
const RECENT_MEDIA_CACHE_TTL_MS = 60 * 1000;
const HASHTAGS_CACHE_TTL_MS     = 5 * 60 * 1000;

// Wire retry substrate's cache clear hook
_setClearAccountsCache(() => { _accountsCache = { data: [], expiresAt: 0 }; });

// ═══════════════════════════════════════════════════════════════════════════════
// ACCOUNT DISCOVERY
// ═══════════════════════════════════════════════════════════════════════════════

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
    console.error('[persistence] Failed to fetch active accounts:', error.message);
    return _accountsCache.data;
  }

  _accountsCache = { data: data || [], expiresAt: Date.now() + ACCOUNTS_CACHE_TTL_MS };
  return _accountsCache.data;
}

// ═══════════════════════════════════════════════════════════════════════════════
// MEDIA & HASHTAG DISCOVERY
// ═══════════════════════════════════════════════════════════════════════════════

async function getRecentMedia(accountId) {
  const cached = _recentMediaCache.get(accountId);
  if (cached && Date.now() < cached.expiresAt) return cached.data;

  const supabase = getSupabaseAdmin();
  if (!supabase) return [];

  const { data, error } = await supabase
    .from('instagram_media')
    .select('instagram_media_id')
    .eq('business_account_id', accountId)
    .order('published_at', { ascending: false })
    .limit(10);

  if (error) {
    console.warn('[persistence] Failed to fetch recent media:', error.message);
    return cached?.data || [];
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
    console.warn('[persistence] Failed to fetch hashtags:', error.message);
    return cached?.data || [];
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

// ═══════════════════════════════════════════════════════════════════════════════
// CREDENTIAL RESOLUTION
// ═══════════════════════════════════════════════════════════════════════════════

// Re-export for single import surface
module.exports.resolveAccountCredentials = resolveAccountCredentials;

// ═══════════════════════════════════════════════════════════════════════════════
// COMMENTS — BATCH WRITER
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Batch-writes comment records from multiple media posts in a single DB round-trip.
 *
 * @param {string} businessAccountId
 * @param {Array<{mediaId: string, comments: Array}>} batches
 * @returns {Promise<{count: number}>}
 */
async function storeCommentBatches(businessAccountId, batches) {
  const supabase = getSupabaseAdmin();
  if (!supabase || !batches.length) return { count: 0 };

  const mediaIds = [...new Set(batches.map(b => b.mediaId))];

  // Batch-resolve existing media UUIDs in one SELECT
  const { data: existingMedia } = await supabase
    .from('instagram_media')
    .select('id, instagram_media_id')
    .in('instagram_media_id', mediaIds);

  const mediaUUIDMap = {};
  for (const row of existingMedia || []) {
    mediaUUIDMap[row.instagram_media_id] = row.id;
  }

  // Upsert any missing media stubs in one call
  const missingIds = mediaIds.filter(id => !mediaUUIDMap[id]);
  if (missingIds.length > 0) {
    const stubs = missingIds.map(id => ({
      instagram_media_id: id,
      business_account_id: businessAccountId,
    }));
    const { data: created } = await supabase
      .from('instagram_media')
      .upsert(stubs, { onConflict: 'instagram_media_id' })
      .select('id, instagram_media_id');
    for (const row of created || []) {
      mediaUUIDMap[row.instagram_media_id] = row.id;
    }
  }

  // Build flat comment records across all batches
  const allRecords = [];
  for (const { mediaId, comments } of batches) {
    const mediaUUID = mediaUUIDMap[mediaId];
    if (!mediaUUID) continue;
    for (const c of comments) {
      if (!c.id) continue;
      allRecords.push({
        instagram_comment_id: c.id,
        text: c.text || '',
        author_username: c.username || '',
        author_instagram_id: null,
        media_id: mediaUUID,
        business_account_id: businessAccountId,
        created_at: c.timestamp,
        like_count: c.like_count || 0,
        reply_count: 0,
      });
    }
  }

  if (allRecords.length === 0) return { count: 0 };

  const { error: upsertErr } = await supabase
    .from('instagram_comments')
    .upsert(allRecords, { onConflict: 'instagram_comment_id', ignoreDuplicates: false });

  if (upsertErr) {
    await logWithDomain('messaging', {
      endpoint: '/post-comments/upsert', method: 'SYSTEM', success: false,
      business_account_id: businessAccountId,
      error: upsertErr.message,
      details: { action: 'db_upsert_failed', table: 'instagram_comments', count_attempted: allRecords.length },
    });
    throw upsertErr;
  }

  return { count: allRecords.length };
}

// ═══════════════════════════════════════════════════════════════════════════════
// CONVERSATIONS — BATCH WRITER
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Batch-writes conversation records in a single DB round-trip.
 * Calculates 24h messaging window from the CUSTOMER's last message only.
 *
 * @param {string} businessAccountId
 * @param {Array} rawConversations - Raw API conversations array
 * @param {string} igUserId - Business IG User ID
 * @param {string|null} pageId - Facebook Page ID
 * @returns {Promise<{count: number, conversations: Array}>}
 */
async function storeConversationBatches(businessAccountId, rawConversations, igUserId, pageId) {
  const supabase = getSupabaseAdmin();
  if (!supabase || !rawConversations.length) return { count: 0, conversations: [] };

  const now = new Date();
  const convRecords = [];
  const shapedConversations = [];

  for (const conv of rawConversations) {
    const customerMsg = conv.messages?.data?.find(
      m => m.from?.id !== igUserId && m.from?.id !== pageId
    );
    const lastCustomerTime = customerMsg ? new Date(customerMsg.created_time) : null;
    const hoursSince = lastCustomerTime ? (now - lastCustomerTime) / 3_600_000 : null;
    const isOpen = hoursSince !== null && hoursSince < 24;
    const hoursRemaining = hoursSince !== null ? Math.max(0, 24 - hoursSince) : null;
    const windowExpiresAt = isOpen && hoursRemaining != null
      ? new Date(Date.now() + hoursRemaining * 3_600_000).toISOString()
      : null;

    const participants = conv.participants?.data || [];
    const customerParticipant = participants.find(
      p => p.id !== igUserId && p.id !== pageId
    ) || participants[0];

    if (!customerParticipant?.id) continue;

    convRecords.push({
      instagram_thread_id: conv.id,
      customer_instagram_id: customerParticipant.id,
      customer_username: customerParticipant.username || null,
      business_account_id: businessAccountId,
      within_window: isOpen,
      window_expires_at: windowExpiresAt,
      last_message_at: conv.updated_time || null,
      last_user_message_at: lastCustomerTime ? lastCustomerTime.toISOString() : null,
      message_count: conv.message_count || 0,
      conversation_status: 'active',
    });

    shapedConversations.push({
      id: conv.id, participants,
      last_message_at: conv.updated_time,
      message_count: conv.message_count || 0,
      last_message: conv.messages?.data?.[0] || null,
      messaging_window: {
        is_open: isOpen,
        hours_remaining: hoursRemaining !== null ? parseFloat(hoursRemaining.toFixed(1)) : null,
        requires_template: hoursSince !== null && hoursSince >= 24,
        last_customer_message_at: lastCustomerTime ? lastCustomerTime.toISOString() : null,
      },
      within_window: isOpen,
      can_send_messages: isOpen,
    });
  }

  if (convRecords.length === 0) return { count: 0, conversations: shapedConversations };

  // Batch-resolve customer_user_id
  const igIds = convRecords.map(r => r.customer_instagram_id).filter(Boolean);
  if (igIds.length > 0) {
    const { data: knownAccounts } = await supabase
      .from('instagram_business_accounts')
      .select('instagram_business_id, user_id')
      .in('instagram_business_id', igIds);
    const igIdToUserId = {};
    for (const a of knownAccounts || []) igIdToUserId[a.instagram_business_id] = a.user_id;
    for (const r of convRecords) {
      if (r.customer_instagram_id) r.customer_user_id = igIdToUserId[r.customer_instagram_id] || null;
    }
  }

  const { error: upsertErr } = await supabase
    .from('instagram_dm_conversations')
    .upsert(convRecords, { onConflict: 'instagram_thread_id', ignoreDuplicates: false });

  if (upsertErr) {
    await logWithDomain('messaging', {
      endpoint: '/conversations/upsert', method: 'SYSTEM', success: false,
      business_account_id: businessAccountId,
      error: upsertErr.message,
      details: { action: 'db_upsert_failed', table: 'instagram_dm_conversations', count_attempted: convRecords.length },
    });
    throw upsertErr;
  }

  return { count: convRecords.length, conversations: shapedConversations };
}

// ═══════════════════════════════════════════════════════════════════════════════
// MESSAGES — BATCH WRITER
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Batch-writes message records from multiple conversations in a single DB round-trip.
 * Never silently drops messages — calls ensureConversationRows for unknown conversations.
 *
 * @param {string} businessAccountId
 * @param {Array<{conversationId: string, rawMessages: Array}>} batches
 * @param {string} igUserId
 * @param {string|null} pageId
 * @param {object|null} [credentials] - { pageToken, igUserId, pageId }
 * @returns {Promise<{count: number}>}
 */
async function storeMessageBatches(businessAccountId, batches, igUserId, pageId, credentials = null) {
  const supabase = getSupabaseAdmin();
  if (!supabase || !batches.length) return { count: 0 };

  // Batch-resolve conversation UUIDs + customer IDs
  const threadIds = batches.map(b => b.conversationId);
  const { data: convRows } = await supabase
    .from('instagram_dm_conversations')
    .select('id, instagram_thread_id, customer_instagram_id')
    .in('instagram_thread_id', threadIds);

  const convMap = {};
  for (const row of convRows || []) {
    convMap[row.instagram_thread_id] = { uuid: row.id, customerIgId: row.customer_instagram_id };
  }

  // Recover any missing conversations — never silently drop messages
  const missingThreadIds = threadIds.filter(id => !convMap[id]);
  if (missingThreadIds.length > 0) {
    await ensureConversationRows(supabase, businessAccountId, missingThreadIds, igUserId, credentials?.pageToken || null, pageId);

    const { data: newRows } = await supabase
      .from('instagram_dm_conversations')
      .select('id, instagram_thread_id, customer_instagram_id')
      .in('instagram_thread_id', missingThreadIds);
    for (const row of newRows || []) {
      convMap[row.instagram_thread_id] = { uuid: row.id, customerIgId: row.customer_instagram_id };
    }
  }

  // Transform each message
  const allMsgRecords = [];
  for (const { conversationId, rawMessages } of batches) {
    const conv = convMap[conversationId];
    if (!conv) continue;
    for (const m of rawMessages) {
      if (!m.id) continue;
      allMsgRecords.push(transformMessage(m, conv.uuid, businessAccountId, igUserId, pageId, conv.customerIgId));
    }
  }

  if (allMsgRecords.length === 0) return { count: 0 };

  const { error: upsertErr } = await supabase
    .from('instagram_dm_messages')
    .upsert(allMsgRecords, { onConflict: 'instagram_message_id', ignoreDuplicates: true });

  if (upsertErr) {
    await logWithDomain('messaging', {
      endpoint: '/conversation-messages/upsert', method: 'SYSTEM', success: false,
      business_account_id: businessAccountId,
      error: upsertErr.message,
      details: { action: 'db_upsert_failed', table: 'instagram_dm_messages', count_attempted: allMsgRecords.length },
    });
    throw upsertErr;
  }

  // Parallel orphan repair
  const messageIdsByConv = {};
  for (const r of allMsgRecords) {
    if (!r.conversation_id) continue;
    if (!messageIdsByConv[r.conversation_id]) messageIdsByConv[r.conversation_id] = [];
    messageIdsByConv[r.conversation_id].push(r.instagram_message_id);
  }

  const repairPromises = Object.entries(messageIdsByConv).map(([convUUID, msgIds]) =>
    supabase
      .from('instagram_dm_messages')
      .update({ conversation_id: convUUID, business_account_id: businessAccountId })
      .in('instagram_message_id', msgIds)
      .is('conversation_id', null)
      .select('instagram_message_id')
      .then(({ data: repaired }) => {
        if (repaired?.length) {
          logWithDomain('messaging', {
            endpoint: '/messaging/orphan_repair', method: 'SYSTEM', success: true,
            business_account_id: businessAccountId,
            details: { action: 'orphan_repair', messages_repaired: repaired.length, conversation_id: convUUID },
          }).catch(() => {});
        }
      })
  );
  await Promise.allSettled(repairPromises);

  return { count: allMsgRecords.length };
}

// ═══════════════════════════════════════════════════════════════════════════════
// UGC — BATCH WRITER
// ═══════════════════════════════════════════════════════════════════════════════

async function storeUgcContentBatch(ugcRecords) {
  if (!ugcRecords || ugcRecords.length === 0) return { count: 0 };
  const supabase = getSupabaseAdmin();
  if (!supabase) return { count: 0 };

  const businessAccountId = ugcRecords[0]?.business_account_id || null;

  const { error: upsertErr } = await supabase
    .from('ugc_content')
    .upsert(ugcRecords, { onConflict: 'business_account_id,visitor_post_id', ignoreDuplicates: false });

  if (upsertErr) {
    await logWithDomain('ugc', {
      endpoint: '/ugc-content/upsert', method: 'SYSTEM', success: false,
      business_account_id: businessAccountId,
      error: upsertErr.message,
      details: { action: 'db_upsert_failed', table: 'ugc_content', count_attempted: ugcRecords.length },
    });
    throw upsertErr;
  }

  return { count: ugcRecords.length };
}

// ═══════════════════════════════════════════════════════════════════════════════
// MEDIA INSIGHTS — BATCH WRITER
// ═══════════════════════════════════════════════════════════════════════════════

async function storeMediaInsightsBatch(businessAccountId, mediaInsights, captions) {
  if (!mediaInsights.length) return { count: 0 };
  const supabase = getSupabaseAdmin();
  if (!supabase) return { count: 0 };

  const { normalizeMediaInsight } = require('./normalization');
  const mediaRecords = mediaInsights.map(m => normalizeMediaInsight(m, businessAccountId));

  const { error: mediaErr } = await supabase
    .from('instagram_media')
    .upsert(mediaRecords, { onConflict: 'instagram_media_id', ignoreDuplicates: false });

  if (mediaErr) {
    await logWithDomain('media', {
      endpoint: '/media-insights/upsert', method: 'SYSTEM', success: false,
      business_account_id: businessAccountId,
      error: mediaErr.message,
      details: { action: 'db_upsert_failed', table: 'instagram_media', count_attempted: mediaRecords.length },
    });
    throw mediaErr;
  }

  if (captions.length > 0) {
    await syncHashtagsFromCaptions(supabase, businessAccountId, captions);
  }

  return { count: mediaRecords.length };
}

// ═══════════════════════════════════════════════════════════════════════════════
// BUSINESS POSTS — BATCH WRITER
// ═══════════════════════════════════════════════════════════════════════════════

async function storeBusinessPosts(businessAccountId, posts) {
  if (!posts.length) return { count: 0 };
  const supabase = getSupabaseAdmin();
  if (!supabase) return { count: 0 };

  const { normalizeBusinessPost, syncHashtagsFromCaptions } = require('./normalization');
  const mediaRecords = posts.map(p => normalizeBusinessPost(p, businessAccountId));

  const { error: upsertErr } = await supabase
    .from('instagram_media')
    .upsert(mediaRecords, { onConflict: 'instagram_media_id', ignoreDuplicates: false });

  if (upsertErr) {
    await logWithDomain('media', {
      endpoint: '/sync/posts/upsert', method: 'SYSTEM', success: false,
      business_account_id: businessAccountId,
      error: upsertErr.message,
      details: { action: 'db_upsert_failed', table: 'instagram_media', count_attempted: mediaRecords.length },
    });
    throw upsertErr;
  }

  const captions = posts.map(p => p.caption).filter(Boolean);
  if (captions.length > 0) {
    await syncHashtagsFromCaptions(supabase, businessAccountId, captions);
  }

  return { count: mediaRecords.length };
}

// ═══════════════════════════════════════════════════════════════════════════════

module.exports = {
  // Account discovery
  getActiveAccounts,

  // Per-account caches
  getRecentMedia,
  getMonitoredHashtags,
  clearRecentMediaCache,
  clearHashtagsCache,

  // Credential resolution
  resolveAccountCredentials,

  // Batch writers
  storeCommentBatches,
  storeConversationBatches,
  storeMessageBatches,
  storeUgcContentBatch,
  storeMediaInsightsBatch,
  storeBusinessPosts,
};
