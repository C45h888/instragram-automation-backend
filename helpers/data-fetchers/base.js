// backend.api/helpers/data-fetchers/base.js
// Shared wiring for all domain fetcher modules.
// Each domain file requires only this file instead of importing 5+ things individually.
//
// IMPORTANT: Do NOT add route logic here. This file is infrastructure only.

const axios = require('axios');
const { getSupabaseAdmin, logApiRequest } = require('../../config/supabase');
const {
  resolveAccountCredentials,
  categorizeIgError,
  ensureMediaRecord,
  ensureConversationRows,
  syncHashtagsFromCaptions,
  GRAPH_API_BASE,
} = require('../agent-helpers');
const { mapRawPostToUgcContent } = require('../ugc-field-map');
const { parseUsageHeader } = require('../../services/sync/helpers');

// ============================================
// PER-DOMAIN LOGGER
// ============================================

/**
 * Tags an api_usage log row with a domain identifier.
 * Enables instant domain-scoped failure queries:
 *   SELECT * FROM api_usage WHERE domain = 'ugc' AND success = false ORDER BY created_at DESC
 *
 * @param {'messaging'|'ugc'|'media'|'account'} domain
 * @param {Object} payload  - Same shape as logApiRequest: { endpoint, method, business_account_id, ... }
 */
async function logWithDomain(domain, payload) {
  return logApiRequest({ ...payload, domain }).catch(() => {});
}

// ============================================
// MESSAGE TRANSFORM
// ============================================

/**
 * Maps any Meta message object → DB row shape for instagram_dm_messages.
 * Pure function — no async, no imports. Used by storeMessageBatches and
 * the fetchAndStoreMessages shim in messaging-fetchers.js.
 *
 * @param {object} m                - Raw IG message object from Graph API
 * @param {string|null} conversationUUID  - Supabase UUID for the conversation row
 * @param {string} businessAccountId
 * @param {string} igUserId         - IG User ID (IGSID for the business)
 * @param {string|null} pageId      - Facebook Page ID (defensive fallback)
 * @param {string|null} customerIgId - Customer's IGSID (for recipient fallback)
 * @returns {object} DB row ready for upsert
 */
function transformMessage(m, conversationUUID, businessAccountId, igUserId, pageId, customerIgId) {
  // pageId: defensive fallback — Meta docs list IGSID for business as either igUserId or pageId
  const fromBusiness = m.from?.id === igUserId || (pageId && m.from?.id === pageId);

  // Graph API attachment sub-fields (NOT webhook payload.url format)
  const att = m.attachments?.data?.[0] || null;
  const imgData = att?.image_data || null;
  const isSticker = imgData?.render_as_sticker === true;

  // Media URL: priority order covers all attachment variants
  const mediaUrl = imgData?.url
    || imgData?.animated_gif_url       // animated GIF
    || att?.file_url                   // PDF / file
    || m.story?.link                   // story reply CDN URL (top-level field)
    || null;

  // message_type: mapped to DB CHECK constraint values.
  // DB allows: text, media, story_reply, story_mention, post_share,
  //            voice_note, reel_share, icebreaker
  let messageType = 'text';
  if (isSticker)                    messageType = 'media';      // sticker = image attachment
  else if (att)                     messageType = 'media';      // image, GIF, audio, video, file
  else if (m.story)                 messageType = 'story_reply';
  else if (m.shares?.data?.length)  messageType = 'post_share'; // DB enum is 'post_share'
  else if (m.is_unsupported)        messageType = 'text';       // unrenderable — safe fallback

  // media_type: coarse MIME category for frontend rendering decisions
  const mediaType = imgData ? 'image' : att?.file_url ? 'file' : null;

  return {
    instagram_message_id: m.id,
    message_text: m.message || null,  // null not '' — empty string is semantically wrong
    message_type: messageType,
    media_url: mediaUrl,
    media_type: mediaType,
    conversation_id: conversationUUID,
    business_account_id: businessAccountId,
    is_from_business: fromBusiness,
    // Meta omits `to` when no data — derive from known IDs as fallback
    recipient_instagram_id: m.to?.data?.[0]?.id
      || (fromBusiness ? customerIgId : igUserId)
      || '',
    sender_username: m.from?.username || null,
    sent_at: m.created_time,
    send_status: fromBusiness ? 'sent' : 'delivered',
  };
}

// ============================================
// BATCH WRITERS
// ============================================

/**
 * Batch-writes comment records from multiple media posts in a single DB round-trip.
 * Replaces N×ensureMediaRecord + N×upsertComments with 1×SELECT + 1×upsert (+ 1 stub upsert if needed).
 *
 * @param {string} businessAccountId
 * @param {Array<{mediaId: string, comments: Array}>} batches
 * @returns {Promise<{count: number}>}
 */
async function storeCommentBatches(businessAccountId, batches) {
  const supabase = getSupabaseAdmin();
  if (!supabase || !batches.length) return { count: 0 };

  // Step 1: Collect all unique mediaIds across all batches
  const mediaIds = [...new Set(batches.map(b => b.mediaId))];

  // Step 2: Batch-resolve existing media UUIDs in one SELECT
  const { data: existingMedia } = await supabase
    .from('instagram_media')
    .select('id, instagram_media_id')
    .in('instagram_media_id', mediaIds);

  const mediaUUIDMap = {};
  for (const row of existingMedia || []) {
    mediaUUIDMap[row.instagram_media_id] = row.id;
  }

  // Step 3: Upsert any missing media stubs in one call
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

  // Step 4: Build flat comment records across all batches
  const allCommentRecords = [];
  for (const { mediaId, comments } of batches) {
    const mediaUUID = mediaUUIDMap[mediaId];
    if (!mediaUUID) continue;
    for (const c of comments) {
      if (!c.id) continue;
      allCommentRecords.push({
        instagram_comment_id: c.id,
        text: c.text || '',
        author_username: c.username || '',
        author_instagram_id: null,
        media_id: mediaUUID,
        business_account_id: businessAccountId,
        created_at: c.timestamp,
        like_count: c.like_count || 0,
        reply_count: 0, // replies_count is not a valid IG field — only a replies edge exists
      });
    }
  }

  if (allCommentRecords.length === 0) return { count: 0 };

  // Step 5: Single batch upsert
  // ignoreDuplicates: false (ON CONFLICT DO UPDATE) is intentional — updates in place
  // producing one new heap tuple per upsert. ignoreDuplicates: true fires INSERT and
  // silently discards on conflict — still writes, but with no update benefit.
  // Aggressive autovacuum tuning (threshold=5, scale_factor=0) keeps bloat minimal.
  const { error: upsertErr } = await supabase
    .from('instagram_comments')
    .upsert(allCommentRecords, { onConflict: 'instagram_comment_id', ignoreDuplicates: false });

  if (upsertErr) {
    await logWithDomain('messaging', {
      endpoint: '/post-comments/upsert', method: 'SYSTEM', success: false,
      business_account_id: businessAccountId,
      error: upsertErr.message,
      details: { action: 'db_upsert_failed', table: 'instagram_comments', count_attempted: allCommentRecords.length },
    });
    throw upsertErr;
  }

  return { count: allCommentRecords.length };
}

/**
 * Batch-writes message records from multiple conversations in a single DB round-trip.
 * Replaces N×UUIDlookup + N×upsert + N×orphanRepair with 1×SELECT + 1×upsert + N×orphan (parallel).
 *
 * Data leak fix: instead of silently dropping messages for unknown conversations,
 * this function calls ensureConversationRows() to create the missing conversation
 * row (via API retry → stub fallback) before proceeding. No messages are ever dropped.
 *
 * @param {string} businessAccountId
 * @param {Array<{conversationId: string, rawMessages: Array}>} batches
 * @param {string} igUserId   - Business IG User ID (from resolveAccountCredentials)
 * @param {string|null} pageId - Facebook Page ID (defensive fallback)
 * @param {object|null} [credentials] - { pageToken, igUserId, pageId } — enables API recovery
 * @returns {Promise<{count: number}>}
 */
async function storeMessageBatches(businessAccountId, batches, igUserId, pageId, credentials = null) {
  const supabase = getSupabaseAdmin();
  if (!supabase || !batches.length) return { count: 0 };

  // Step 1: Batch-resolve conversation UUIDs + customer IDs in one SELECT
  const threadIds = batches.map(b => b.conversationId);
  const { data: convRows } = await supabase
    .from('instagram_dm_conversations')
    .select('id, instagram_thread_id, customer_instagram_id')
    .in('instagram_thread_id', threadIds);

  const convMap = {}; // threadId → { uuid, customerIgId }
  for (const row of convRows || []) {
    convMap[row.instagram_thread_id] = {
      uuid: row.id,
      customerIgId: row.customer_instagram_id,
    };
  }

  // Step 1b: Recover any missing conversations — never silently drop messages
  const missingThreadIds = threadIds.filter(id => !convMap[id]);
  if (missingThreadIds.length > 0) {
    await ensureConversationRows(
      supabase,
      businessAccountId,
      missingThreadIds,
      igUserId,
      credentials?.pageToken || null,
      pageId
    );

    // Re-SELECT to pick up newly inserted rows (proper or stub)
    const { data: newRows } = await supabase
      .from('instagram_dm_conversations')
      .select('id, instagram_thread_id, customer_instagram_id')
      .in('instagram_thread_id', missingThreadIds);
    for (const row of newRows || []) {
      convMap[row.instagram_thread_id] = {
        uuid: row.id,
        customerIgId: row.customer_instagram_id,
      };
    }
  }

  // Step 2: Apply transformMessage for each message
  const allMsgRecords = [];
  for (const { conversationId, rawMessages } of batches) {
    const conv = convMap[conversationId];
    if (!conv) {
      // Should not reach here after ensureConversationRows — log if it does
      console.error(`[messaging] storeMessageBatches: ${conversationId} still missing after recovery — this is a bug`);
      continue;
    }
    for (const m of rawMessages) {
      if (!m.id) continue;
      allMsgRecords.push(transformMessage(m, conv.uuid, businessAccountId, igUserId, pageId, conv.customerIgId));
    }
  }

  if (allMsgRecords.length === 0) return { count: 0 };

  // Step 3: Single batch upsert
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

  // Step 4: Parallel orphan repair — per conversation, runs concurrently via allSettled
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

/**
 * Batch-writes conversation records in a single DB round-trip.
 * Correctly calculates the 24h messaging window from the CUSTOMER's last message only
 * (not from any last message, which could be a business reply violating Meta's window policy).
 *
 * Requires rawConversations to include messages.limit(2){created_time,from{id}} so we can
 * find the customer-sent message even when the business replied most recently.
 *
 * @param {string} businessAccountId
 * @param {Array} rawConversations - Raw API conversations array from fetchConversations()
 * @param {string} igUserId - Business IG User ID (to identify business messages)
 * @param {string|null} pageId - Facebook Page ID (defensive fallback for business ID check)
 * @returns {Promise<{count: number, conversations: Array}>} count = rows written, conversations = shaped array
 */
async function storeConversationBatches(businessAccountId, rawConversations, igUserId, pageId) {
  const supabase = getSupabaseAdmin();
  if (!supabase || !rawConversations.length) return { count: 0, conversations: [] };

  const now = new Date();
  const convRecords = [];
  const shapedConversations = [];

  for (const conv of rawConversations) {
    // Find the customer's last message — skip any sent by the business account
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

    // Identify customer participant (not the business)
    const participants = conv.participants?.data || [];
    const customerParticipant = participants.find(
      p => p.id !== igUserId && p.id !== pageId
    ) || participants[0];

    if (!customerParticipant?.id) continue; // skip conversations with no resolvable customer

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

    // Shaped output for callers (same contract as the old fetchAndStoreConversations return)
    shapedConversations.push({
      id: conv.id,
      participants,
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

  // Batch-resolve customer_user_id: if a DM sender is also a platform business account,
  // link the conversation to their user_id (FK → user_profiles). Regular Instagram users stay NULL.
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

/**
 * Batch-writes UGC records (already shaped by mapRawPostToUgcContent) in a single upsert.
 * Replaces N×upsertUgcContent with 1×upsert across all hashtags.
 *
 * @param {Array} ugcRecords - Records already shaped by mapRawPostToUgcContent
 * @returns {Promise<{count: number}>}
 */
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

module.exports = {
  axios,
  getSupabaseAdmin,
  resolveAccountCredentials,
  categorizeIgError,
  ensureMediaRecord,
  ensureConversationRows,
  syncHashtagsFromCaptions,
  mapRawPostToUgcContent,
  GRAPH_API_BASE,
  logWithDomain,
  transformMessage,
  storeCommentBatches,
  storeConversationBatches,
  storeMessageBatches,
  storeUgcContentBatch,
  parseUsageHeader,
};
