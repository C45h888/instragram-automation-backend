// substrates/transport/publishing.js
// Bounded substrate: pure Instagram Graph API write operations.
//
// Owns: making HTTP calls to the Instagram Graph API for write actions.
//        No persistence, no queue management, no retry logic.
// Does NOT own: database writes, schema normalization, retry decisions, orchestration.
//
// Action types handled:
//   publish_post  — 2-step: create container → poll → publish
//   repost_ugc    — same 2-step pattern for UGC reposting
//   reply_comment — POST /{comment_id}/replies
//   reply_dm      — POST /{conversation_id}/messages
//   send_dm       — POST /{page-id}/messages (new thread)

const axios = require('axios');
const { categorizeIgError, GRAPH_API_BASE } = require('../../helpers/agent-helpers');
const { logWithDomain } = require('../telemetry');

// ═══════════════════════════════════════════════════════════════════════════════
// MEDIA CONTAINER (shared by publish_post and repost_ugc)
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Creates a media container (Step 1 of 2-step publish).
 * @param {string} igUserId
 * @param {string} pageToken
 * @param {object} payload — { caption, image_url|video_url, media_type }
 * @returns {Promise<{creationId: string}>}
 */
async function createMediaContainer(igUserId, pageToken, payload) {
  const { caption, image_url, video_url, media_type } = payload;
  const type = (media_type || 'IMAGE').toUpperCase();

  const createParams = { caption, access_token: pageToken };
  if (type === 'VIDEO' || type === 'REELS') {
    createParams.video_url = video_url;
    createParams.media_type = type;
  } else {
    createParams.image_url = image_url;
  }

  const res = await axios.post(`${GRAPH_API_BASE}/${igUserId}/media`, null, {
    params: createParams,
    timeout: 15000,
  });

  const creationId = res.data.id;
  if (!creationId) throw new Error('Failed to create media container');
  return { creationId };
}

/**
 * Polls a media container until status is FINISHED, then publishes it.
 * Used for VIDEO/REELS which require polling before publish.
 *
 * @param {string} igUserId
 * @param {string} pageToken
 * @param {string} creationId
 * @param {string} mediaType — 'IMAGE' | 'VIDEO' | 'REELS'
 * @returns {Promise<{mediaId: string}>}
 */
async function pollAndPublish(igUserId, pageToken, creationId, mediaType) {
  const type = (mediaType || 'IMAGE').toUpperCase();

  if (type === 'VIDEO' || type === 'REELS') {
    await pollContainerUntilFinished(creationId, pageToken);
  }

  const publishRes = await axios.post(`${GRAPH_API_BASE}/${igUserId}/media_publish`, null, {
    params: { creation_id: creationId, access_token: pageToken },
    timeout: 15000,
  });

  const mediaId = publishRes.data.id;
  if (!mediaId) throw new Error('Failed to publish media container');
  return { mediaId };
}

/**
 * Polls container status every 10s for up to 2 minutes.
 * @param {string} creationId
 * @param {string} pageToken
 * @throws {Error} if status is ERROR/EXPIRED or max attempts exceeded
 */
async function pollContainerUntilFinished(creationId, pageToken, opts = {}) {
  const { maxAttempts = 12, intervalMs = 10000 } = opts;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const { data } = await axios.get(`${GRAPH_API_BASE}/${creationId}`, {
      params: { fields: 'status_code,status', access_token: pageToken },
      timeout: 10000,
    });

    const statusCode = data?.status_code || data?.status;

    if (statusCode === 'FINISHED') return;

    if (['ERROR', 'EXPIRED'].includes(statusCode)) {
      throw new Error(`Media container ${statusCode.toLowerCase()}: ${creationId}`);
    }

    // IN_PROGRESS, PENDING — keep polling
    await new Promise(resolve => setTimeout(resolve, intervalMs));
  }

  throw new Error(`Media container polling timed out after ${maxAttempts} attempts: ${creationId}`);
}

// ═══════════════════════════════════════════════════════════════════════════════
// PUBLISH POST
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Publishes a scheduled post to Instagram (2-step: container + publish).
 *
 * @param {string} igUserId
 * @param {string} pageToken
 * @param {object} payload — {
 *   image_url, caption, media_type,
 *   creation_id?,   // for retries: skip container creation
 *   scheduled_post_id?
 * }
 * @returns {Promise<{mediaId: string, creationId?: string, success: true}>}
 */
async function publishPost(igUserId, pageToken, payload) {
  let creationId = payload.creation_id;

  if (!creationId) {
    const container = await createMediaContainer(igUserId, pageToken, {
      caption: payload.caption,
      image_url: payload.image_url,
      video_url: payload.video_url,
      media_type: payload.media_type,
    });
    creationId = container.creationId;
  }

  const mediaId = await pollAndPublish(
    igUserId, pageToken, creationId,
    payload.media_type || 'IMAGE'
  );

  return { success: true, mediaId, creationId };
}

// ═══════════════════════════════════════════════════════════════════════════════
// REPOST UGC
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Reposts a piece of UGC content after permission is granted.
 * Receives pre-resolved media_url, caption, and media_type from control plane.
 * No database access — transport is a pure Instagram API write substrate.
 *
 * @param {string} igUserId
 * @param {string} pageToken
 * @param {object} payload — { permission_id, creation_id?, media_url, caption, media_type }
 * @returns {Promise<{mediaId: string, creationId?: string, success: true}>}
 */
async function repostUgc(igUserId, pageToken, payload) {
  let creationId = payload.creation_id;

  if (!creationId) {
    // Payload already contains pre-resolved media_url, caption, media_type
    // resolved by control-plane via dbWorker.resolveUgcContent()
    const { media_url, caption, media_type } = payload;
    if (!media_url) throw new Error('UGC media URL not found');

    const ugcMediaType = media_type || 'IMAGE';

    const container = await createMediaContainer(igUserId, pageToken, {
      caption: caption || '',
      image_url: ugcMediaType === 'IMAGE' || ugcMediaType === 'CAROUSEL' ? media_url : undefined,
      video_url: ugcMediaType === 'VIDEO' || ugcMediaType === 'REELS' ? media_url : undefined,
      media_type: ugcMediaType,
    });
    creationId = container.creationId;
  }

  const mediaId = await pollAndPublish(
    igUserId, pageToken, creationId,
    payload.media_type || 'IMAGE'
  );

  return { success: true, mediaId, creationId };
}

// ═══════════════════════════════════════════════════════════════════════════════
// REPLY COMMENT
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Posts a reply to an Instagram comment.
 *
 * @param {string} commentId — Instagram comment ID
 * @param {string} pageToken
 * @param {string} replyText — max 2200 chars
 * @returns {Promise<{id: string, success: true}>}
 */
async function replyComment(commentId, pageToken, replyText) {
  const res = await axios.post(`${GRAPH_API_BASE}/${commentId}/replies`, null, {
    params: { message: replyText.trim(), access_token: pageToken },
    timeout: 10000,
  });

  return { success: true, id: res.data.id };
}

// ═══════════════════════════════════════════════════════════════════════════════
// REPLY DM
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Sends a reply message into an existing DM conversation.
 *
 * @param {string} conversationId — Instagram thread ID
 * @param {string} pageToken
 * @param {string} messageText — max 1000 chars
 * @returns {Promise<{id: string, success: true}>}
 */
async function replyDm(conversationId, pageToken, messageText) {
  const res = await axios.post(`${GRAPH_API_BASE}/${conversationId}/messages`, null, {
    params: { message: messageText.trim(), access_token: pageToken },
    timeout: 10000,
  });

  return { success: true, id: res.data.id };
}

// ═══════════════════════════════════════════════════════════════════════════════
// SEND DM (new thread)
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Initiates a new DM thread to a user.
 *
 * @param {string} pageId — Facebook Page ID (preferred node for /messages)
 * @param {string} igUserId — fallback if pageId absent
 * @param {string} pageToken
 * @param {string} recipientId — numeric IGSID of recipient
 * @param {string} messageText — max 1000 chars
 * @returns {Promise<{messageId: string, success: true}>}
 */
async function sendDm(pageId, igUserId, pageToken, recipientId, messageText) {
  const node = pageId || igUserId;
  const res = await axios.post(`${GRAPH_API_BASE}/${node}/messages`, {
    recipient: { id: String(recipientId) },
    message: { text: messageText.trim() },
  }, {
    params: { access_token: pageToken },
    timeout: 10000,
  });

  return { success: true, messageId: res.data.message_id || res.data.id };
}

// ═══════════════════════════════════════════════════════════════════════════════
// DISPATCHER
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Routes an action to the correct handler.
 * Called by publish-worker._execute().
 *
 * @param {string} actionType — 'publish_post'|'repost_ugc'|'reply_comment'|'reply_dm'|'send_dm'
 * @param {string} accountId
 * @param {object} credentials — { igUserId, pageToken, pageId }
 * @param {object} payload — action-specific payload
 * @returns {Promise<object>} — { success, instagram_id?, error?, retryable?, error_category? }
 */
async function executeAction(actionType, accountId, credentials, payload) {
  try {
    const { igUserId, pageToken, pageId } = credentials;

    switch (actionType) {

      case 'publish_post': {
        const result = await publishPost(igUserId, pageToken, payload);
        await logWithDomain('publish', {
          endpoint: '/publish-post', method: 'POST',
          business_account_id: accountId,
          success: true,
          latency: 0,
          details: { action_type: 'publish_post', media_id: result.mediaId },
        });
        return { success: true, instagram_id: result.mediaId, creationId: result.creationId };
      }

      case 'repost_ugc': {
        const result = await repostUgc(igUserId, pageToken, payload);
        await logWithDomain('publish', {
          endpoint: '/repost-ugc', method: 'POST',
          business_account_id: accountId,
          success: true,
          latency: 0,
          details: { action_type: 'repost_ugc', media_id: result.mediaId },
        });
        return { success: true, instagram_id: result.mediaId, creationId: result.creationId };
      }

      case 'reply_comment': {
        const result = await replyComment(payload.comment_id, pageToken, payload.reply_text);
        await logWithDomain('publish', {
          endpoint: '/reply-comment', method: 'POST',
          business_account_id: accountId,
          success: true,
          latency: 0,
          details: { action_type: 'reply_comment', comment_id: payload.comment_id },
        });
        return { success: true, instagram_id: result.id };
      }

      case 'reply_dm': {
        const result = await replyDm(payload.conversation_id, pageToken, payload.message_text);
        await logWithDomain('publish', {
          endpoint: '/reply-dm', method: 'POST',
          business_account_id: accountId,
          success: true,
          latency: 0,
          details: { action_type: 'reply_dm', conversation_id: payload.conversation_id },
        });
        return { success: true, instagram_id: result.id };
      }

      case 'send_dm': {
        const result = await sendDm(pageId, igUserId, pageToken, payload.recipient_id, payload.message_text);
        await logWithDomain('publish', {
          endpoint: '/send-dm', method: 'POST',
          business_account_id: accountId,
          success: true,
          latency: 0,
          details: { action_type: 'send_dm', recipient_id: payload.recipient_id },
        });
        return { success: true, instagram_id: result.messageId };
      }

      default:
        return { success: false, error: `Unknown action_type: ${actionType}`, retryable: false };
    }
  } catch (error) {
    const errorMessage = error.response?.data?.error?.message || error.message;
    const { retryable, error_category, retry_after_seconds } = categorizeIgError(error);
    return { success: false, error: errorMessage, retryable, error_category, retry_after_seconds };
  }
}

// ── Exports ──────────────────────────────────────────────────────────────────

module.exports = {
  executeAction,
  publishPost,
  repostUgc,
  replyComment,
  replyDm,
  sendDm,
  pollContainerUntilFinished,
};
