// publishing-kernel/substrates/content/transport.js
// Content publishing transport: pure Instagram Graph API write operations.
//
// Owns: post creation (IMAGE/CAROUSEL_ALBUM), story creation (VIDEO/REELS),
//       UGC repost, container polling.
// Does NOT own: DB writes, credential resolution, retry logic, rate-limiting.
//
// Migrated from substrates/transport/publishing.js — content-specific actions only.
// Shared container helpers (createMediaContainer, pollAndPublish, pollContainerUntilFinished)
// also imported by engagement/transport.js for any engagement-side media actions.

const axios = require('axios');
const { GRAPH_API_BASE } = require('../../../config/instagram');

// ═══════════════════════════════════════════════════════════════════════════════
// MEDIA CONTAINER (shared)
// ═══════════════════════════════════════════════════════════════════════════════

async function createMediaContainer(igUserId, pageToken, payload) {
  const { caption, image_url, video_url, media_type } = payload;
  const type = (media_type || 'IMAGE').toUpperCase();

  const createParams = { caption, access_token: pageToken };
  if (type === 'VIDEO' || type === 'REELS') {
    createParams.video_url = video_url;
    createParams.media_type = type;
  } else if (type === 'CAROUSEL_ALBUM') {
    createParams.image_url = image_url;
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

    await new Promise(resolve => setTimeout(resolve, intervalMs));
  }

  throw new Error(`Media container polling timed out after ${maxAttempts} attempts: ${creationId}`);
}

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

// ═══════════════════════════════════════════════════════════════════════════════
// PUBLISH POST
// ═══════════════════════════════════════════════════════════════════════════════

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

  const { mediaId } = await pollAndPublish(igUserId, pageToken, creationId, payload.media_type || 'IMAGE');
  return { success: true, mediaId, creationId };
}

// ═══════════════════════════════════════════════════════════════════════════════
// PUBLISH STORY
// ═══════════════════════════════════════════════════════════════════════════════

async function publishStory(igUserId, pageToken, payload) {
  let creationId = payload.creation_id;

  if (!creationId) {
    const storyType = payload.media_type || 'REELS';
    const container = await createMediaContainer(igUserId, pageToken, {
      caption: payload.caption || '',
      video_url: payload.video_url,
      image_url: payload.image_url,
      media_type: storyType,
    });
    creationId = container.creationId;
  }

  const { mediaId } = await pollAndPublish(igUserId, pageToken, creationId, payload.media_type || 'REELS');
  return { success: true, mediaId, creationId };
}

// ═══════════════════════════════════════════════════════════════════════════════
// REPOST UGC
// ═══════════════════════════════════════════════════════════════════════════════

async function repostUgc(igUserId, pageToken, payload) {
  let creationId = payload.creation_id;

  if (!creationId) {
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

  const { mediaId } = await pollAndPublish(igUserId, pageToken, creationId, payload.media_type || 'IMAGE');
  return { success: true, mediaId, creationId };
}

module.exports = {
  createMediaContainer,
  pollContainerUntilFinished,
  pollAndPublish,
  publishPost,
  publishStory,
  repostUgc,
};
