// retry-cadence-kernel/workers/ig-media-container-recovery-worker.js
// IG Media Container Recovery Worker — recreates a media container
// for a failed publish. Used when the substrate emits
// RECOVER_MEDIA_CONTAINER (media container is in ERROR/EXPIRED
// state — retrying the publish call with the same container_id
// will fail; the container must be recreated).

const axios = require('axios');
const { resolveAccountCredentials } =
  require('../../graph-capability-kernel/substrates/credential-resolver');

const GRAPH_API_BASE = 'https://graph.instagram.com';

async function execute(event, governance) {
  const startTime = Date.now();
  const { accountId, intentId, analysis, params } = event || {};

  // The original publish payload is needed to recreate the container
  const originalPayload = params?.payload || analysis?.media || null;
  if (!originalPayload || !originalPayload.image_url) {
    return {
      success: false,
      workerName: 'ig-media-container-recovery-worker',
      durationMs: 0,
      error: 'missing_original_payload',
    };
  }

  let credentials;
  try {
    credentials = await resolveAccountCredentials(accountId);
  } catch (err) {
    return {
      success: false,
      workerName: 'ig-media-container-recovery-worker',
      durationMs: Date.now() - startTime,
      error: `credential_resolver_failure: ${err.message}`,
    };
  }

  // Create a new media container
  let response;
  try {
    response = await axios.post(`${GRAPH_API_BASE}/${credentials.igUserId}/media`, {
      image_url: originalPayload.image_url,
      video_url: originalPayload.video_url,
      caption: originalPayload.caption,
      media_type: originalPayload.media_type,
      is_carousel_item: originalPayload.is_carousel_item,
      access_token: credentials.accessToken,
    }, { timeout: 20000 });
  } catch (err) {
    return {
      success: false,
      workerName: 'ig-media-container-recovery-worker',
      durationMs: Date.now() - startTime,
      error: `ig_container_create_failure: ${err.message}`,
    };
  }

  const newContainerId = response.data?.id;
  if (!newContainerId) {
    return {
      success: false,
      workerName: 'ig-media-container-recovery-worker',
      durationMs: Date.now() - startTime,
      error: 'ig_container_create_returned_no_id',
    };
  }

  if (governance?.dispatchGlobal) {
    governance.dispatchGlobal({
      type: 'IG_MEDIA_CONTAINER_RECOVERED',
      accountId, intentId,
      oldContainerId: originalPayload.id,
      newContainerId,
      durationMs: Date.now() - startTime,
    });
  }

  return {
    success: true,
    workerName: 'ig-media-container-recovery-worker',
    durationMs: Date.now() - startTime,
    error: null,
    data: {
      oldContainerId: originalPayload.id,
      newContainerId,
      recoveryCompleted: true,
    },
  };
}

module.exports = {
  name: 'ig-media-container-recovery-worker',
  execute,
};
