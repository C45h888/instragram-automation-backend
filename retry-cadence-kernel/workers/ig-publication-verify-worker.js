// retry-cadence-kernel/workers/ig-publication-verify-worker.js
// IG Publication Verify Worker — fetches a media container to
// confirm its publication state. Used when the substrate emits
// VERIFY_PUBLICATION (PENDING/TIMEOUT media state).

const axios = require('axios');
const { resolveAccountCredentials } =
  require('../../graph-capability-kernel/substrates/credential-resolver');

const GRAPH_API_BASE = 'https://graph.instagram.com';

async function execute(event, governance) {
  const startTime = Date.now();
  const { accountId, intentId, analysis, containerId, publicationId } = event || {};

  const id = publicationId || containerId || analysis?.media?.publicationId || analysis?.media?.containerId;
  if (!id) {
    return {
      success: false,
      workerName: 'ig-publication-verify-worker',
      durationMs: 0,
      error: 'missing_publication_id',
    };
  }

  let credentials;
  try {
    credentials = await resolveAccountCredentials(accountId);
  } catch (err) {
    return {
      success: false,
      workerName: 'ig-publication-verify-worker',
      durationMs: Date.now() - startTime,
      error: `credential_resolver_failure: ${err.message}`,
    };
  }

  let response;
  try {
    response = await axios.get(`${GRAPH_API_BASE}/${id}`, {
      params: {
        fields: 'id,status_code,status,media_type,media_url,permalink,timestamp',
        access_token: credentials.accessToken,
      },
      timeout: 15000,
    });
  } catch (err) {
    return {
      success: false,
      workerName: 'ig-publication-verify-worker',
      durationMs: Date.now() - startTime,
      error: `ig_verify_endpoint_failure: ${err.message}`,
    };
  }

  const media = response.data || {};
  const statusCode = media.status_code || media.status;
  const isPublished = statusCode === 'PUBLISHED' || statusCode === 'FINISHED';

  if (governance?.dispatchGlobal) {
    governance.dispatchGlobal({
      type: 'IG_PUBLICATION_VERIFY_COMPLETE',
      accountId, intentId,
      publicationId: id,
      statusCode,
      isPublished,
      durationMs: Date.now() - startTime,
    });
  }

  return {
    success: isPublished,
    workerName: 'ig-publication-verify-worker',
    durationMs: Date.now() - startTime,
    error: isPublished ? null : `publication_not_live: ${statusCode}`,
    data: { publicationId: id, statusCode, isPublished, media },
  };
}

module.exports = {
  name: 'ig-publication-verify-worker',
  execute,
};
