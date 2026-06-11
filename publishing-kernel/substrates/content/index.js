// publishing-kernel/substrates/content/index.js
// Content bounded substrate: owns worker factory, bedrock routing, execution loop.
//
// Owns: factory-creating content workers, pre-flight checks, per-item iteration,
//       delegation to PublicationStateWorker (9-state machine), outcome routing
//       via ig-reliability-substrate.analyzeFailure(). Zero internal retry.
// Does NOT own: state machine logic (PublicationStateWorker owns), IG API calls
//               (transport owns), error classification (bedrock owns),
//               rate limiting (bedrock §5 owns).
//
// Workers:
//   'posts'   → PublicationStateWorker (9-state pipeline)
//   'stories' → PublicationStateWorker (9-state pipeline)
//
// Direct execute methods (executePost, executeStory, executeRepostUgc)
// use ContentWorker for one-shot calls.

const ContentWorker = require('./worker');
const { PublicationStateWorker, createAggregate } = require('../workers/publication-state-worker');
const { resolveAccountCredentials } =
  require('../../../graph-capability-kernel/substrates/credential-resolver');
const retryStub = require('../../retry-state-transition-stub');
const igReliability = require('../../../substrates/ig-reliability-substrate');

// ── FSM reference — set by orchestator at wire time for checkpoint writes ──
let _fsm = null;
function setFsm(fsm) { _fsm = fsm; }

/**
 * Execute a batch of content publishing items.
 * Each item creates a PublicationAggregate and runs through the 9-state
 * machine (DRAFT → MEDIA_CONTAINER_CREATED → PROCESSING → READY →
 * SUBMITTED → VERIFIED → COMPLETED).
 */
async function execute(accountId, items, governance) {
  const credentials = await resolveAccountCredentials(accountId);
  for (const item of items) {
    await _executeItem(accountId, item, governance, credentials);
  }
}

// ── Per-item execution — state machine delegation ─────────────────────────

async function _executeItem(accountId, item, governance, credentials) {
  const { actionType, worker: workerName, record } = item;

  // Pre-flight: circuit breaker check
  governance.dispatch({
    type: 'CIRCUIT_BREAKER_CHECK',
    accountId,
    domain: 'content',
  });

  // Build aggregate from the post_queue record
  const aggregate = createAggregate({
    accountId,
    queueId: record.id,
    mediaUrl: record.image_url || record.video_url || record.media_url,
    caption: record.caption,
    mediaType: record.media_type,
  });

  // Run through the 9-state machine
  const worker = new PublicationStateWorker({
    governance,
    accountId,
    credentials,
    fsm: _fsm,
  });

  const result = await worker.beginPublication(aggregate);

  if (result.success) {
    return; // PUBLISHING_OBSERVATION already emitted by _transitionCompleted
  }

  // Terminal failure — classify via bedrock
  const analysis = igReliability.analyzeFailure(
    new Error(result.aggregate?.last_error || 'Publication failed'),
    actionType === 'publish_story' ? 'publish:story' : 'publish:post',
    'ig-graph',
    { accountId }
  );

  if (analysis.recommendations.includes('THROTTLE_ACCOUNT') || analysis.recommendations.includes('DEFER_NONCRITICAL_WORK')) {
    governance.dispatch({
      type: 'RATE_LIMIT_DETECTED', accountId, domain: 'content',
      cooldownMs: analysis.rateLimit?.retryAfterMs ?? 3600000,
      analysis: { category: analysis.category, severity: analysis.severity },
    });
    return;
  }

  if (analysis.recommendations.includes('REFRESH_TOKEN') || analysis.recommendations.includes('REAUTHORIZE_USER')) {
    governance.dispatch({
      type: 'AUTH_FAILURE_STRIKE', accountId,
      error: result.aggregate?.last_error || 'Authentication failure',
      analysis: { category: analysis.category, severity: analysis.severity, recommendations: analysis.recommendations },
    });
    return;
  }

  if (analysis.retryable && analysis.recommendations.includes('REQUEUE_OPERATION')) {
    retryStub.requestRetry({
      accountId, intentId: record.id || null, workerName,
      params: { actionType, worker: workerName, record },
      error: result.aggregate?.last_error || 'Publication failed',
      errorCategory: analysis.category,
      retryAfterMs: analysis.backoff?.computedMs || 0,
      analysis: { severity: analysis.severity, confidence: analysis.confidence },
    });
    return;
  }

  governance.dispatch({
    type: 'PUBLISH_FAILURE', accountId,
    domain: actionType === 'publish_story' ? 'publish:story' : 'publish:post',
    intentId: record.id || null,
    reason: result.aggregate?.last_error || 'Permanent publish failure',
    metadata: { actionType, worker: workerName, severity: analysis.severity, recommendations: analysis.recommendations },
  });
}

// ── Direct execute methods (for non-batched, non-governed calls) ──────────

const transport = require('./transport');

async function executePost(accountId, credentials, payload) {
  const { igUserId, pageToken } = credentials;
  try {
    const result = await transport.publishPost(igUserId, pageToken, payload);
    return { success: true, instagram_id: result.mediaId, creationId: result.creationId };
  } catch (error) {
    return { success: false, rawError: error, code: error?.response?.data?.error?.code ?? null };
  }
}

async function executeStory(accountId, credentials, payload) {
  const { igUserId, pageToken } = credentials;
  try {
    const result = await transport.publishStory(igUserId, pageToken, payload);
    return { success: true, instagram_id: result.mediaId, creationId: result.creationId };
  } catch (error) {
    return { success: false, rawError: error, code: error?.response?.data?.error?.code ?? null };
  }
}

async function executeRepostUgc(accountId, credentials, payload) {
  const { igUserId, pageToken } = credentials;
  try {
    const result = await transport.repostUgc(igUserId, pageToken, payload);
    return { success: true, instagram_id: result.mediaId, creationId: result.creationId };
  } catch (error) {
    return { success: false, rawError: error, code: error?.response?.data?.error?.code ?? null };
  }
}

module.exports = { execute, setFsm, executePost, executeStory, executeRepostUgc };
