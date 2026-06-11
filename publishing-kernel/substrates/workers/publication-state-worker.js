// publishing-kernel/substrates/workers/publication-state-worker.js
// Publication State Worker — deterministic executor for content publishing.
//
// Owns: state machine EXECUTION — transport calls, checkpoint persistence,
//       aggregate tracking. Reports to FSM for all governance decisions.
// Does NOT own: stall detection (FSM), timeout routing (FSM), verification
//               retry counting (FSM), recovery routing (FSM), error
//               classification (bedrock), credential resolution (GC kernel).
//
// Architecture:
//   FSM (fsm.js) = governance membrane — owns policy, stall counting,
//                  timeout detection, verification retry decisions.
//   Worker = bounded executor — reports facts via governance.dispatch(),
//            receives FSM decisions via FSM-emitted actions.
//
// States: DRAFT → MEDIA_CONTAINER_CREATED → MEDIA_PROCESSING → MEDIA_READY
//         → PUBLICATION_SUBMITTED → PUBLICATION_VERIFIED → COMPLETED
//         FAILED, RECOVERING

const crypto = require('crypto');
const axios = require('axios');
const transport = require('../content/transport');
const policy = require('../../publication-policy');
const igReliability = require('../../../substrates/ig-reliability-substrate');

// ═══════════════════════════════════════════════════════════════════════════════
// PUBLICATION AGGREGATE
// ═══════════════════════════════════════════════════════════════════════════════

class PublicationAggregate {
  constructor({ publicationId, accountId, queueId, platform, mediaUrl, caption, mediaType }) {
    this.publication_id = publicationId || crypto.randomUUID();
    this.platform = platform || 'instagram';
    this.account_id = accountId;
    this.queue_id = queueId || null;
    this.media_url = mediaUrl || null;
    this.caption = caption || null;
    this.media_type = mediaType || 'IMAGE';
    this.current_state = 'DRAFT';
    this.previous_state = null;
    this.retry_count = 0;
    this.container_id = null;
    this.published_media_id = null;
    this.verification_status = 'PENDING';
    this.transition_history = [];
    this.last_error = null;
    this.checkpoint_metadata = {};
    this.payload_fingerprint = this._computeFingerprint();
  }

  _computeFingerprint() {
    return crypto.createHash('sha256')
      .update([this.account_id, this.media_url, this.caption, this.media_type].join('|'))
      .digest('hex').slice(0, 32);
  }

  transition(to, cause) {
    this.previous_state = this.current_state;
    this.current_state = to;
    this.transition_history.push({ from: this.previous_state, to, timestamp: new Date().toISOString(), cause });
  }

  toCheckpointRow() {
    return {
      publication_id: this.publication_id, queue_id: this.queue_id, account_id: this.account_id,
      current_state: this.current_state, container_id: this.container_id,
      published_media_id: this.published_media_id, verification_status: this.verification_status,
      retry_count: this.retry_count, last_error: this.last_error, checkpoint_metadata: this.checkpoint_metadata,
    };
  }

  _rehydrateFromCheckpoint() {
    return new PublicationAggregate({
      publicationId: this.publication_id, accountId: this.account_id, queueId: this.queue_id,
      mediaUrl: this.media_url, caption: this.caption, mediaType: this.media_type,
    });
  }
}

function createAggregate({ accountId, queueId, mediaUrl, caption, mediaType }) {
  return new PublicationAggregate({ accountId, queueId, mediaUrl, caption, mediaType });
}

// ═══════════════════════════════════════════════════════════════════════════════
// PUBLICATION STATE WORKER — EXECUTOR ONLY
// ═══════════════════════════════════════════════════════════════════════════════

class PublicationStateWorker {
  constructor({ governance, accountId, credentials, fsm }) {
    this._governance = governance;
    this._accountId = accountId;
    this._credentials = credentials;
    this._fsm = fsm;
    const { GRAPH_API_BASE: g } = require('../../../config/instagram');
    this._GRAPH_API_BASE = g;
  }

  // ── Public entry point ──────────────────────────────────────────────────

  async beginPublication(aggregate) {
    const { igUserId, pageToken } = this._credentials;
    try {
      aggregate = await this._transitionDraft(aggregate);
      aggregate = await this._transitionMediaCreated(aggregate, igUserId, pageToken);
      aggregate = await this._transitionMediaProcessing(aggregate, pageToken);
      aggregate = await this._transitionMediaReady(aggregate);
      aggregate = await this._transitionPublicationSubmitted(aggregate, igUserId, pageToken);
      aggregate = await this._transitionPublicationVerified(aggregate, pageToken);
      aggregate = await this._transitionCompleted(aggregate);
      return { success: true, aggregate };
    } catch (err) {
      if (aggregate.current_state === 'RECOVERING') {
        return this._transitionRecovering(aggregate, igUserId, pageToken);
      }
      return this._transitionFailed(aggregate, err);
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // STATES — EXECUTION ONLY (FSM owns decisions)
  // ═══════════════════════════════════════════════════════════════════════════

  async _transitionDraft(aggregate) {
    aggregate.transition('DRAFT', 'publication_begun');
    await this._checkpoint(aggregate);
    return aggregate;
  }

  async _transitionMediaCreated(aggregate, igUserId, pageToken) {
    aggregate.transition('MEDIA_CONTAINER_CREATED', 'creating_container');
    try {
      const payload = {
        caption: aggregate.caption,
        image_url: aggregate.media_type === 'IMAGE' || aggregate.media_type === 'CAROUSEL_ALBUM' ? aggregate.media_url : undefined,
        video_url: aggregate.media_type === 'VIDEO' || aggregate.media_type === 'REELS' ? aggregate.media_url : undefined,
        media_type: aggregate.media_type,
      };
      const result = await transport.createMediaContainer(igUserId, pageToken, payload);
      aggregate.container_id = result.creationId;
      aggregate.checkpoint_metadata.creationId = result.creationId;
      await this._checkpoint(aggregate);
      return aggregate;
    } catch (error) {
      const analysis = igReliability.analyzeFailure(error, 'publish:post', 'ig-graph', { accountId: aggregate.account_id });
      if (!analysis.retryable) throw error;
      aggregate.retry_count++;
      await this._checkpoint(aggregate);
      return aggregate;
    }
  }

  async _transitionMediaProcessing(aggregate, pageToken) {
    aggregate.transition('MEDIA_PROCESSING', 'polling_container');
    const { processingPollIntervalMs, processingMaxAttempts } = policy;

    for (let attempt = 1; attempt <= processingMaxAttempts; attempt++) {
      try {
        const { data } = await axios.get(`${this._GRAPH_API_BASE}/${aggregate.container_id}`, {
          params: { fields: 'status_code,status', access_token: pageToken },
          timeout: 10000,
        });
        const statusCode = data?.status_code || data?.status;

        // REPORT to FSM — FSM decides STALL or TIMEOUT
        this._governance.dispatch({
          type: 'PUBLICATION_POLL_RESULT',
          accountId: aggregate.account_id,
          statusCode,
          attempt,
          publicationId: aggregate.publication_id,
          creationId: aggregate.container_id,
        });

        if (statusCode === 'FINISHED') {
          aggregate.checkpoint_metadata.pollAttempts = attempt;
          await this._checkpoint(aggregate);
          return aggregate;
        }

        if (['ERROR', 'EXPIRED'].includes(statusCode)) {
          throw new Error(`Media container ${statusCode.toLowerCase()}: ${aggregate.container_id}`);
        }

        await new Promise(resolve => setTimeout(resolve, processingPollIntervalMs));
      } catch (error) {
        const analysis = igReliability.analyzeFailure(error, 'publish:post', 'ig-graph', { accountId: aggregate.account_id });
        if (!analysis.retryable) throw error;
      }
    }

    // Exhausted — emit TIMEOUT through governance
    this._governance.dispatch({
      type: 'PUBLICATION_TIMEOUT',
      accountId: aggregate.account_id,
      publicationId: aggregate.publication_id,
      creationId: aggregate.container_id,
      elapsedMs: processingMaxAttempts * processingPollIntervalMs,
      lastKnownState: 'poll_exhausted',
    });
    throw new Error(`Media processing timed out after ${processingMaxAttempts} attempts`);
  }

  async _transitionMediaReady(aggregate) {
    aggregate.transition('MEDIA_READY', 'container_ready');
    await this._checkpoint(aggregate);
    return aggregate;
  }

  async _transitionPublicationSubmitted(aggregate, igUserId, pageToken) {
    aggregate.transition('PUBLICATION_SUBMITTED', 'publishing');
    if (aggregate.published_media_id) return aggregate;
    try {
      const result = await transport.pollAndPublish(igUserId, pageToken, aggregate.container_id, aggregate.media_type);
      aggregate.published_media_id = result.mediaId;
      await this._checkpoint(aggregate);
      return aggregate;
    } catch (error) {
      const analysis = igReliability.analyzeFailure(error, 'publish:post', 'ig-graph', { accountId: aggregate.account_id, containerId: aggregate.container_id });
      if (!analysis.retryable) throw error;
      aggregate.retry_count++;
      await this._checkpoint(aggregate);
      return aggregate;
    }
  }

  async _transitionPublicationVerified(aggregate, pageToken) {
    aggregate.transition('PUBLICATION_VERIFIED', 'verifying');
    aggregate.verification_status = 'VERIFYING';
    const { verificationAttempts, verificationIntervalMs, verificationTimeoutMs } = policy;

    for (let attempt = 1; attempt <= verificationAttempts; attempt++) {
      try {
        const { status, data } = await axios.get(`${this._GRAPH_API_BASE}/${aggregate.published_media_id}`, {
          params: { fields: 'id,timestamp,permalink', access_token: pageToken },
          timeout: verificationTimeoutMs,
        });

        const checksPassed = status === 200 && data?.id != null && data?.timestamp != null && data?.permalink != null;
        const missing = [];
        if (status !== 200) missing.push('status');
        if (!data?.id) missing.push('id');
        if (!data?.timestamp) missing.push('timestamp');
        if (!data?.permalink) missing.push('permalink');

        // REPORT to FSM — FSM owns retry counting
        this._governance.dispatch({
          type: 'PUBLICATION_VERIFY_RESULT',
          accountId: aggregate.account_id,
          checksPassed,
          attempt,
          publicationId: aggregate.publication_id,
          missingChecks: missing,
        });

        if (checksPassed) {
          aggregate.verification_status = 'VERIFIED';
          aggregate.checkpoint_metadata.verifiedAt = new Date().toISOString();
          await this._checkpoint(aggregate);
          return aggregate;
        }
      } catch (error) {
        this._governance.dispatch({
          type: 'PUBLICATION_VERIFY_RESULT',
          accountId: aggregate.account_id,
          checksPassed: false,
          attempt,
          publicationId: aggregate.publication_id,
          missingChecks: [error.message],
        });
      }

      if (attempt < verificationAttempts) {
        await new Promise(resolve => setTimeout(resolve, verificationIntervalMs));
      }
    }

    throw new Error('Verification exhausted');
  }

  async _transitionCompleted(aggregate) {
    aggregate.transition('COMPLETED', 'publication_complete');
    if (this._fsm && typeof this._fsm.requestDBWrite === 'function') {
      this._fsm.requestDBWrite({
        table: 'post_queue', operation: 'update_status', accountId: aggregate.account_id,
        rows: [{ id: aggregate.queue_id, status: 'published', container_id: aggregate.container_id, published_media_id: aggregate.published_media_id }],
      });
    }
    await this._checkpoint(aggregate);
    this._governance.dispatch({
      type: 'PUBLISHING_OBSERVATION', status: 'ok', accountId: aggregate.account_id,
      metadata: { publication_id: aggregate.publication_id, instagram_id: aggregate.published_media_id, creationId: aggregate.container_id, domain: 'content' },
    });
    return { success: true, aggregate };
  }

  async _transitionFailed(aggregate, error) {
    aggregate.transition('FAILED', error?.message || 'unknown_error');
    aggregate.last_error = error?.message || null;
    await this._checkpoint(aggregate);
    this._governance.dispatch({
      type: 'PUBLISHING_OBSERVATION', status: 'error', accountId: aggregate.account_id, error: aggregate.last_error,
      metadata: { publication_id: aggregate.publication_id, current_state: aggregate.current_state, container_id: aggregate.container_id, domain: 'content' },
    });
    return { success: false, aggregate };
  }

  async _transitionRecovering(aggregate, igUserId, pageToken) {
    const { recoveryAttempts } = policy;
    for (let attempt = 1; attempt <= recoveryAttempts; attempt++) {
      aggregate.transition('RECOVERING', `recovery_attempt_${attempt}`);
      aggregate.retry_count++;
      await this._checkpoint(aggregate);
      try {
        if (!aggregate.container_id) return this.beginPublication(aggregate._rehydrateFromCheckpoint());
        if (!aggregate.published_media_id) return this._transitionMediaProcessing(aggregate, pageToken);
        return this._transitionPublicationVerified(aggregate, pageToken);
      } catch (err) {
        if (attempt >= recoveryAttempts) return this._transitionFailed(aggregate, err);
      }
    }
    return this._transitionFailed(aggregate, new Error('Recovery exhausted'));
  }

  async _checkpoint(aggregate) {
    if (!this._fsm || typeof this._fsm.requestDBWrite !== 'function') return;
    this._fsm.requestDBWrite({ table: 'publication_state', operation: 'upsert_checkpoint', accountId: aggregate.account_id, rows: [aggregate.toCheckpointRow()] });
  }

  static async recover(queueId, { governance, fsm }) {
    if (!governance || typeof governance.governedRead !== 'function') return null;
    const result = await governance.governedRead('db.publication-state', { query: 'getByQueueId', queueId });
    if (!result.success || !result.data) return null;
    const row = result.data;
    const agg = new PublicationAggregate({ publicationId: row.publication_id, accountId: row.account_id, queueId: row.queue_id, mediaUrl: null, caption: null, mediaType: null });
    agg.current_state = row.current_state || 'DRAFT';
    agg.container_id = row.container_id || null;
    agg.published_media_id = row.published_media_id || null;
    agg.verification_status = row.verification_status || 'PENDING';
    agg.retry_count = row.retry_count || 0;
    agg.last_error = row.last_error || null;
    agg.checkpoint_metadata = row.checkpoint_metadata || {};
    return agg;
  }
}

module.exports = { PublicationStateWorker, createAggregate, PublicationAggregate };
