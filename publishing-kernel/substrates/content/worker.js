// publishing-kernel/substrates/content/worker.js
// Content publishing worker: one bounded IG API call per instantiation.
//
// Owns: ONE HTTP call to IG Graph API via transport.
// Does NOT own: retry, credentials, rate limiting, error classification, state.
//
// Contract: execute() → Promise<{ success, instagram_id?, error?, error_category?, retryable? }>
// Stateless. Created by content substrate (factory), destroyed after result.

const transport = require('./transport');
const { suspectIgCategory } = require('../../../substrates/transport/error-classifier');

module.exports = class ContentWorker {
  /**
   * @param {string} actionType — 'publish_post' | 'repost_ugc' | 'publish_story'
   */
  constructor(actionType) {
    this._actionType = actionType;
  }

  /**
   * Execute one bounded IG Graph API call.
   * @param {string} accountId
   * @param {{ igUserId: string, pageToken: string }} credentials
   * @param {object} payload — { image_url, video_url, caption, media_type, creation_id, ... }
   * @returns {Promise<{ success: boolean, instagram_id?: string, creationId?: string,
   *                     error?: string, error_category?: string, retryable?: boolean }>}
   */
  async execute(accountId, credentials, payload) {
    const { igUserId, pageToken } = credentials;

    try {
      let result;

      switch (this._actionType) {
        case 'repost_ugc':
          result = await transport.repostUgc(igUserId, pageToken, payload);
          break;
        case 'publish_story':
          result = await transport.publishStory(igUserId, pageToken, payload);
          break;
        case 'publish_post':
        default:
          result = await transport.publishPost(igUserId, pageToken, payload);
      }

      return {
        success: true,
        instagram_id: result.mediaId,
        creationId: result.creationId,
      };
    } catch (error) {
      const msg = error.response?.data?.error?.message || error.message;
      // Phase 2: emit raw error + cheap hint. The substrate is the
      // canonical classifier — see IG_FAILURE_OBSERVED in
      // retry-cadence-kernel/fsm.js for the constitutional path.
      const suspectedCategory = suspectIgCategory(error);
      return {
        success: false,
        error: msg,
        code: error.response?.data?.error?.code || null,
        suspectedCategory,  // cheap hint, not a classification
        rawError: error,    // raw axios error — substrate's analyzeFailure consumes this
        // legacy fields preserved for back-compat; the substrate
        // is the canonical authority.
        error_category: suspectedCategory,
        retryable: null,
        retry_after_seconds: null,
      };
    }
  }
};
