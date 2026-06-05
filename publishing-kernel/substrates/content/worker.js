// publishing-kernel/substrates/content/worker.js
// Content publishing worker: one bounded IG API call per instantiation.
//
// Owns: ONE HTTP call to IG Graph API via transport.
// Does NOT own: retry, credentials, rate limiting, error classification, state.
//
// Contract: execute() → Promise<{ success, instagram_id?, error?, error_category?, retryable? }>
// Stateless. Created by content substrate (factory), destroyed after result.

const transport = require('./transport');
const { categorizeIgError } = require('../../../helpers/agent-helpers');

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
      const { retryable, error_category, retry_after_seconds } = categorizeIgError(error);
      return {
        success: false,
        error: msg,
        error_category,
        retryable,
        retry_after_seconds,
      };
    }
  }
};
