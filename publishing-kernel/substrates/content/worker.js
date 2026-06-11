// publishing-kernel/substrates/content/worker.js
// Content publishing worker: one bounded IG API call per instantiation.
//
// Owns: ONE HTTP call to IG Graph API via transport.
// Does NOT own: retry, credentials, rate limiting, error classification, state.
// Error classification owned by substrates/ig-reliability-substrate.js.
//
// Contract: execute() → { success, instagram_id?, creationId? }
//                         on failure { success: false, rawError, code?, httpStatus? }
// No error_category, no retryable, no classification — raw error only.
// Stateless. Created by content substrate (factory), destroyed after result.

const transport = require('./transport');

module.exports = class ContentWorker {
  constructor(actionType) {
    this._actionType = actionType;
  }

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
        instagram_id: result.mediaId || null,
        creationId: result.creationId || null,
      };
    } catch (error) {
      // Raw error pass-through — bedrock classifies.
      return {
        success: false,
        rawError: error,
        code: error?.response?.data?.error?.code ?? error?.code ?? null,
        httpStatus: error?.response?.status ?? null,
        retryAfterHeader: error?.response?.headers?.['retry-after'] ?? null,
      };
    }
  }
};
