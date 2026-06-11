// publishing-kernel/substrates/engagement/worker.js
// Engagement publishing worker: one bounded IG API call per instantiation.
//
// Owns: ONE HTTP call to IG Graph API via transport.
// Does NOT own: retry, credentials, rate limiting, error classification, state.
// Error classification owned by substrates/ig-reliability-substrate.js.
//
// Contract: execute() → { success, instagram_id? }
//                         on failure { success: false, rawError, code?, httpStatus? }
// No error_category, no retryable, no classification — raw error only.
// Stateless. Created by engagement substrate (factory), destroyed after result.

const transport = require('./transport');

module.exports = class EngagementWorker {
  constructor(actionType) {
    this._actionType = actionType;
  }

  async execute(accountId, credentials, payload) {
    const { pageToken, igUserId, pageId } = credentials;

    try {
      let result;

      switch (this._actionType) {
        case 'reply_dm': {
          const res = await transport.replyDm(payload.conversation_id, pageToken, payload.message_text);
          result = { instagram_id: res.id };
          break;
        }
        case 'send_dm': {
          const res = await transport.sendDm(pageId, igUserId, pageToken, payload.recipient_id, payload.message_text);
          result = { instagram_id: res.messageId };
          break;
        }
        case 'reply_comment':
        default: {
          const res = await transport.replyComment(payload.comment_id, pageToken, payload.reply_text);
          result = { instagram_id: res.id };
        }
      }

      return { success: true, instagram_id: result.instagram_id };
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
