// publishing-kernel/substrates/engagement/worker.js
// Engagement publishing worker: one bounded IG API call per instantiation.
//
// Owns: ONE HTTP call to IG Graph API via transport.
// Does NOT own: retry, credentials, rate limiting, error classification, state.
//
// Contract: execute() → Promise<{ success, instagram_id?, error?, error_category?, retryable? }>
// Stateless. Created by engagement substrate (factory), destroyed after result.

const transport = require('./transport');
const { suspectIgCategory } = require('../../../substrates/transport/error-classifier');

module.exports = class EngagementWorker {
  /**
   * @param {string} actionType — 'reply_comment' | 'reply_dm' | 'send_dm'
   */
  constructor(actionType) {
    this._actionType = actionType;
  }

  /**
   * Execute one bounded IG Graph API call.
   * @param {string} accountId
   * @param {{ igUserId: string, pageToken: string, pageId?: string }} credentials
   * @param {object} payload — { comment_id, reply_text, conversation_id, message_text, recipient_id }
   * @returns {Promise<{ success: boolean, instagram_id?: string,
   *                     error?: string, error_category?: string, retryable?: boolean }>}
   */
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
