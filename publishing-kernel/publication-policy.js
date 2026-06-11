// publishing-kernel/publication-policy.js
// Publication operational policy constants. Zero logic.
// Owned by the governance membrane (fsm.js) — imported there, re-exported.
// Workers import this file for operational constants.

module.exports = {
  // MEDIA_CONTAINER_CREATED
  containerCreateTimeoutMs: 15000,

  // MEDIA_PROCESSING — container polling
  processingPollIntervalMs: 10000,
  processingMaxAttempts: 12,
  processingStallThreshold: 5,

  // PUBLICATION_SUBMITTED — publish call
  publishTimeoutMs: 15000,

  // PUBLICATION_VERIFIED — post-publish verification
  verificationAttempts: 3,
  verificationIntervalMs: 5000,
  verificationTimeoutMs: 10000,

  // RECOVERING — crash recovery
  recoveryAttempts: 3,
};
