// substrates/webhook-acquisition-substrate/intent-id.js
// Intent ID generator — single source of truth.
//
// Owns: stable, unique intentId generation for webhook events.
// Does NOT own: dispatch, routing, validation, normalization, governance.

const crypto = require('crypto');

/**
 * Generate a stable intentId for a webhook event.
 *
 * @param {string} prefix   — event-type prefix (e.g. 'messaging', 'comments')
 * @param {string} accountId — IG account id (from entry.id)
 * @returns {string}        — intentId in form: {prefix}-{timestamp}-{random}
 */
function newIntentId(prefix, accountId) {
  const ts = Date.now();
  const rnd = crypto.randomBytes(4).toString('hex');
  return `${prefix || 'webhook'}-${ts}-${rnd}`;
}

module.exports = { newIntentId };