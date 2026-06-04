// substrates/vault/uat-substrate/index.js
// UAT substrate façade: factory-creates workers, owns pre-flight + signal dispatch.
// Does NOT do I/O — workers do.

const StoreWorker = require('./workers/store-worker');
const RetrieveWorker = require('./workers/retrieve-worker');
const RefreshWorker = require('./workers/refresh-worker');
const DetectWorker = require('./workers/detect-worker');

/**
 * Store a UAT credential row.
 * @param {{ userId: string, businessAccountId: string, userAccessToken: string, scope?: string[], expiresAt?: string|null, dataAccessExpiresAt?: string|null }} input
 */
async function store(input) {
  if (!input.userId || !input.businessAccountId || !input.userAccessToken) {
    return { success: false, error: 'userId, businessAccountId, userAccessToken are required' };
  }
  const worker = new StoreWorker();
  return worker.execute(input);
}

/**
 * Retrieve a UAT (decrypt + expiry check). Throws on failure.
 * @param {{ userId: string, businessAccountId: string }} input
 */
async function retrieve({ userId, businessAccountId }) {
  if (!userId || !businessAccountId) {
    throw new Error('userId and businessAccountId are required');
  }
  const worker = new RetrieveWorker();
  return worker.execute({ userId, businessAccountId });
}

/**
 * Refresh a UAT via fb_exchange_token. On success, emits TOKEN_REFRESHED trigger.
 * @param {{ userId: string, businessAccountId: string, triggerBridge?: object }} input
 */
async function refresh({ triggerBridge, ...input }) {
  if (!input.userId || !input.businessAccountId) {
    return { success: false, error: 'userId and businessAccountId are required' };
  }
  const worker = new RefreshWorker();
  const result = await worker.execute(input);
  if (result.success && triggerBridge) {
    try {
      triggerBridge.emitTokenRefreshed({ userId: input.userId, businessAccountId: input.businessAccountId });
    } catch (emitErr) {
      console.warn('⚠️ trigger-bridge emitTokenRefreshed failed:', emitErr.message);
    }
  }
  return result;
}

/**
 * Detect a UAT/PAT token type via /debug_token. Returns null on failure.
 * @param {{ token: string }} input
 */
async function detect({ token }) {
  if (!token) return null;
  const worker = new DetectWorker();
  return worker.execute({ token });
}

module.exports = { store, retrieve, refresh, detect };
