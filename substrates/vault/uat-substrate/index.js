// substrates/vault/uat-substrate/index.js
// UAT substrate façade: factory-creates workers, owns pre-flight + signal dispatch.
// Does NOT do I/O — workers do.
//
// Constitutional wiring:
//   Every successful worker call emits a trigger event via trigger-bridge → ck → FSM.

const StoreWorker = require('./workers/store-worker');
const RetrieveWorker = require('./workers/retrieve-worker');
const RefreshWorker = require('./workers/refresh-worker');
const DetectWorker = require('./workers/detect-worker');

/**
 * Helper: emit a CAPABILITY_EVALUATE trigger on success.
 * Substrate is the mutation plane — it owns the signal dispatch.
 * @param {{ triggerBridge?: object, businessAccountId?: string, userId?: string, source: string }} params
 */
function _emitEvaluate({ triggerBridge, businessAccountId, userId, source }) {
  if (!triggerBridge) return;
  try {
    triggerBridge.emitCapabilityEvaluate({
      businessAccountId: businessAccountId || null,
      userId: userId || null,
      source,
    });
  } catch (emitErr) {
    console.warn('⚠️ trigger-bridge emitCapabilityEvaluate failed:', emitErr.message);
  }
}

/**
 * Store a UAT credential row.
 * @param {{ userId: string, businessAccountId: string, userAccessToken: string, scope?: string[], expiresAt?: string|null, dataAccessExpiresAt?: string|null, triggerBridge?: object }} input
 */
async function store(input) {
  const { triggerBridge, ...workerInput } = input;
  if (!workerInput.userId || !workerInput.businessAccountId || !workerInput.userAccessToken) {
    return { success: false, error: 'userId, businessAccountId, userAccessToken are required' };
  }
  const worker = new StoreWorker();
  const result = await worker.execute(workerInput);
  if (result.success) {
    _emitEvaluate({ triggerBridge, businessAccountId: workerInput.businessAccountId, userId: workerInput.userId, source: 'vault.uat.store' });
  }
  return result;
}

/**
 * Retrieve a UAT (decrypt + expiry check). Throws on failure.
 * @param {{ userId: string, businessAccountId: string, triggerBridge?: object }} input
 */
async function retrieve({ triggerBridge, userId, businessAccountId }) {
  if (!userId || !businessAccountId) {
    throw new Error('userId and businessAccountId are required');
  }
  const worker = new RetrieveWorker();
  const result = await worker.execute({ userId, businessAccountId });
  _emitEvaluate({ triggerBridge, businessAccountId, userId, source: 'vault.uat.retrieve' });
  return result;
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
 * @param {{ token: string, triggerBridge?: object, businessAccountId?: string, userId?: string }} input
 */
async function detect({ triggerBridge, businessAccountId, userId, token }) {
  if (!token) return null;
  const worker = new DetectWorker();
  const result = await worker.execute({ token });
  if (result && result.isValid) {
    _emitEvaluate({ triggerBridge, businessAccountId, userId, source: 'vault.uat.detect' });
  }
  return result;
}

module.exports = { store, retrieve, refresh, detect };
