// graph-capability-kernel/substrates/vault/uat-substrate/index.js
// UAT substrate façade: factory-creates workers, owns pre-flight + signal dispatch.
// Does NOT do I/O — workers do.
// Migrated from substrates/vault/uat-substrate/index.js
//
// Constitutional wiring:
//   Every successful worker call emits a trigger event via signal-dispatch → trigger-bridge → ck → FSM.
//
// Layer 2: each success path also builds a canonical observation envelope and
// emits it via signal-dispatch.emitEnvelope() → ck.dispatch(CAPABILITY_OBSERVATION).

const StoreWorker = require('./workers/store-worker');
const RetrieveWorker = require('./workers/retrieve-worker');
const RefreshWorker = require('./workers/refresh-worker');
const DetectWorker = require('./workers/detect-worker');
const signalDispatch = require('../signal-dispatch');
const observations = require('../../graph-capability/observations');

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
    signalDispatch.emitEvaluate({
      triggerBridge,
      businessAccountId: workerInput.businessAccountId,
      userId: workerInput.userId,
      source: 'vault.uat.store',
    });
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
  signalDispatch.emitEvaluate({
    triggerBridge,
    businessAccountId,
    userId,
    source: 'vault.uat.retrieve',
  });
  // Layer 2: emit envelope with isDecryptable=true.
  const envelope = observations.newEnvelope({ businessAccountId, userId });
  envelope.uat = { isDecryptable: true, ...result };
  signalDispatch.emitEnvelope({ triggerBridge, envelope });
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
  if (result.success) {
    signalDispatch.emitTokenRefreshed({
      triggerBridge,
      businessAccountId: input.businessAccountId,
      userId: input.userId,
    });
    // Layer 2: emit envelope — UAT refreshed, isDecryptable=true with new scope.
    const envelope = observations.newEnvelope({ businessAccountId: input.businessAccountId, userId: input.userId });
    envelope.uat = {
      isDecryptable: true,
      expiresAt: result.expiresAt || null,
      scope: result.scopes || [],
    };
    signalDispatch.emitEnvelope({ triggerBridge, envelope });
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
    signalDispatch.emitEvaluate({
      triggerBridge,
      businessAccountId,
      userId,
      source: 'vault.uat.detect',
    });
    // Layer 2: emit envelope with detection.isValid.
    if (businessAccountId) {
      const envelope = observations.newEnvelope({ businessAccountId, userId });
      envelope.detection = {
        isValid: true,
        reliabilityImpaired: false,
        reason: null,
        ...result,
      };
      signalDispatch.emitEnvelope({ triggerBridge, envelope });
    }
  } else if (result && !result.isValid && businessAccountId) {
    // Layer 2: detection failed — emit envelope with isValid=false.
    const envelope = observations.newEnvelope({ businessAccountId, userId });
    envelope.detection = {
      isValid: false,
      reliabilityImpaired: false,
      reason: 'Token validation failed',
      ...result,
    };
    signalDispatch.emitEnvelope({ triggerBridge, envelope });
  }
  return result;
}

module.exports = { store, retrieve, refresh, detect };
