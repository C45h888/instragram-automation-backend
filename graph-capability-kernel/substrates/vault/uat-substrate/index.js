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

const RetrieveWorker = require('./workers/retrieve-worker');
const ExchangeRefreshWorker = require('./workers/exchange-refresh-worker');
const DetectWorker = require('./workers/detect-worker');
const signalDispatch = require('../signal-dispatch');
const fsm = require('../../fsm');

/**
 * Store a UAT credential row. Dispatches through graph-capability FSM → CK → persist-telemetry FSM → credential-store-writer.
 * @param {{ userId: string, businessAccountId: string, userAccessToken: string, scope?: string[], expiresAt?: string|null, dataAccessExpiresAt?: string|null }} input
 */
async function store(input) {
  if (!input.userId || !input.businessAccountId || !input.userAccessToken) {
    return { success: false, error: 'userId, businessAccountId, userAccessToken are required' };
  }

  // Chain: substrate → graph-capability FSM → CK → persist-telemetry FSM → credential-store-writer
  const result = fsm.requestCredentialStore({
    operation: 'store_uat',
    userId: input.userId,
    businessAccountId: input.businessAccountId,
    userAccessToken: input.userAccessToken,
    scope: input.scope,
    expiresAt: input.expiresAt,
    dataAccessExpiresAt: input.dataAccessExpiresAt,
    tokenType: 'user',
  });

  if (result.success) {
    signalDispatch.emitEvaluate({
      businessAccountId: input.businessAccountId,
      userId: input.userId,
      source: 'vault.uat.store',
    });
  }
  return result;
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
  const result = await worker.execute({ userId, businessAccountId });
  signalDispatch.emitEvaluate({
    businessAccountId,
    userId,
    source: 'vault.uat.retrieve',
  });
  // Layer 2: emit envelope with isDecryptable=true.
  const envelope = fsm.newEnvelope({ businessAccountId, userId });
  envelope.uat = { isDecryptable: true, ...result };
  signalDispatch.emitEnvelope({ envelope });
  return result;
}

/**
 * Refresh a UAT via fb_exchange_token. Façade orchestrates three bounded workers:
 * retrieve → exchange-refresh → detect. Store dispatched through graph-capability FSM → CK → persist-telemetry.
 * @param {{ userId: string, businessAccountId: string }} input
 */
async function refresh(input) {
  if (!input.userId || !input.businessAccountId) {
    return { success: false, error: 'userId and businessAccountId are required' };
  }

  // Step 1: Retrieve current UAT (bounded worker — decrypt + expiry check)
  const retrieveWorker = new RetrieveWorker();
  let current;
  try {
    current = await retrieveWorker.execute({ userId: input.userId, businessAccountId: input.businessAccountId });
  } catch (err) {
    return { success: false, error: `Retrieve failed: ${err.message}` };
  }

  // Step 2: Exchange for fresh token (bounded worker — single fb_exchange_token call)
  const exchangeWorker = new ExchangeRefreshWorker();
  const exchangeResult = await exchangeWorker.execute({ token: current.token });
  if (!exchangeResult.success) {
    return { success: false, error: exchangeResult.error };
  }

  // Step 3: Validate the refreshed token (bounded worker — single /debug_token call)
  const detectWorker = new DetectWorker();
  const tokenInfo = await detectWorker.execute({ token: exchangeResult.accessToken });
  if (!tokenInfo || !tokenInfo.isValid) {
    return { success: false, error: 'Refreshed UAT failed /debug_token validation' };
  }

  const newExpiresAt = exchangeResult.expiresIn
    ? new Date(Date.now() + exchangeResult.expiresIn * 1000).toISOString()
    : null;
  const dataAccessExpiresAt = tokenInfo.dataAccessExpiresAt
    ? new Date(tokenInfo.dataAccessExpiresAt * 1000).toISOString()
    : null;

  // Step 4: Dispatch credential store through graph-capability FSM → CK → persist-telemetry FSM
  fsm.requestCredentialStore({
    operation: 'store_uat',
    userId: input.userId,
    businessAccountId: input.businessAccountId,
    userAccessToken: exchangeResult.accessToken,
    scope: tokenInfo.scopes,
    expiresAt: newExpiresAt,
    dataAccessExpiresAt,
    tokenType: 'user',
  });

  signalDispatch.emitTokenRefreshed({
    businessAccountId: input.businessAccountId,
    userId: input.userId,
  });
  const envelope = fsm.newEnvelope({ businessAccountId: input.businessAccountId, userId: input.userId });
  envelope.uat = {
    isDecryptable: true,
    expiresAt: newExpiresAt || null,
    scope: tokenInfo.scopes || [],
  };
  signalDispatch.emitEnvelope({ envelope });

  return { success: true, expiresAt: newExpiresAt, scopes: tokenInfo.scopes };
}

/**
 * Detect a UAT/PAT token type via /debug_token. Returns null on failure.
 * @param {{ token: string, businessAccountId?: string, userId?: string }} input
 */
async function detect({ businessAccountId, userId, token }) {
  if (!token) return null;
  const worker = new DetectWorker();
  const result = await worker.execute({ token });
  if (result && result.isValid) {
    signalDispatch.emitEvaluate({
      businessAccountId,
      userId,
      source: 'vault.uat.detect',
    });
    // Layer 2: emit envelope with detection.isValid.
    if (businessAccountId) {
      const envelope = fsm.newEnvelope({ businessAccountId, userId });
      envelope.detection = {
        isValid: true,
        reliabilityImpaired: false,
        reason: null,
        ...result,
      };
      signalDispatch.emitEnvelope({ envelope });
    }
  } else if (result && !result.isValid && businessAccountId) {
    // Layer 2: detection failed — emit envelope with isValid=false.
    const envelope = fsm.newEnvelope({ businessAccountId, userId });
    envelope.detection = {
      isValid: false,
      reliabilityImpaired: false,
      reason: 'Token validation failed',
      ...result,
    };
    signalDispatch.emitEnvelope({ envelope });
  }
  return result;
}

module.exports = { store, retrieve, refresh, detect };
