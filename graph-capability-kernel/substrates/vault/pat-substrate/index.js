// graph-capability-kernel/substrates/vault/pat-substrate/index.js
// PAT substrate façade: factory-creates workers, owns pre-flight + orchestration.
// Does NOT do I/O — workers do.
// Migrated from substrates/vault/pat-substrate/index.js
//
// Architectural invariant:
//   Substrate = mutation plane (state, pre-flight, factory, signal dispatch)
//   Worker    = executor plane (one bounded I/O call, no state)
//
// Constitutional wiring:
//   Every successful worker call emits a trigger event via signal-dispatch → trigger-bridge → ck → FSM.
//   signal-dispatch is the single source of truth for vault signal emission (signal-dispatch.js).
//
// Layer 2: each success path also builds a canonical observation envelope and
// emits it via signal-dispatch.emitEnvelope() → ck.dispatch(CAPABILITY_OBSERVATION).
// The envelope shape is declared in graph-capability/observations.js.

const ExchangeWorker = require('./workers/exchange-worker');
const StoreWorker = require('./workers/store-worker');
const RetrieveWorker = require('./workers/retrieve-worker');
const signalDispatch = require('../signal-dispatch');
const fsm = require('../../fsm');

/**
 * Exchange a user access token for a page access token + IG business account discovery.
 * One bounded /me/accounts call. No state.
 * @param {{ userAccessToken: string, triggerBridge?: object, businessAccountId?: string|null, userId?: string|null }} input
 */
async function exchange({ userAccessToken, triggerBridge, businessAccountId, userId }) {
  if (!userAccessToken) {
    return { success: false, error: 'userAccessToken is required' };
  }
  const worker = new ExchangeWorker();
  const result = await worker.execute({ userAccessToken });

  if (result.success) {
    signalDispatch.emitEvaluate({
      businessAccountId: result.igBusinessAccountId,
      userId,
      source: 'vault.pat.exchange',
    });
  }
  return result;
}

/**
 * Store a page access token: provision vault key, upsert business account, encrypt, upsert credential.
 * On success, emits NEW_ACCOUNT_CONNECTED so the capability FSM can evaluate.
 * @param {{ userId: string, igBusinessAccountId: string, pageAccessToken: string, pageId: string, pageName: string, scope?: string[], triggerBridge?: object, businessAccountId?: string, userId_?: string }} input
 */
async function store(input) {
  const { triggerBridge, businessAccountId, userId_, ...workerInput } = input;
  if (!workerInput.userId || !workerInput.igBusinessAccountId || !workerInput.pageAccessToken || !workerInput.pageName) {
    return { success: false, error: 'userId, igBusinessAccountId, pageAccessToken, pageName are required' };
  }
  const worker = new StoreWorker();
  const result = await worker.execute(workerInput);

  if (result.success) {
    signalDispatch.emitNewAccountConnected({
      businessAccountId: result.businessAccountId || businessAccountId,
      userId: workerInput.userId || userId_,
    });
    // Layer 2: emit a fresh envelope reporting the new PAT is decryptable.
    const envelope = fsm.newEnvelope({
      businessAccountId: result.businessAccountId || businessAccountId,
      userId: workerInput.userId || userId_,
    });
    envelope.pat = { isDecryptable: true, ...result };
    signalDispatch.emitEnvelope({ envelope });
  }
  return result;
}

/**
 * Retrieve and decrypt a page access token.
 * Throws on failure (matches legacy contract).
 * On success, emits CAPABILITY_EVALUATE so the FSM observes vault liveness.
 * @param {{ userId: string, businessAccountId: string, triggerBridge?: object }} input
 */
async function retrieve({ triggerBridge, userId, businessAccountId }) {
  if (!userId || !businessAccountId) {
    throw new Error('userId and businessAccountId are required');
  }
  const worker = new RetrieveWorker();
  const result = await worker.execute({ userId, businessAccountId });
  signalDispatch.emitTokenRefreshed({
    businessAccountId,
    userId,
    source: 'vault.pat.retrieve',
  });
  // Layer 2: emit envelope with isDecryptable=true.
  const envelope = fsm.newEnvelope({ businessAccountId, userId });
  envelope.pat = { isDecryptable: true, token: result };
  signalDispatch.emitEnvelope({ envelope });
  return result;
}

module.exports = { exchange, store, retrieve };
