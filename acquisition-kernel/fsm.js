// control-plane/governance/domains/acquisition-fsm.js
// Acquisition Domain FSM: federated state machine governing acquisition lifecycle.
//
// Owns: intent discovery → execution → completion lifecycle ONLY.
// Does NOT own: engagement signals (auth strikes, circuit breakers, retry counting),
//               cross-domain event emission, execution mechanics.
//
// Constitutional purity: acquisition-fsm is a PURE intent lifecycle domain.
// Engagement signals (AUTH_FAILURE_STRIKE, RATE_LIMIT_DETECTED, RETRY_EXHAUSTED,
// AUTH_SUCCESS, RETRY_COUNT_INCREMENTED) are emitted by retry-cadence workers
// directly to CK. DOMAIN_EVENT_MAP routes them to engagement-fsm independently.
// Acquisition-fsm never emits engagement-domain events.
//
// Reports to: constitutional kernel for transition validation + global observability.
//
// ── Local intelligence enhancements (Phase 8 FSM enrichment) ────────────────
// Per-intent state records replace the flat global state.
// Global state (IDLE/ACQUIRING) is DERIVED from the intent record set.
// 3-layer guards: state check → intent presence → payload shape.
// Span events emit structured lifecycle telemetry per intent.
// Gate telemetry accumulates structured veto reasons per intent.
// Health surface is opaque — no raw Map exposure.
// Timeout sweeper force-closes stale intents.

let _observability = null;
function _obs() {
  if (!_observability) {
    try { _observability = require('../control-plane/observability/emitters/transition-emitter'); }
    catch (_) { _observability = null; }
  }
  return _observability;
}

// ── Governance reference (set by CK at boot) ────────────────────────────
// The FSM holds a governance ref for worker invocation and event dispatch.
// engagement-fsm pattern: set at boot, passed through execution contexts.
let _governance = null;

function setGovernance(governance) {
  if (governance && typeof governance.dispatch === 'function') {
    _governance = governance;
  }
}

function getGovernance() {
  return _governance;
}

// ═══════════════════════════════════════════════════════════════════════════════
// 0. Timeout and telemetry configuration
// ═══════════════════════════════════════════════════════════════════════════════

const TIMEOUT_CONFIG = {
  parseTimeoutMs:        300_000,  // 5 min — PARSING phase max age before stale
  intentTimeoutMs:        900_000,  // 15 min — total intent max age before force-close
  gateVetoWindowMs:       60_000,  // 1 min — rate window for veto telemetry
  historyRingSize:         10,       // bounded ring: phase history per intent record
  gateVetoRingSize:        20,      // bounded ring: gate vetoes per intent
  dedupFingerprintSize:  1000,      // LRU cap for intent fingerprints
};

let _timeoutConfig = { ...TIMEOUT_CONFIG };

function setTimeoutConfig(overrides) {
  _timeoutConfig = { ..._timeoutConfig, ...overrides };
}

function getTimeoutConfig() {
  return { ..._timeoutConfig };
}

// ═══════════════════════════════════════════════════════════════════════════════
// 1. Local State Registry
// ═══════════════════════════════════════════════════════════════════════════════

const STATE_REGISTRY = {
  IDLE: {
    description: 'No active intents — ready for intake',
  },
  ACQUIRING: {
    description: 'One or more intents in flight (ACQUIRING | PARSING | COMPLETING)',
  },
  STAGING: {
    description: 'Webhook events staged in the FSM, pending Phase 2 persistence',
  },
};

// ── Staged event map: accountId → canonicalEvent[] ──────────────────────
// Phase 1 of the webhook acquisition substrate normalizes incoming
// webhooks into canonical event objects and dispatches them here. The FSM
// holds them in memory; Phase 2 will resolve staged events into
// DB_WRITE_REQUESTED → postgres-telemetry-kernel.
const STAGED_EVENT_CAP = 1000; // bounded; oldest dropped first on overflow
const _stagedEvents = new Map(); // accountId → { events: CanonicalEvent[], enqueuedAt: number }

function _stageEvent(accountId, event) {
  if (!accountId || !event) return;
  let bucket = _stagedEvents.get(accountId);
  if (!bucket) {
    bucket = { events: [], enqueuedAt: Date.now() };
    _stagedEvents.set(accountId, bucket);
  }
  if (bucket.events.length >= STAGED_EVENT_CAP) {
    bucket.events.shift(); // bounded ring — drop oldest
  }
  bucket.events.push(event);
}

function getStagedEvents(accountId) {
  const bucket = _stagedEvents.get(accountId);
  return bucket ? [...bucket.events] : [];
}

function clearStagedEvents(accountId) {
  if (accountId) {
    _stagedEvents.delete(accountId);
  } else {
    _stagedEvents.clear();
  }
}

// ── Staged event lifecycle helpers (Phase 2) ─────────────────────────────
// The FSM owns the lifecycle: it stages events (Phase 1) and resolves
// them into DB writes (Phase 2). Hydration (account UUID, conversation
// UUID, media UUID) happens HERE through governed reads so the resolvers
// remain pure transforms.

function _removeStagedEvent(accountId, intentId) {
  const bucket = _stagedEvents.get(accountId);
  if (!bucket) return false;
  const idx = bucket.events.findIndex((e) => e.intentId === intentId);
  if (idx === -1) return false;
  bucket.events.splice(idx, 1);
  if (bucket.events.length === 0) {
    _stagedEvents.delete(accountId);
  }
  return true;
}

function _getStagedEvent(accountId, intentId) {
  const bucket = _stagedEvents.get(accountId);
  if (!bucket) return null;
  return bucket.events.find((e) => e.intentId === intentId) || null;
}

function _findStagedEventById(accountId, eventId) {
  const bucket = _stagedEvents.get(accountId);
  if (!bucket) return null;
  return bucket.events.find((e) => e.eventId === eventId) || null;
}

// ── Webhook resolvers (Phase 2 — pure transforms) ─────────────────────────
// The FSM hydrates context via governed reads, then calls the resolver
// to produce the DB row. Resolvers are mounted via the substrate index.
const resolvers =
  require('./substrates/webhook-acquisition-substrate/resolvers');

// ── Inference engine (deterministic state reducer) ──────────────────────
// The FSM uses this engine to compute the inferred state for any
// (accountId, intentId) by replaying the transition log. State is
// COMPUTED, not stored. Same log → same state, always.
const inferenceEngine = require('./inference-engine');

/**
 * Hydrate the resolved context that resolvers expect.
 * Performs governed reads for account UUID, media UUID, conversation UUID.
 * Returns a context object with all fields populated (or error descriptor).
 */
async function _hydrateResolverContext(canonicalEvent, governance) {
  if (!canonicalEvent || !governance) {
    return { error: 'missing_governance' };
  }
  const igAccountId = canonicalEvent.igAccountId || canonicalEvent.accountId;
  const context = {
    accountId: null,
    businessAccountId: null,
    mediaId: null,            // internal UUID for instagram_media
    conversationId: null,      // internal UUID for instagram_dm_conversations
    customerInstagramId: null,
    customerUsername: null,
  };

  // ── 1. Resolve IG account id → internal account UUID ────────────────
  try {
    const r = await governance.governedRead('db.accounts', {
      query: 'igAccountIdToUuid',
      igAccountId,
    });
    if (r && r.success && r.data) {
      context.accountId = r.data.id || r.data.account_id || null;
      context.businessAccountId = r.data.business_account_id || context.accountId;
    }
  } catch (_) {
    // fall through — caller will see null and fail
  }
  if (!context.accountId) {
    return { error: 'account_not_resolved', igAccountId };
  }

  // ── 2. Resolve media UUID for comment/mention events ────────────────
  const igMediaId = canonicalEvent.normalized?.mediaId;
  if (igMediaId) {
    try {
      const r = await governance.governedRead('db.media', {
        query: 'igMediaIdToUuid',
        igMediaId,
        businessAccountId: context.businessAccountId,
      });
      if (r && r.success && r.data) {
        context.mediaId = r.data.id || null;
      }
    } catch (_) {
      // media UUID may be null; resolver writes row with instagram_media_id only
    }
  }

  // ── 3. Resolve conversation UUID for DM events ──────────────────────
  if (canonicalEvent.eventType === 'dm_echo' || canonicalEvent.eventType === 'dm_postback') {
    const recipientId = canonicalEvent.normalized?.recipientId;
    if (recipientId) {
      try {
        const r = await governance.governedRead('db.accounts', {
          query: 'igThreadIdToUuid',
          threadIds: [recipientId],
        });
        if (r && r.success && r.data && r.data.length > 0) {
          context.conversationId = r.data[0].id || null;
          context.customerInstagramId = r.data[0].customer_instagram_id || null;
          context.customerUsername = r.data[0].customer_username || null;
        }
      } catch (_) {
        // conversation missing — caller signals REPAIR_CONVERSATION
      }
    }
  }

  return context;
}

// ═══════════════════════════════════════════════════════════════════════════════
// 2. Intent record helpers (private)
// ═══════════════════════════════════════════════════════════════════════════════

function _makeRing(maxSize) {
  return { _items: [], _max: maxSize };
}

function _ringPush(ring, item) {
  if (ring && ring._max > 0) {
    ring._items.push(item);
    if (ring._items.length > ring._max) {
      ring._items.shift();
    }
  }
}

function _createIntentRecord(event) {
  const now = Date.now();
  return {
    intentId:            event.intentId,
    accountId:            event.accountId,
    domain:               event.domain,
    params:               event.params || {},
    intentFingerprint:    _computeFingerprint(event),
    intakeAt:             now,
    currentPhase:         'ACQUIRING',
    lastTransitionAt:     now,
    executionDispatchedAt: null,
    parsingDispatchedAt:  null,
    parsingJobId:         null,
    rawCount:             0,
    history:              _makeRing(_timeoutConfig.historyRingSize),
    failureReason:        null,
    gateVetoes:           _makeRing(_timeoutConfig.gateVetoRingSize),
    outcome:              null,
    lastFailureAt:        null,  // set by INSIGHTS_POLL_FAILURE
    lastFailureError:     null,  // set by INSIGHTS_POLL_FAILURE
    deferredAt:           null,  // set by ACQUISITION_DEFER
    deferredReason:       null,  // set by ACQUISITION_DEFER
  };
}

function _deriveGlobalState() {
  const active = Array.from(_intents.values()).filter(
    (r) => r.currentPhase === 'ACQUIRING' || r.currentPhase === 'PARSING' || r.currentPhase === 'COMPLETING'
  );
  return active.length > 0 ? 'ACQUIRING' : 'IDLE';
}

function _computeFingerprint(event) {
  try {
    const key = `${event.domain || ''}:${event.accountId || ''}:${JSON.stringify(event.params || {})}`.replace(/\s/g, '');
    let hash = 0;
    for (let i = 0; i < key.length; i++) {
      hash = ((hash << 5) - hash + key.charCodeAt(i)) | 0;
    }
    return String(Math.abs(hash));
  } catch (_) {
    return null;
  }
}

function _enforceFingerprintCap() {
  if (_fingerprintDedup.size >= _timeoutConfig.dedupFingerprintSize) {
    let oldest = null;
    let oldestAt = Infinity;
    for (const [fp, entry] of _fingerprintDedup) {
      if (entry.at < oldestAt) {
        oldestAt = entry.at;
        oldest = fp;
      }
    }
    if (oldest) _fingerprintDedup.delete(oldest);
  }
}

function _recordPhaseHistory(intentId, phase, eventType) {
  const rec = _intents.get(intentId);
  if (rec) {
    rec.currentPhase = phase;
    rec.lastTransitionAt = Date.now();
    _ringPush(rec.history, { phase, eventType, at: Date.now() });
  }
}

function _closeIntent(intentId, outcome, failureReason) {
  const rec = _intents.get(intentId);
  if (rec) {
    rec.outcome = outcome;
    rec.failureReason = failureReason || null;
    rec.currentPhase = 'CLOSED';
    rec.lastTransitionAt = Date.now();
    _ringPush(rec.history, { phase: 'CLOSED', eventType: outcome, at: Date.now() });
  }
  // Remove fingerprint
  for (const [fp, entry] of _fingerprintDedup) {
    if (entry.intentId === intentId) { _fingerprintDedup.delete(fp); break; }
  }
}

// ── Span event emitter (additive to existing transition record) ─────────────────
// Spans ride the existing transition() infrastructure with entity='intent-span'.
// This avoids modifying the observability plane — spans are structured as transitions.

function _emitSpan(type, intentId, accountId, domain, extras = {}) {
  const rec = intentId ? _intents.get(intentId) : null;
  const at = Date.now();
  try {
    const obs = _obs();
    if (obs && typeof obs.transition === 'function') {
      obs.transition({
        domain:   'acquisition',
        entity:   'intent-span',
        entityId: intentId || null,
        previousState: null,
        nextState:  type,
        authority: 'acquisition-fsm',
        raw: {
          type,
          intentId: intentId || null,
          accountId: accountId || null,
          domain:   domain   || null,
          at,
          ms_since_intake: rec ? at - rec.intakeAt : 0,
          ...extras,
        },
      });
    }
  } catch (_) {}
  return { type, intentId, accountId, domain, at, ms_since_intake: rec ? at - rec.intakeAt : 0, ...extras };
}

// ═══════════════════════════════════════════════════════════════════════════════
// 3. Domain-local runtime state (private)
// ═══════════════════════════════════════════════════════════════════════════════

// Global state is DERIVED from the intent record set.
let _localState = 'IDLE';
let _lastTransitionedAt = null;

// intentId → IntentRecord
const _intents = new Map();

// fingerprint → { intentId, at } for dedup
const _fingerprintDedup = new Map();

// ── Default fail-open sanity check (universal gate pattern) ──────────────────

const _defaultSanityCheck = async () => ({ allowed: true });

function _resolveSanityCheck(ctx, action) {
  if (ctx && typeof ctx.sanityCheck === 'function') {
    return ctx.sanityCheck(action);
  }
  return _defaultSanityCheck(action);
}

// ═══════════════════════════════════════════════════════════════════════════════
// 4. Guard helpers (3-layer guards per transition)
// ═══════════════════════════════════════════════════════════════════════════════

// Layer 1: global state check
function _guardState(fromState, expected) {
  if (fromState !== expected) {
    return { allowed: false, reason: `Cannot transition from ${fromState}` };
  }
  return { allowed: true };
}

// Layer 2: intent presence check — returns the record if found, else null
function _guardIntentExists(intentId, expectedPhase) {
  const rec = _intents.get(intentId);
  if (!rec) {
    return { allowed: false, reason: `intent_not_found:${intentId}`, record: null };
  }
  if (expectedPhase && rec.currentPhase !== expectedPhase) {
    return { allowed: false, reason: `intent_phase_mismatch:expected=${expectedPhase},actual=${rec.currentPhase}`, record: rec };
  }
  return { allowed: true, record: rec };
}

// Layer 3: payload shape checks
const REQUIRED_PAYLOAD_FIELDS = {
  ACQUISITION_INTENT_RECEIVED: ['accountId', 'domain', 'intentId'],
  PARSING_DISPATCHED:           ['intentId', 'jobId', 'domain', 'accountId'],
  PARSING_COMPLETE:             ['intentId', 'result'],
  ACQUISITION_COMPLETE:         ['intentId'],
  ACQUISITION_EXECUTING:         ['intentId'],
  WEBHOOK_EVENT_RECEIVED:       ['accountId', 'intentId', 'domain', 'eventType', 'eventId', 'normalized'],
  WEBHOOK_EVENT_DISCARDED:      ['accountId', 'intentId', 'domain', 'eventType', 'reason'],
  PERSIST_STAGED_EVENT:         ['accountId', 'intentId', 'eventId'],
  WEBHOOK_EVENT_PERSISTED:      ['accountId', 'intentId', 'eventId', 'table', 'count'],
  WEBHOOK_EVENT_PERSIST_FAILED: ['accountId', 'intentId', 'eventId', 'error'],
  // ── Inference engine inputs (Phase 1) ─────────────────────────────────
  // The FSM records these transitions to its log; the reducer computes
  // the inferred state. Workers and the substrate emit these.
  WORKER_STATE_TRANSITION:      ['accountId', 'intentId', 'from', 'to', 'domain', 'eventType'],
  SUBSTRATE_STATE_TRANSITION:   ['accountId', 'intentId', 'from', 'to'],
  // ── Cross-kernel failure intake (Phase 8) ─────────────────────────────
  INSIGHTS_POLL_FAILURE:        ['accountId', 'intentId', 'domain', 'error'],
  ACQUISITION_DEFER:            ['accountId', 'domain', 'reason'],
};

function _guardPayload(event) {
  const required = REQUIRED_PAYLOAD_FIELDS[event.type];
  if (!required) return { allowed: true };
  for (const field of required) {
    if (event[field] === undefined || event[field] === null) {
      return { allowed: false, reason: `malformed_payload:missing_field=${field}` };
    }
  }
  return { allowed: true };
}

// Combined 3-layer guard factory
function _makeGuard(fromState, intentPhase = null) {
  return (event) => {
    // Layer 1: global state
    const s = _deriveGlobalState();
    const l1 = _guardState(s, fromState);
    if (!l1.allowed) return l1;

    // Layer 2: intent presence
    if (intentPhase && event.intentId) {
      const l2 = _guardIntentExists(event.intentId, intentPhase);
      if (!l2.allowed) return l2;
    }

    // Layer 3: payload shape
    const l3 = _guardPayload(event);
    if (!l3.allowed) return l3;

    return { allowed: true };
  };
}

// ── Sanity gate resolver with intent-level veto tracking ─────────────────────

async function _resolveSanityCheckWithTelemetry(ctx, action, intentId) {
  const gate = await _resolveSanityCheck(ctx, action);
  if (!gate.allowed) {
    const reason = gate.reason || 'gate_rejected';
    const at = Date.now();
    if (intentId) {
      const rec = _intents.get(intentId);
      if (rec) {
        _ringPush(rec.gateVetoes, { op: action.operation, reason, at });
      }
    }
    _emitSpan('INTENT_GATE_VETO', intentId, action.accountId, action.domain, {
      operation: action.operation,
      reason,
    });
  }
  return gate;
}

// ═══════════════════════════════════════════════════════════════════════════════
// 5. Domain Transition Map
// ═══════════════════════════════════════════════════════════════════════════════

const TRANSITION_MAP = {

  // ── Intent received → begin acquisition ─────────────────────────────────
  ACQUISITION_INTENT_RECEIVED: {
    target: 'ACQUIRING',
    guard: (event) => {
      const s = _deriveGlobalState();
      const l1 = _guardState(s, 'IDLE');
      if (!l1.allowed) return l1;

      const l3 = _guardPayload(event);
      if (!l3.allowed) return l3;

      // Dedup: check fingerprint within window
      const fp = _computeFingerprint(event);
      if (fp && _fingerprintDedup.has(fp)) {
        const entry = _fingerprintDedup.get(fp);
        const age = Date.now() - entry.at;
        // Only reject if the prior intent is still active (not closed)
        const priorRec = _intents.get(entry.intentId);
        if (priorRec && priorRec.currentPhase !== 'CLOSED') {
          _emitSpan('INTENT_DEDUP', event.intentId, event.accountId, event.domain, {
            priorIntentId: entry.intentId,
            fingerprintAgeMs: age,
          });
          return { allowed: false, reason: `duplicate_intent:fingerprint=${fp}` };
        }
        // Prior intent closed — allow; update dedup entry
        _fingerprintDedup.delete(fp);
      }

      return { allowed: true };
    },
    buildActions: async (event, ctx) => {
      // Create intent record
      const rec = _createIntentRecord(event);
      _intents.set(event.intentId, rec);

      // Register fingerprint
      const fp = rec.intentFingerprint;
      if (fp) {
        _enforceFingerprintCap();
        _fingerprintDedup.set(fp, { intentId: event.intentId, at: Date.now() });
      }

      _emitSpan('INTENT_INTAKE', event.intentId, event.accountId, event.domain, {
        fingerprint: fp,
      });

      const gate = await _resolveSanityCheckWithTelemetry(ctx, {
        operation: 'execute_acquisition',
        accountId: event.accountId,
        domain: event.domain,
        intentId: event.intentId,
      }, event.intentId);

      if (!gate.allowed) {
        rec.outcome = 'gated';
        rec.currentPhase = 'CLOSED';
        rec.failureReason = { code: gate.reason || 'gate_rejected', source: 'sanity_check' };
        _ringPush(rec.history, { phase: 'CLOSED', eventType: 'INTENT_GATE_VETO', at: Date.now() });
        return [{
          type: 'GATE_REJECTED',
          operation: 'execute_acquisition',
          accountId: event.accountId,
          domain: event.domain,
          intentId: event.intentId,
          reason: gate.reason || 'gate_rejected',
        }];
      }

      rec.executionDispatchedAt = Date.now();
      _emitSpan('INTENT_DISPATCHED', event.intentId, event.accountId, event.domain);

      return [{
        type: 'EXECUTE_ACQUISITION',
        accountId: event.accountId,
        domain: event.domain,
        intentId: event.intentId,
        params: event.params,
      }];
    },
  },

  // ── Execution started → stop intent discovery ───────────────────────────
  ACQUISITION_EXECUTING: {
    target: 'ACQUIRING',
    guard: _makeGuard('ACQUIRING', 'ACQUIRING'),
    buildActions: async (event) => {
      _recordPhaseHistory(event.intentId, 'ACQUIRING', 'ACQUISITION_EXECUTING');
      return [{ type: 'STOP_INTENT_DISCOVERY' }];
    },
  },

  // ── Parsing dispatched → parsing worker in flight ───────────────────────
  PARSING_DISPATCHED: {
    target: 'ACQUIRING', // stays ACQUIRING — wait for PARSING_COMPLETE
    guard: (event) => {
      const s = _deriveGlobalState();
      const l1 = _guardState(s, 'ACQUIRING');
      if (!l1.allowed) return l1;

      const l3 = _guardPayload(event);
      if (!l3.allowed) return l3;

      // Intent must exist in ACQUIRING phase (not yet parsing)
      const rec = _intents.get(event.intentId);
      if (!rec) {
        return { allowed: false, reason: `orphaned_parse_dispatch:intent=${event.intentId}` };
      }
      if (rec.currentPhase !== 'ACQUIRING') {
        return { allowed: false, reason: `intent_phase_mismatch:expected=ACQUIRING,actual=${rec.currentPhase}` };
      }

      return { allowed: true };
    },
    buildActions: async (event) => {
      const rec = _intents.get(event.intentId);
      if (rec) {
        rec.parsingDispatchedAt = Date.now();
        rec.parsingJobId = event.jobId;
        rec.rawCount = event.rawCount || 0;
        _recordPhaseHistory(event.intentId, 'PARSING', 'PARSING_DISPATCHED');
      }
      _emitSpan('INTENT_PARSING_START', event.intentId, event.accountId, event.domain, {
        jobId: event.jobId,
        rawCount: event.rawCount || 0,
      });
      return [];
    },
  },

  // ── Parsing complete → worker finished ───────────────────────────────────
  PARSING_COMPLETE: {
    target: 'ACQUIRING', // global state derived from intents; stays ACQUIRING if other intents active
    guard: (event) => {
      const s = _deriveGlobalState();
      const l1 = _guardState(s, 'ACQUIRING');
      if (!l1.allowed) return l1;

      const l3 = _guardPayload(event);
      if (!l3.allowed) return l3;

      // Intent must exist in PARSING phase
      const rec = _intents.get(event.intentId);
      if (rec) {
        if (rec.currentPhase !== 'PARSING') {
          return { allowed: false, reason: `intent_phase_mismatch:expected=PARSING,actual=${rec.currentPhase}` };
        }
        return { allowed: true };
      }

      // Phase 2: webhook event cross-kernel return path. The
      // persist-telemetry-fsm emits PARSING_COMPLETE in response to
      // the DB_WRITE_REQUESTED we dispatched. The intentId is the
      // webhook event's intentId. If a staged webhook event with
      // this intentId exists, accept the completion.
      const accountId = event.accountId;
      const staged = _findStagedEventById(accountId, event.intentId)
                  || _getStagedEvent(accountId, event.intentId);
      if (staged) {
        return { allowed: true };
      }

      return { allowed: false, reason: `stale_complete:intent=${event.intentId}` };
    },
    buildActions: async (event, ctx) => {
      const { accountId, domain, intentId, result } = event;
      const rec = _intents.get(intentId);
      const staged = !rec ? _getStagedEvent(accountId, intentId) : null;

      // ── Phase 2 path: webhook cross-kernel write completed ──────────
      if (staged) {
        const count = (result && typeof result.count === 'number') ? result.count : 1;
        const table = (result && result.table) || (domain === 'messages' ? 'instagram_dm_messages' : 'instagram_comments');
        const error = result && result.status === 'failed' ? (result.error || 'unknown') : null;
        const removed = _removeStagedEvent(accountId, intentId);

        _emitSpan('WEBHOOK_EVENT_PERSISTED', intentId, accountId, staged.domain, {
          eventId: staged.eventId,
          eventType: staged.eventType,
          table,
          count,
          removed,
        });

        if (error) {
          return [{
            type: 'WEBHOOK_EVENT_PERSIST_FAILED',
            accountId, intentId, eventId: staged.eventId,
            error,
            phase: 'write',
          }];
        }
        return [{
          type: 'WEBHOOK_EVENT_PERSISTED',
          accountId, intentId, eventId: staged.eventId, table, count,
          removedFromStaging: removed,
        }];
      }

      // Defensive: malformed/missing result
      if (!result || typeof result !== 'object') {
        if (rec) {
          _closeIntent(intentId, 'failed', { code: 'missing_result', source: 'fsm_parse_complete' });
        }
        _emitSpan('INTENT_PARSING_END', intentId, accountId, domain, {
          status: 'malformed',
          error: 'missing_result',
        });
        // Retry-exhaustion logic REMOVED. Acquisition FSM no longer
        // emits RETRY_EXHAUSTED on parsing failure. The terminal
        // failure is recorded on the intent record itself.
        return [{
          type: 'PARSING_FAILED',
          accountId,
          domain,
          intentId,
          error: 'missing_result',
        }];
      }

      _emitSpan('INTENT_PARSING_END', intentId, accountId, domain, {
        status: result.status,
        rawCount: result.rawCount || rec?.rawCount || 0,
      });

      if (result.status === 'failed') {
        // Retry-exhaustion logic REMOVED. No more sanity-gate check
        // for parsing_failed_retry_exhausted. The terminal failure
        // is recorded on the intent record itself; downstream
        // consumers (the new system) decide what to do with it.
        if (rec) {
          rec.failureReason = { code: 'parsing_failed', source: 'parsing_worker', message: result.error || 'parsing_failed' };
        }
        return [{
          type: 'PARSING_FAILED',
          accountId, domain, intentId,
          error: result.error || 'parsing_failed',
        }];
      }

      // Success path: restart intent discovery
      _recordPhaseHistory(intentId, 'ACQUIRING', 'PARSING_COMPLETE_SUCCESS');
      return [{ type: 'START_INTENT_DISCOVERY' }];
    },
  },

  // ── Acquisition complete (terminal) ──────────────────────────────────────
  ACQUISITION_COMPLETE: {
    target: null, // derived after action building (depends on whether other intents exist)
    guard: (event) => {
      const s = _deriveGlobalState();
      // Allow from ACQUIRING; also allow from IDLE if the intent exists (stale complete — defensive)
      if (s !== 'ACQUIRING' && s !== 'IDLE') {
        return { allowed: false, reason: `Cannot complete from ${s}` };
      }

      const l3 = _guardPayload(event);
      if (!l3.allowed) return l3;

      // If from IDLE, the intent must exist and be closable
      if (s === 'IDLE' && event.intentId) {
        const rec = _intents.get(event.intentId);
        if (!rec) {
          return { allowed: false, reason: `stale_complete:intent=${event.intentId}` };
        }
        if (rec.currentPhase === 'CLOSED') {
          return { allowed: false, reason: `already_closed:intent=${event.intentId}` };
        }
      }

      return { allowed: true };
    },
    buildActions: async (event, ctx) => {
      const { accountId, domain, intentId, result } = event;
      const actions = [];
      const rec = intentId ? _intents.get(intentId) : null;

      if (result) {
        const gate = await _resolveSanityCheckWithTelemetry(ctx, {
          operation: 'write_acquisition_result',
          accountId, domain, intentId,
        }, intentId);

        if (!gate.allowed) {
          actions.push({
            type: 'GATE_REJECTED',
            operation: 'write_acquisition_result',
            accountId, domain, intentId,
            reason: gate.reason || 'gate_rejected',
          });
        } else {
          actions.push({
            type: 'WRITE_ACQUISITION_RESULT',
            accountId, domain, intentId,
            result: event.result,
          });
        }
      }

      // Close the intent record
      if (rec) {
        const outcome = result?.status === 'failed' ? 'failed' : 'success';
        _closeIntent(intentId, outcome, result?.error ? { code: result.error, source: 'acquisition_complete' } : null);
      }

      _emitSpan('INTENT_COMPLETE', intentId, accountId, domain, {
        outcome: rec?.outcome || (result?.status === 'failed' ? 'failed' : 'success'),
        failureReason: rec?.failureReason || null,
        gateVetoCount: rec?.gateVetoes?._items?.length || 0,
      });

      actions.push({ type: 'START_INTENT_DISCOVERY' });
      return actions;
    },
  },

  // ── Webhook event received (Phase 1: stage in FSM, no DB write) ──────────
  // The webhook-acquisition substrate's bounded workers dispatch this
  // action after normalizing the Meta payload into a canonical event
  // object. The FSM stages the event in _stagedEvents (account-keyed)
  // and emits WEBHOOK_EVENT_STAGED so observability can see it.
  //
  // Phase 2 will add a PERSIST_STAGED_EVENT action that resolves
  // staged events into DB_WRITE_REQUESTED → postgres-telemetry-kernel.
  //
  // This transition is non-blocking — it does not interact with the
  // intent lifecycle (intents are for polling-based acquisition). The
  // STAGING state coexists with ACQUIRING.
  WEBHOOK_EVENT_RECEIVED: {
    target: 'STAGING',
    guard: (event) => {
      const l3 = _guardPayload(event);
      if (!l3.allowed) return l3;
      return { allowed: true };
    },
    buildActions: async (event) => {
      const { accountId, intentId, domain, eventType, eventId, occurredAt, source, priority, normalized, raw } = event;

      const canonicalEvent = {
        intentId,
        accountId,
        domain,
        eventType,
        eventId,
        occurredAt,
        source,
        priority,
        normalized,
        raw,
        stagedAt: Date.now(),
      };

      _stageEvent(accountId, canonicalEvent);

      // Seed the inference engine: the event is now staged. This is the
      // canonical source — the FSM itself records the transition because
      // it just staged the event. Workers do NOT dispatch through the
      // inference engine; substrate transitions cover the pre-stage path.
      inferenceEngine.recordTransition(accountId, intentId, 'SUBSTRATE', 'WORKER_DISPATCHED', 'STAGED');

      _emitSpan('WEBHOOK_EVENT_STAGED', intentId, accountId, domain, {
        eventType,
        eventId,
        occurredAt,
      });

      return [{
        type: 'WEBHOOK_EVENT_STAGED',
        accountId,
        intentId,
        domain,
        eventType,
        eventId,
        stagedAt: canonicalEvent.stagedAt,
      }];
    },
  },

  // ── Webhook event discarded (Phase 1: validation failure in worker) ──────
  // The worker called ig-reliability.analyzeFailure on the bad payload,
  // got recommendations, and is now signaling the FSM that the event
  // was rejected. The FSM records the discard for observability. No
  // canonical event is staged.
  WEBHOOK_EVENT_DISCARDED: {
    target: 'STAGING',
    guard: (event) => {
      const l3 = _guardPayload(event);
      if (!l3.allowed) return l3;
      return { allowed: true };
    },
    buildActions: async (event) => {
      const { accountId, intentId, domain, eventType, reason, recommendations } = event;

      _emitSpan('WEBHOOK_EVENT_DISCARDED', intentId, accountId, domain, {
        eventType,
        reason,
        recommendationCount: Array.isArray(recommendations) ? recommendations.length : 0,
      });

      return [{
        type: 'WEBHOOK_EVENT_DISCARDED',
        accountId,
        intentId,
        domain,
        eventType,
        reason,
        recommendations: recommendations || [],
      }];
    },
  },

  // ── PERSIST_STAGED_EVENT (Phase 2: drain staged → DB_WRITE_REQUESTED) ────
  // Triggered after WEBHOOK_EVENT_RECEIVED. The FSM:
  //   1. Looks up the staged canonical event
  //   2. Hydrates context (account UUID, media UUID, conversation UUID)
  //      via governed reads — the resolvers themselves are pure transforms
  //   3. Calls the matching resolver to produce { table, operation, rows }
  //   4. Emits DB_WRITE_REQUESTED for the constitutional kernel to route
  //      to persist-telemetry-fsm
  //
  // Failure modes (all leave the event staged for retry):
  //   - hydration returns error (e.g., account not resolved) → emit
  //     LOG_DEGRADED + WEBHOOK_EVENT_PERSIST_FAILED with reason
  //   - resolver returns { error } → same
  //   - resolve() returns null (wrong event type) → silent drop
  PERSIST_STAGED_EVENT: {
    target: 'STAGING',
    guard: (event) => {
      const l3 = _guardPayload(event);
      if (!l3.allowed) return l3;
      const staged = _getStagedEvent(event.accountId, event.intentId);
      if (!staged) {
        return { allowed: false, reason: `staged_event_not_found:${event.accountId}:${event.intentId}` };
      }
      // Inference engine gate: the FSM only accepts PERSIST_STAGED_EVENT
      // when the inferred state shows the worker has completed.
      const inferred = inferenceEngine.reduceInferredState(event.accountId, event.intentId);
      if (inferred === inferenceEngine.INFERRED.STAGED ||
          inferred === inferenceEngine.INFERRED.PERSIST_REQUESTED) {
        return { allowed: true, inferredState: inferred };
      }
      return {
        allowed: false,
        reason: `inferred_state_not_ready:${inferred}`,
        inferredState: inferred,
      };
    },
    buildActions: async (event, ctx) => {
      const { accountId, intentId, eventId } = event;
      const effectiveCtx = ctx || _governance;
      const staged = _getStagedEvent(accountId, intentId);
      if (!staged) {
        return [];
      }

      // Note: the INTERNAL:PERSIST_REQUESTED→PERSIST_ACKNOWLEDGED
      // transition is recorded LATER in this buildActions — only after
      // hydration, resolve, and the gate all pass. The intermediate
      // failure paths (FAILED_HYDRATION, FAILED_RESOLVE, FAILED_GATE)
      // move the state directly from PERSIST_REQUESTED to a terminal
      // failure state.

      // ── Hydrate context via governed reads ───────────────────────────
      const context = await _hydrateResolverContext(staged, effectiveCtx);
      if (context.error) {
        inferenceEngine.recordTransition(accountId, intentId, 'SUBSTRATE', 'PERSIST_REQUESTED', 'FAILED_HYDRATION');
        _emitSpan('WEBHOOK_EVENT_PERSIST_FAILED', intentId, accountId, staged.domain, {
          eventType: staged.eventType,
          reason: context.error,
          phase: 'hydration',
        });
        return [{
          type: 'WEBHOOK_EVENT_PERSIST_FAILED',
          accountId, intentId, eventId,
          error: context.error,
          phase: 'hydration',
        }];
      }

      // ── Resolve canonical event → DB row (pure transform) ────────────
      const result = resolvers.resolveForEvent(staged, context);
      if (!result) {
        // Resolver rejected the event (e.g., wrong type). Drop silently
        // after removing from staging.
        _removeStagedEvent(accountId, intentId);
        return [];
      }
      if (result.error) {
        inferenceEngine.recordTransition(accountId, intentId, 'SUBSTRATE', 'PERSIST_REQUESTED', 'FAILED_RESOLVE');
        _emitSpan('WEBHOOK_EVENT_PERSIST_FAILED', intentId, accountId, staged.domain, {
          eventType: staged.eventType,
          reason: result.error,
          phase: 'resolve',
        });
        return [{
          type: 'WEBHOOK_EVENT_PERSIST_FAILED',
          accountId, intentId, eventId,
          error: result.error,
          phase: 'resolve',
        }];
      }

      // ── Sanity gate (defense-in-depth, the universal gate) ───────────
      const gate = await _resolveSanityCheckWithTelemetry(effectiveCtx, {
        operation: 'db_write',
        accountId,
        table: result.table,
        intentId,
        rowCount: result.rows?.length || 0,
      }, null);
      if (!gate.allowed) {
        inferenceEngine.recordTransition(accountId, intentId, 'SUBSTRATE', 'PERSIST_REQUESTED', 'FAILED_GATE');
        return [{
          type: 'GATE_REJECTED',
          operation: 'db_write',
          accountId,
          table: result.table,
          intentId,
          reason: gate.reason || 'gate_rejected',
        }, {
          type: 'WEBHOOK_EVENT_PERSIST_FAILED',
          accountId, intentId, eventId,
          error: `gate_rejected:${gate.reason || 'gate_rejected'}`,
          phase: 'gate',
        }];
      }

      _emitSpan('WEBHOOK_EVENT_PERSIST_DISPATCHED', intentId, accountId, staged.domain, {
        eventType: staged.eventType,
        table: result.table,
        operation: result.operation,
        rowCount: result.rows?.length || 0,
      });

      // ── Internal transition: PERSIST_REQUESTED → PERSIST_ACKNOWLEDGED ──
      // The FSM is committing to dispatch; record the internal transition
      // BEFORE the action emit so any subsequent reader sees the state.
      inferenceEngine.recordTransition(accountId, intentId, 'INTERNAL', 'PERSIST_REQUESTED', 'PERSIST_ACKNOWLEDGED');
      // ── Internal transition: PERSIST_ACKNOWLEDGED → DB_WRITE_DISPATCHED ─
      inferenceEngine.recordTransition(accountId, intentId, 'INTERNAL', 'PERSIST_ACKNOWLEDGED', 'DB_WRITE_DISPATCHED');

      // ── Cross-kernel dispatch via CK (constitutional flow) ───────────
      const actions = [{
        type: 'DB_WRITE_REQUESTED',
        accountId,
        domain: 'persist-telemetry',
        intentId,
        table: result.table,
        operation: result.operation,
        rows: result.rows,
        lineageId: `webhook:${accountId}:${eventId}`,
        lineageDomain: 'acquisition-fsm',
        extra: {
          source: 'webhook',
          webhookEventType: staged.eventType,
        },
      }];

      // ── If a conversation upsert is required, emit a second write ───
      if (result.requiresConversationUpsert && result.conversationContext) {
        const conv = result.conversationContext;
        const convRow = {
          instagram_thread_id:    conv.instagram_thread_id,
          business_account_id:    conv.business_account_id,
          customer_instagram_id:  conv.customer_instagram_id,
          customer_username:      conv.customer_username || null,
          last_message_at:        new Date(staged.occurredAt || Date.now()).toISOString(),
          message_count:          1,
          conversation_status:    'active',
        };
        actions.push({
          type: 'DB_WRITE_REQUESTED',
          accountId,
          domain: 'persist-telemetry',
          intentId,
          table: 'instagram_dm_conversations',
          operation: 'batch_upsert_conversations',
          rows: [convRow],
          lineageId: `webhook-conv:${accountId}:${eventId}`,
          lineageDomain: 'acquisition-fsm',
          extra: { source: 'webhook', webhookEventType: staged.eventType },
        });
      }

      return actions;
    },
  },

  // ── WEBHOOK_EVENT_PERSISTED (Phase 2: write succeeded) ───────────────────
  // persist-telemetry-fsm emits DB_WRITE_COMPLETE; CK routes it here.
  // The FSM removes the staged event and emits the success span.
  WEBHOOK_EVENT_PERSISTED: {
    target: 'STAGING',
    guard: (event) => {
      const l3 = _guardPayload(event);
      if (!l3.allowed) return l3;
      // Inference engine gate: only accept the persisted signal if the
      // inferred state shows the write was actually dispatched.
      const inferred = inferenceEngine.reduceInferredState(event.accountId, event.intentId);
      if (inferred === inferenceEngine.INFERRED.DB_WRITE_DISPATCHED) {
        return { allowed: true, inferredState: inferred };
      }
      return {
        allowed: false,
        reason: `inferred_state_not_dispatched:${inferred}`,
        inferredState: inferred,
      };
    },
    buildActions: async (event) => {
      const { accountId, intentId, eventId, table, count } = event;
      // Record the terminal transition.
      inferenceEngine.recordTransition(accountId, intentId, 'INTERNAL', 'DB_WRITE_DISPATCHED', 'PERSISTED');
      const removed = _removeStagedEvent(accountId, intentId);
      // Housekeeping: drop the inference log for terminal intents.
      inferenceEngine.purgeIfTerminal(accountId, intentId);

      _emitSpan('WEBHOOK_EVENT_PERSISTED', intentId, accountId, null, {
        eventId,
        table,
        count,
        removed,
      });

      return [{
        type: 'WEBHOOK_EVENT_PERSISTED',
        accountId, intentId, eventId, table, count,
        removedFromStaging: removed,
      }];
    },
  },

  // ── WEBHOOK_EVENT_PERSIST_FAILED (Phase 2: write failed) ────────────────
  // persist-telemetry-fsm emits DB_WRITE_FAILED; CK routes it here.
  // The FSM leaves the event staged for retry (backoff controlled by
  // the persist-telemetry-fsm) and emits the failure span.
  WEBHOOK_EVENT_PERSIST_FAILED: {
    target: 'STAGING',
    guard: (event) => {
      const l3 = _guardPayload(event);
      if (!l3.allowed) return l3;
      // Accept only when the inferred state shows the write was dispatched.
      const inferred = inferenceEngine.reduceInferredState(event.accountId, event.intentId);
      if (inferred === inferenceEngine.INFERRED.DB_WRITE_DISPATCHED) {
        return { allowed: true, inferredState: inferred };
      }
      return {
        allowed: false,
        reason: `inferred_state_not_dispatched:${inferred}`,
        inferredState: inferred,
      };
    },
    buildActions: async (event, ctx) => {
      const { accountId, intentId, eventId, error, phase } = event;
      // Record the terminal transition.
      inferenceEngine.recordTransition(accountId, intentId, 'INTERNAL', 'DB_WRITE_DISPATCHED', 'PERSIST_FAILED');
      inferenceEngine.purgeIfTerminal(accountId, intentId);
      const errorString = typeof error === 'string' ? error : (error?.message || 'unknown');

      _emitSpan('WEBHOOK_EVENT_PERSIST_FAILED', intentId, accountId, null, {
        eventId,
        phase,
        error: errorString,
      });

      const actions = [{
        type: 'WEBHOOK_EVENT_PERSIST_FAILED',
        accountId, intentId, eventId,
        error: errorString,
        phase,
      }];

      // High-severity failure → escalate through CK
      if (phase === 'gate' || phase === 'resolve') {
        actions.push({
          type: 'LOG_DEGRADED',
          substate: 'WEBHOOK_PERSIST_FAILURE',
          reason: `webhook persist failed at ${phase}: ${errorString}`,
        });
      }

      return actions;
    },
  },

  // ═══════════════════════════════════════════════════════════════════════
  // INFERENCE ENGINE INPUTS — Phase 1
  // ═══════════════════════════════════════════════════════════════════════
  // The FSM records these transitions to the inference log; the reducer
  // computes the inferred state. No side effects, no dispatch. Pure
  // observation + recording.

  WORKER_STATE_TRANSITION: {
    target: () => _localState,  // global state is unrelated to per-intent state
    guard: (event) => {
      const l3 = _guardPayload(event);
      if (!l3.allowed) return l3;
      if (!event.from || !event.to) {
        return { allowed: false, reason: 'worker_transition_missing_from_or_to' };
      }
      return { allowed: true };
    },
    buildActions: async (event) => {
      const { accountId, intentId, from, to, eventType, domain, reason } = event;
      // Record to the inference log (the reducer will compute the new state).
      inferenceEngine.recordTransition(accountId, intentId, 'WORKER', from, to);
      // Validate the transition is legal from the inferred state.
      // If not, the transition is silently dropped (defensive: the log
      // already records the emission, but the inferred state doesn't move).
      const inferredBefore = inferenceEngine.reduceInferredState(accountId, intentId);
      const inferredAfter  = inferenceEngine.reduceInferredState(accountId, intentId);
      const isLegal = inferenceEngine.isLegalTransition(accountId, intentId, 'WORKER', from, to);
      if (process.env.WEBHOOK_DEBUG && !isLegal) {
        console.log(`[fsm] illegal worker transition: ${from}→${to} (inferred=${inferredBefore})`);
      }
      _emitSpan('WORKER_TRANSITION_RECORDED', intentId, accountId, domain, {
        from, to, eventType, reason: reason || null,
        inferredState: inferredAfter,
      });
      return [];
    },
  },

  SUBSTRATE_STATE_TRANSITION: {
    target: () => _localState,
    guard: (event) => {
      const l3 = _guardPayload(event);
      if (!l3.allowed) return l3;
      if (!event.from || !event.to) {
        return { allowed: false, reason: 'substrate_transition_missing_from_or_to' };
      }
      return { allowed: true };
    },
    buildActions: async (event) => {
      const { accountId, intentId, from, to, reason } = event;
      inferenceEngine.recordTransition(accountId, intentId, 'SUBSTRATE', from, to);
      const inferredAfter = inferenceEngine.reduceInferredState(accountId, intentId);
      const isLegal = inferenceEngine.isLegalTransition(accountId, intentId, 'SUBSTRATE', from, to);
      if (process.env.WEBHOOK_DEBUG && !isLegal) {
        console.log(`[fsm] illegal substrate transition: ${from}→${to} (inferred=${inferredAfter})`);
      }
      _emitSpan('SUBSTRATE_TRANSITION_RECORDED', intentId, accountId, null, {
        from, to, reason: reason || null, inferredState: inferredAfter,
      });
      return [];
    },
  },

  // ── INSIGHTS_POLL_FAILURE — cross-kernel failure intake (Phase 8) ──────
  // Emitted by insights-retry-worker on failure. The acquisition FSM
  // records the failure on the intent and escalates to CK for
  // authority-vector orchestration. CK queries GCK for token health
  // and quota state, decides retry vs defer vs token-refresh.
  INSIGHTS_POLL_FAILURE: {
    target: () => 'ACQUIRING',  // stays ACQUIRING — intent is still alive
    guard: (event) => {
      const l3 = _guardPayload(event);
      if (!l3.allowed) return l3;
      const rec = _intents.get(event.intentId);
      if (!rec) {
        return { allowed: false, reason: `intent_not_found:${event.intentId}` };
      }
      return { allowed: true };
    },
    buildActions: (event, ctx) => {
      const { accountId, intentId, domain, error, errorShape } = event;
      const rec = _intents.get(intentId);
      if (rec) {
        rec.lastFailureAt = Date.now();
        rec.lastFailureError = error || 'insights_poll_failure';
        _emitSpan('INSIGHTS_POLL_FAILURE_RECORDED', intentId, accountId, domain, {
          error: error || null,
          errorShape: errorShape || null,
        });
      }
      if (ctx && ctx.dispatchGlobal) {
        ctx.dispatchGlobal({
          type: 'CK_INSIGHTS_FAILURE_OBSERVED',
          accountId, intentId, domain,
          error: error || null,
          errorShape: errorShape || null,
        });
      }
      return [];
    },
  },

  // ── ACQUISITION_DEFER — CK-ordered deferral (Phase 8) ─────────────────
  // CK dispatches this when token health is unrecoverable or quota is
  // exhausted. The acquisition FSM records the deferral on the intent
  // and leaves it for cadence-tick re-evaluation.
  ACQUISITION_DEFER: {
    target: () => 'ACQUIRING',  // stays ACQUIRING — intent is deferred, not dead
    guard: (event) => {
      const l3 = _guardPayload(event);
      if (!l3.allowed) return l3;
      return { allowed: true };
    },
    buildActions: (event) => {
      const { accountId, intentId, domain, reason, capabilityState, quotaState } = event;
      const rec = intentId ? _intents.get(intentId) : null;
      if (rec) {
        rec.deferredAt = Date.now();
        rec.deferredReason = reason || 'unknown';
        _emitSpan('ACQUISITION_DEFERRED', intentId, accountId, domain, {
          reason: reason || null,
          capabilityState: capabilityState || null,
          quotaState: quotaState || null,
        });
      }
      return [];
    },
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// 6. Dispatch — process event, ask constitutional for validation, transition
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Process a domain event within the acquisition FSM.
 *
 * @param {{ type: string, [key: string]: any }} event — domain event
 * @param {{ validate: Function, dispatchGlobal: Function, getGlobalState: Function }} ctx — constitutional kernel context
 * @returns {{ allowed: boolean, from?: string, to?: string, actions?: Array, reason?: string }}
 */
async function _syncProjectionState() {
  try {
    const { getRedisClient } = require('../config/redis');
    const redis = getRedisClient();
    if (redis && redis.status === 'ready') {
      const raw = await redis.get('lineage:projection:domain:acquisition');
      if (raw) {
        const parsed = JSON.parse(raw);
        if (parsed && parsed.projection && parsed.projection.state) {
          if (typeof _localState !== 'undefined') {
            _localState = parsed.projection.state;
          }
        }
      }
    }
  } catch (_) {}
}

async function dispatch(event, ctx) {
  if (!event || typeof event !== 'object' || typeof event.type !== 'string') {
    return { allowed: false, reason: `event must be { type: string }, got ${typeof event}` };
  }

  // Sync with lineage-projected namespace state before processing event
  await _syncProjectionState();

  // Fall back to the bound governance ref (set by setGovernance at boot)
  // when the caller does not pass an explicit ctx. This keeps the existing
  // ctx.sanityCheck + ctx.validate contract working for direct dispatches
  // that don't thread their own ctx.
  const effectiveCtx = ctx || _governance;

  const txn = TRANSITION_MAP[event.type];
  if (!txn) {
    return { allowed: false, reason: `unknown event type: ${event.type}` };
  }

  const from = _localState;

  // 1. Run per-transition guard
  if (txn.guard) {
    const result = txn.guard(event);
    if (!result.allowed) {
      return { allowed: false, reason: result.reason || 'guard blocked' };
    }
  }

  // 2. Build actions FIRST (before state mutation) — ACQUISITION_COMPLETE target is derived
  const actions = (txn.buildActions ? await txn.buildActions(event, effectiveCtx) : []);

  // 3. Derive target state AFTER actions (ACQUISITION_COMPLETE may close the last intent)
  const newGlobalState = _deriveGlobalState();
  const rawTarget = txn.target;
  let target;
  if (rawTarget === null) {
    target = newGlobalState;
  } else if (typeof rawTarget === 'function') {
    // Legacy: dynamic target function — call with new state
    target = rawTarget(event);
  } else {
    target = rawTarget;
  }

  // 4. No-op if state hasn't changed
  if (target === from && actions.length === 0) {
    return { allowed: true, from, to: from, actions: [], reason: 'no-transition' };
  }

  // 5. Ask constitutional kernel for transition approval
  if (effectiveCtx && effectiveCtx.validate) {
    const validation = effectiveCtx.validate(from, target, event);
    if (!validation.allowed) {
      return { allowed: false, reason: validation.reason || 'constitutional validation failed' };
    }
  }

  // 6. Materialize state
  _localState = target;
  _lastTransitionedAt = Date.now();

  // 7. Emit observability transition for domain FSM state change
  try {
    const obs = _obs();
    if (obs) {
      obs.transition({
        domain: 'acquisition',
        entity: 'fsm',
        entityId: 'acquisition-fsm',
        previousState: from,
        nextState: target,
        authority: 'acquisition-fsm',
        raw: { intent: event.type, intentId: event.intentId || null, accountId: event.accountId || null },
      });
    }
  } catch (_) {}

  return {
    allowed: true,
    from,
    to: target,
    actions,
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// 7. Timeout sweeper — force-closes stale intents
// ═══════════════════════════════════════════════════════════════════════════════

let _sweepIntervalHandle = null;

function _sweepTimeouts() {
  const now = Date.now();
  const parseDeadline = now - _timeoutConfig.parseTimeoutMs;
  const intentDeadline = now - _timeoutConfig.intentTimeoutMs;
  const closed = [];

  for (const [intentId, rec] of _intents) {
    if (rec.currentPhase === 'CLOSED') {
      // Remove closed intents older than 5 minutes
      if (rec.lastTransitionAt < intentDeadline) {
        _intents.delete(intentId);
        closed.push(intentId);
      }
      continue;
    }

    // Force-close if total intent age exceeded
    if (rec.intakeAt < intentDeadline) {
      rec.outcome = 'failed';
      rec.failureReason = { code: 'intent_timeout', source: 'timeout_sweeper' };
      rec.currentPhase = 'CLOSED';
      rec.lastTransitionAt = now;
      _emitSpan('INTENT_TIMEOUT', intentId, rec.accountId, rec.domain, {
        ageMs: now - rec.intakeAt,
        thresholdMs: _timeoutConfig.intentTimeoutMs,
      });
      closed.push(intentId);
      continue;
    }

    // Flag stale parse (parsing phase overdue) but do not force-close — let the
    // PARSING_COMPLETE event arrive naturally; sweeper only closes on total timeout.
    if (rec.currentPhase === 'PARSING' && rec.parsingDispatchedAt && rec.parsingDispatchedAt < parseDeadline) {
      // Stale signal goes into health, not a forced close
    }
  }

  return closed;
}

function startTimeoutSweeper(intervalMs = 30_000) {
  if (_sweepIntervalHandle) clearInterval(_sweepIntervalHandle);
  _sweepIntervalHandle = setInterval(_sweepTimeouts, intervalMs);
  // Immediate first sweep
  _sweepTimeouts();
}

function stopTimeoutSweeper() {
  if (_sweepIntervalHandle) {
    clearInterval(_sweepIntervalHandle);
    _sweepIntervalHandle = null;
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// 8. Gate telemetry surface
// ═══════════════════════════════════════════════════════════════════════════════

function getGateTelemetry() {
  const now = Date.now();
  const windowStart = now - _timeoutConfig.gateVetoWindowMs;
  let totalVetoes = 0;
  const vetoesByReason = {};
  const vetoesByOp = {};
  let lastVetoAt = null;
  let lastVetoIntentId = null;

  for (const rec of _intents.values()) {
    const vetoes = rec.gateVetoes?._items || [];
    for (const v of vetoes) {
      totalVetoes++;
      vetoesByReason[v.reason] = (vetoesByReason[v.reason] || 0) + 1;
      vetoesByOp[v.op] = (vetoesByOp[v.op] || 0) + 1;
      if (!lastVetoAt || v.at > lastVetoAt) {
        lastVetoAt = v.at;
        lastVetoIntentId = rec.intentId;
      }
    }
  }

  const vetoRate = _timeoutConfig.gateVetoWindowMs > 0
    ? totalVetoes / (_timeoutConfig.gateVetoWindowMs / 1000)
    : 0;

  return {
    totalVetoes,
    vetoesByReason,
    vetoesByOp,
    vetoRate,
    lastVetoAt,
    lastVetoIntentId,
    windowMs: _timeoutConfig.gateVetoWindowMs,
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// 9. Intent snapshot (reconciliation / observability consumers)
// ═══════════════════════════════════════════════════════════════════════════════

function getIntentSnapshot(intentId) {
  const rec = _intents.get(intentId);
  if (!rec) return null;
  return {
    intentId:       rec.intentId,
    accountId:      rec.accountId,
    domain:         rec.domain,
    intakeAt:       rec.intakeAt,
    currentPhase:   rec.currentPhase,
    lastTransitionAt: rec.lastTransitionAt,
    outcome:        rec.outcome,
    failureReason:  rec.failureReason,
    gateVetoCount:  rec.gateVetoes?._items?.length || 0,
    rawCount:       rec.rawCount,
    history:        [...(rec.history?._items || [])],
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// 10. Initialization
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Initialize the domain FSM with rehydrated state from lineage.
 * @param {string} rehydratedState — 'ACQUIRING' or 'IDLE'
 * @param {object} [opts] — { rehydratedIntents: IntentRecord[] }
 */
function init(rehydratedState, opts = {}) {
  if (rehydratedState && typeof rehydratedState === 'string') {
    _localState = rehydratedState;
    console.log(`[acquisition-fsm] Initialized with rehydrated state: ${rehydratedState}`);
  }
  if (opts.rehydratedIntents && Array.isArray(opts.rehydratedIntents)) {
    for (const rec of opts.rehydratedIntents) {
      if (rec.intentId) {
        _intents.set(rec.intentId, {
          ...rec,
          history:    _makeRing(_timeoutConfig.historyRingSize),
          gateVetoes: _makeRing(_timeoutConfig.gateVetoRingSize),
        });
      }
    }
    console.log(`[acquisition-fsm] Rehydrated ${opts.rehydratedIntents.length} intent records`);
  }
}

function clearIntents() {
  _intents.clear();
  _fingerprintDedup.clear();
  _localState = 'IDLE';
}

// ═══════════════════════════════════════════════════════════════════════════════
// 11. Observability — domain state queries
// ═══════════════════════════════════════════════════════════════════════════════

function getState() {
  return _localState;
}

function exportState() {
  return {
    state: _localState,
    activeIntents: Array.from(_intents.values())
      .filter((r) => r.currentPhase !== 'CLOSED')
      .length,
    closedIntents: Array.from(_intents.values())
      .filter((r) => r.currentPhase === 'CLOSED')
      .length,
  };
}

function getHealth() {
  const now = Date.now();
  const parseDeadline = now - _timeoutConfig.parseTimeoutMs;
  const intentDeadline = now - _timeoutConfig.intentTimeoutMs;
  const vetoWindowStart = now - _timeoutConfig.gateVetoWindowMs;

  const allRecs = Array.from(_intents.values());
  const activeRecs = allRecs.filter((r) => r.currentPhase !== 'CLOSED');
  const parsingRecs = allRecs.filter((r) => r.currentPhase === 'PARSING');
  const staleParseRecs = parsingRecs.filter(
    (r) => r.parsingDispatchedAt && r.parsingDispatchedAt < parseDeadline
  );

  // Intent age stats
  let oldestIntentAgeMs = 0;
  let lastSuccessAt = null;
  let lastFailureAt = null;
  for (const rec of allRecs) {
    const age = now - rec.intakeAt;
    if (age > oldestIntentAgeMs) oldestIntentAgeMs = age;
    if (rec.outcome === 'success' && (!lastSuccessAt || rec.lastTransitionAt > lastSuccessAt)) {
      lastSuccessAt = rec.lastTransitionAt;
    }
    if (rec.outcome === 'failed' && (!lastFailureAt || rec.lastTransitionAt > lastFailureAt)) {
      lastFailureAt = rec.lastTransitionAt;
    }
  }

  // Gate veto rate (last window)
  let recentVetoes = 0;
  for (const rec of allRecs) {
    const vetoes = rec.gateVetoes?._items || [];
    for (const v of vetoes) {
      if (v.at >= vetoWindowStart) recentVetoes++;
    }
  }
  const gateVetoRate = _timeoutConfig.gateVetoWindowMs > 0
    ? recentVetoes / (_timeoutConfig.gateVetoWindowMs / 1000)
    : 0;

  // Last veto
  let lastVetoAt = null;
  for (const rec of allRecs) {
    const vetoes = rec.gateVetoes?._items || [];
    for (const v of vetoes) {
      if (!lastVetoAt || v.at > lastVetoAt) lastVetoAt = v.at;
    }
  }

  // Derive health flags
  const flags = [];
  if (staleParseRecs.length > 0)    flags.push('stale_parse');
  if (gateVetoRate > 0.5)           flags.push('gate_storm');
  if (activeRecs.length > 50)       flags.push('intent_leak');
  if (activeRecs.length > 0 && oldestIntentAgeMs > _timeoutConfig.intentTimeoutMs) {
    flags.push('intent_timeout');
  }
  if (activeRecs.length === 0 && allRecs.length > 0 && !lastSuccessAt && !lastFailureAt) {
    // Nothing completing — check for stuck
  }

  return {
    ok: flags.length === 0,
    signals: {
      activeIntents:    activeRecs.length,
      parsingInFlight: parsingRecs.length,
      staleParses:      staleParseRecs.length,
      oldestIntentAgeMs,
      gateVetoRate:     parseFloat(gateVetoRate.toFixed(4)),
      lastSuccessAt,
      lastFailureAt,
      lastVetoAt,
      healthFlags: flags,
    },
  };
}

// ── Reconciliation engine getters ────────────────────────────────────────────

function getLastTransitionedAt() {
  return _lastTransitionedAt;
}

function getPendingParsing() {
  // Backward compat — derived from _intents
  return Array.from(_intents.values())
    .filter((r) => r.currentPhase === 'PARSING')
    .map((r) => ({
      intentId:   r.intentId,
      jobId:      r.parsingJobId,
      domain:     r.domain,
      accountId:  r.accountId,
      rawCount:   r.rawCount,
      ageMs:     Date.now() - (r.parsingDispatchedAt || r.intakeAt),
    }));
}

// ── Span query (observability consumers) ─────────────────────────────────────

function getSpan(intentId) {
  const rec = _intents.get(intentId);
  if (!rec) return null;
  return {
    intentId:       rec.intentId,
    accountId:      rec.accountId,
    domain:         rec.domain,
    intakeAt:       rec.intakeAt,
    executionAt:    rec.executionDispatchedAt,
    parsingAt:      rec.parsingDispatchedAt,
    completeAt:     rec.outcome ? rec.lastTransitionAt : null,
    durationMs:     rec.executionDispatchedAt
      ? (rec.lastTransitionAt - rec.executionDispatchedAt)
      : (rec.lastTransitionAt - rec.intakeAt),
    parseDurationMs: rec.parsingDispatchedAt
      ? (rec.lastTransitionAt - rec.parsingDispatchedAt)
      : null,
    totalDurationMs: rec.intakeAt ? rec.lastTransitionAt - rec.intakeAt : null,
    outcome:        rec.outcome,
    failureReason:  rec.failureReason,
    gateVetoCount:  rec.gateVetoes?._items?.length || 0,
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// 12. Module exports
// ═══════════════════════════════════════════════════════════════════════════════

module.exports = {
  name: 'acquisition',
  // Standard FSM contract
  setGovernance,
  getGovernance,
  dispatch,
  init,
  clearIntents,
  getState,
  exportState,
  getHealth,
  getLastTransitionedAt,
  getPendingParsing,
  getGateTelemetry,
  getIntentSnapshot,
  getSpan,
  // Phase 1: webhook event staging surface
  getStagedEvents,
  clearStagedEvents,
  setTimeoutConfig,
  getTimeoutConfig,
  startTimeoutSweeper,
  stopTimeoutSweeper,
  // Phase 2: deterministic inference engine (read-only access)
  inferenceEngine,
};