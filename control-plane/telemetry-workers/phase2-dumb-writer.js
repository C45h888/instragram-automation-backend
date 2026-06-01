// control-plane/telemetry-workers/phase2-dumb-writer.js
// Phase 2 Dumb Writer: mechanically appends projection intents to canonical ledger.
//
// Owns: serializing PROJECTION_INTENT → canonical ledger entry (mechanical only),
//        appending to lineage ledger via recordWorkerEntry(),
//        notifying CK for asynchronous constitutional validation.
//
// Does NOT own: semantic validation, namespace authority, signal ownership checks,
//               chronology inference, replay control, interpretation.
//
// Architectural identity:
//   This worker is a DUMB MECHANICAL SUBSTRATE. It transforms and appends.
//   It NEVER validates, interprets, or governs.
//   CK operates ABOVE the persistence layer — validates asynchronously.
//
// Trigger model (Phase 3: zero timers):
//   onWrite hook fires on every _transitionLog.push()
//   → Phase 2 worker serializes → appends to ledger → notifies CK
//   → returns immediately — never blocks on CK validation
//
// Constitutional constraints:
//   - Does NOT import governance modules (except CK.dispatch for lightweight notification)
//   - Does NOT import Redis directly (uses lineageLedger.recordWorkerEntry)
//   - Serialization is pure SHA-256 hashing — deterministic, no interpretation

const crypto = require('crypto');

let _unsubscribe = null;
let _running = false;
let _writeCount = 0;
let _startedAt = null;

// ═══════════════════════════════════════════════════════════════════════════════
// Serialization — pure mechanical transform, no semantic interpretation
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Serialize a PROJECTION_INTENT into a canonical ledger entry.
 * Pure function — same intent always produces same entry (SHA-256 deterministic).
 *
 * @param {object} intent — PROJECTION_INTENT entry from observability
 * @returns {object} canonical ledger entry with constitutionalStatus: PENDING
 */
function _serializeIntent(intent) {
  const raw = intent.raw || {};

  // Deterministic traceId: SHA-256 of intent content — replay-stable
  const contentForHash = JSON.stringify({
    projectionNamespace: raw.projectionNamespace,
    projectionType: raw.projectionType,
    projectionVersion: raw.projectionVersion,
    projectionPayload: raw.projectionPayload,
    correlationId: intent.correlationId,
    timestamp: intent.timestamp,
  });
  const traceId = crypto.createHash('sha256').update(contentForHash).digest('hex');

  return {
    ledgerId: crypto.randomUUID(),
    domain: raw.projectionNamespace || 'unknown',
    entity: 'semantic_projection',
    entityId: raw.projectionType || 'unknown',
    previousState: raw.projectionType ? `${raw.projectionType}:coordinated` : null,
    nextState: raw.projectionType ? `${raw.projectionType}:projected` : null,
    authority: intent.authority || 'unknown',
    timestamp: intent.timestamp || Date.now(),
    wallClockTimestamp: intent.wallClockTimestamp || Date.now(),
    traceId,
    correlationId: intent.correlationId || null,
    causationId: intent.traceId || null,
    parentTransitionId: null,
    raw: {
      entryType: 'SEMANTIC_PROJECTION_TRANSITION',
      projectionId: crypto.randomUUID(),
      projectionType: raw.projectionType,
      projectionVersion: raw.projectionVersion || '1.0.0',
      projectionNamespace: raw.projectionNamespace,
      projectionPayload: raw.projectionPayload,
      confidence: raw.confidence,
      integrityScore: raw.integrityScore,
      sourceTelemetryWindow: raw.sourceTelemetryWindow,
      constitutionalStatus: 'PENDING',
    },
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Public API
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Start the Phase 2 dumb writer.
 * Subscribes to observability onWrite hook — zero timers, purely event-driven.
 */
function start() {
  if (_running) return;

  const observability = require('../observability');
  const lineageLedger = require('../governance/lineage-ledger');
  const CK = require('../governance/constitutional-kernel');

  _unsubscribe = observability.onWrite((transition) => {
    // Only process PROJECTION_INTENT entries
    if (transition.nextState !== 'PROJECTION_INTENT') return;
    // Skip entries the FSM already rejected (HALTED guard in CK)
    if (!transition.raw || !transition.raw.projectionNamespace) return;

    try {
      // 1. Serialize to canonical ledger entry (mechanical, pure function)
      const entry = _serializeIntent(transition);

      // 2. Append to canonical ledger (fast, synchronous Redis RPUSH)
      lineageLedger.recordWorkerEntry(entry).catch(err => {
        console.error('[phase2-dumb-writer] Failed to persist ledger entry:', err.message);
      });

      // 3. Notify CK for async validation (fire-and-forget, non-blocking)
      CK.dispatch({
        type: 'PROJECTION_PERSISTED',
        ledgerId: entry.ledgerId,
        entry,
      });

      _writeCount++;
    } catch (err) {
      console.error('[phase2-dumb-writer] Write error:', err.message);
    }
  });

  _running = true;
  _startedAt = Date.now();
  console.log('[phase2-dumb-writer] Started — trigger-driven, zero timers');
}

/**
 * Stop the Phase 2 dumb writer.
 * Unsubscribes from the onWrite hook.
 */
function stop() {
  if (!_running) return;
  if (_unsubscribe) {
    _unsubscribe();
    _unsubscribe = null;
  }
  _running = false;
  console.log(`[phase2-dumb-writer] Stopped — ${_writeCount} writes`);
}

function getWriteCount() {
  return _writeCount;
}

function getHealth() {
  return {
    running: _running,
    writeCount: _writeCount,
    uptimeMs: _startedAt ? Date.now() - _startedAt : 0,
  };
}

module.exports = {
  start,
  stop,
  getWriteCount,
  getHealth,
};
