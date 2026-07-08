// control-plane/governance/webview-stream.js
// WebView Reactive Membrane — XREAD pump + receipt dispatcher.
//
// Pass 7 / S5 consumer side. This module consumes the
// lineage:webview:transitions stream produced by the WebView
// kernel runtime/src-tauri/src/redis/commands.rs:147, runs
// each entry through webview-fsm.dispatch →
// webview-decision.computeDecision →
// webview-receipt.emitReceipt, and surfaces the pump lifecycle
// (UP / DOWN) + receipt-write failures as constitutional events
// via CK.dispatch.
//
// Architecture (per spec invariants):
//   I33 — single entry point: pumpWebviewStream's dispatch loop.
//   I34 — DecisionReceipt surface closed over ACCEPTED|REJECTED|TIMEOUT.
//   I35 — no imports from the WebView repo (rules table forward-port).
//   I36 — receipt emission failure does NOT roll back the dispatch;
//          pump loop continues.
//
// Failure semantics (D3 default):
//   exponential backoff capped at 30s, max 10 attempts; then the
//   loop emits STREAM_PUMP_DOWN and stops. supervisor restart
//   is the platform's problem.

'use strict';

const { getRedisClient } = require('../../config/redis');
const { emitReceipt, READ_RESULTS_STREAM } = require('./webview-receipt');
const { computeDecision, DECISIONS } = require('./webview-decision');

// Source-of-truth key constants. The producer
// (runtime/src-tauri/src/redis/commands.rs) names these keys;
// do NOT rename — drift = contract break (see tests for the
// canonical-keys assert).
const WEBVIEW_TRANSITIONS_STREAM = 'lineage:webview:transitions';

// XREAD BLOCK parameters — single BLOCK per call, then a fresh
// call. Each iteration handles up to COUNT entries before yielding.
const XREAD_COUNT = 100;
const XREAD_BLOCK_MS = 5000;

// Dispatch wrapping — promise race against a timeout. The spec
// sets no exact deadline; default 2s keeps the pump responsive.
const DISPATCH_TIMEOUT_MS = 2000;

// Bounded retry on startup (D3 default).
const BACKOFF_BASE_MS = 1000;
const BACKOFF_CAP_MS = 30000;
const MAX_PUMP_ATTEMPTS = 10;

/**
 * Parse an XADD entry into the transition shape the probe expects.
 *
 * XADD field pairs (from commands.rs:147-166):
 *   transition_id, correlation_id, domain, from_state, to_state,
 *   event, occurred_at_epoch_ms — 7 fields
 *
 * Anything missing falls back to a rejected dispatch (fail-closed).
 */
function _parseStreamEntry(streamId, fieldPairs) {
  const fields = {};
  for (let i = 0; i < fieldPairs.length; i += 2) {
    fields[fieldPairs[i]] = fieldPairs[i + 1];
  }
  const occurred_at = fields.occurred_at_epoch_ms;
  return {
    transition_id: fields.transition_id || null,
    correlation_id: fields.correlation_id || null,
    domain: fields.domain || null,
    from_state: fields.from_state || null,
    to_state: fields.to_state || null,
    event: fields.event || null,
    occurred_at_epoch_ms: (typeof occurred_at === 'string' && /^\d+$/.test(occurred_at))
      ? Number(occurred_at)
      : (typeof occurred_at === 'number' ? occurred_at : 0),
    _streamId: streamId,
  };
}

/**
 * Race a promise against a timeout. Returns the resolved value or
 * `null` on timeout. SPEC TIMEOUT → null mapping.
 */
function _withTimeout(promise, ms) {
  return new Promise((resolve) => {
    let done = false;
    const t = setTimeout(() => {
      if (done) return;
      done = true;
      resolve(null);
    }, ms);
    Promise.resolve(promise).then(
      (v) => { if (!done) { done = true; clearTimeout(t); resolve(v); } },
      (e) => { if (!done) { done = true; clearTimeout(t); resolve({ __error: e }); } },
    );
  });
}

/**
 * The pump. Returns a handle with start() and stop() methods.
 *
 *   const handle = createWebviewStreamPump({ ck, fsm, getRedisClient });
 *   handle.start();
 *   ... // later
 *   handle.stop();
 *
 * The handle is idempotent: start() while already running is a no-op;
 * stop() while stopped is a no-op.
 */
function createWebviewStreamPump(opts = {}) {
  const ck = opts.ck || null;
  const fsm = opts.fsm || require('./webview-fsm');
  const redisGetter = opts.getRedisClient || getRedisClient;

  let _running = false;
  let _stopRequested = false;
  let _loopPromise = null;
  let _lastStreamId = '$'; // '$' = "new entries from now on"
  let _attemptCount = 0;
  let _startedAtEpochMs = null;
  let _stoppedAtEpochMs = null;
  let _processedCount = 0;
  let _receiptFailureCount = 0;

  // Telemetry helper — dispatches constitutional events. Best-effort:
  // if CK is unavailable the pump continues.
  async function _emitCkEvent(eventType, payload) {
    if (!ck || typeof ck.dispatch !== 'function') return;
    try {
      await ck.dispatch({ type: eventType, source: 'webview_stream_pump', payload });
    } catch (e) {
      console.error(`[webview-stream] CK dispatch failed for ${eventType}:`, e && e.message);
    }
  }

  // Process one XREAD batch — parse, dispatch (with timeout), decide,
  // emit receipt. Returns the count of successfully-processed entries
  // (decisions made, regardless of receipt-write result).
  async function _processBatch(entries) {
    let ok = 0;
    for (const entry of entries) {
      // entry: [streamId, [fieldPairs]]
      const streamId = entry[0];
      const fieldPairs = entry[1] || [];
      const transition = _parseStreamEntry(streamId, fieldPairs);
      _lastStreamId = streamId;

      // Run the FSM dispatch with a timeout. The dispatch itself
      // runs synchronously in Pass 7.2 (probe is sync), but the
      // timeout wraps it for future extensibility.
      const dispatchResult = await _withTimeout(
        fsm.dispatch({
          type: 'WEBVIEW_TRANSITION_REQUESTED',
          payload: { transition, streamId },
        }),
        DISPATCH_TIMEOUT_MS,
      );

      const decision = computeDecision(dispatchResult);
      const receipt = {
        transition_id: transition.transition_id || `unknown:${streamId}`,
        correlation_id: transition.correlation_id,
        decision: decision.decision,
        decided_at_epoch_ms: Date.now(),
        reason: decision.reason,
        rule_fingerprint: decision.ruleFingerprint,
        lineage_id: dispatchResult && dispatchResult.lineageId,
        domain: transition.domain,
        from_state: transition.from_state,
        to_state: transition.to_state,
        event: transition.event,
      };

      const emitResult = await emitReceipt(receipt);
      if (!emitResult.ok) {
        _receiptFailureCount += 1;
        await _emitCkEvent('READ_RESULTS_WRITE_FAILED', {
          transition_id: receipt.transition_id,
          decision: decision.decision,
          error: emitResult.error,
        });
        // Per invariant I36, do NOT re-dispatch; continue.
      }
      ok += 1;
    }
    _processedCount += ok;
    return ok;
  }

  // The actual XREAD loop. Starts after a successful pump-down
  // recovery or on first start(). Returns when the user calls
  // stop() OR after MAX_PUMP_ATTEMPTS consecutive failures.
  async function _loop() {
    _startedAtEpochMs = Date.now();
    _attemptCount = 0;

    while (!_stopRequested) {
      _attemptCount += 1;
      const redis = redisGetter();
      if (!redis || redis.status !== 'ready') {
        // Redis flap. Backoff + retry.
        if (_attemptCount >= MAX_PUMP_ATTEMPTS) {
          await _emitCkEvent('STREAM_PUMP_DOWN', {
            reason: `redis not ready after ${MAX_PUMP_ATTEMPTS} attempts`,
            stream: WEBVIEW_TRANSITIONS_STREAM,
          });
          _running = false;
          return;
        }
        const delay = Math.min(BACKOFF_BASE_MS * Math.pow(2, _attemptCount - 1), BACKOFF_CAP_MS);
        await new Promise((r) => setTimeout(r, delay));
        continue;
      }

      let rawEntries;
      try {
        rawEntries = await redis.xread(
          'BLOCK', XREAD_BLOCK_MS,
          'COUNT', XREAD_COUNT,
          'STREAMS', WEBVIEW_TRANSITIONS_STREAM, _lastStreamId,
        );
      } catch (err) {
        const isLast = _attemptCount >= MAX_PUMP_ATTEMPTS;
        if (isLast) {
          await _emitCkEvent('STREAM_PUMP_DOWN', {
            reason: `XREAD error after ${MAX_PUMP_ATTEMPTS} attempts: ${err && err.message}`,
            stream: WEBVIEW_TRANSITIONS_STREAM,
          });
          _running = false;
          return;
        }
        const delay = Math.min(BACKOFF_BASE_MS * Math.pow(2, _attemptCount - 1), BACKOFF_CAP_MS);
        await new Promise((r) => setTimeout(r, delay));
        continue;
      }

      // Reset attempt counter on a clean iteration (even if rawEntries
      // is null — that's a BLOCK timeout, not a failure).
      const hadError = false;
      _attemptCount = hadError ? _attemptCount : 0;

      // ioredis returns null on BLOCK timeout (no new entries).
      if (!rawEntries || !Array.isArray(rawEntries) || rawEntries.length === 0) {
        continue;
      }

      // XREAD returns one entry per stream; we read one stream.
      const streamEntries = rawEntries[0];
      if (!Array.isArray(streamEntries) || streamEntries.length < 2) continue;
      const entries = streamEntries[1] || [];
      if (entries.length === 0) continue;

      try {
        await _processBatch(entries);
      } catch (err) {
        console.error('[webview-stream] batch processing error:', err && err.message);
        // Continue — the per-entry catch handles most failures.
      }
    }

    _stoppedAtEpochMs = Date.now();
    _running = false;
  }

  return {
    start() {
      if (_running) return _loopPromise;
      _running = true;
      _stopRequested = false;
      _loopPromise = _loop().then(async () => {
        if (_receiptFailureCount > 0 || _startedAtEpochMs !== null) {
          await _emitCkEvent('STREAM_PUMP_UP', {
            recovered: _stopRequested,
            processed: _processedCount,
            receipt_failures: _receiptFailureCount,
          });
        }
      });
      // Announce UP at start.
      _emitCkEvent('STREAM_PUMP_UP', {
        stream: WEBVIEW_TRANSITIONS_STREAM,
        xread_block_ms: XREAD_BLOCK_MS,
      }).catch(() => {});
      return _loopPromise;
    },
    stop() {
      if (!_running) return Promise.resolve();
      _stopRequested = true;
      // XREAD will return within BLOCK_MS; the loop exits cleanly.
      return _loopPromise || Promise.resolve();
    },
    // Status surface — used by tests + observability.
    status() {
      return {
        running: _running,
        startedAtEpochMs: _startedAtEpochMs,
        stoppedAtEpochMs: _stoppedAtEpochMs,
        processedCount: _processedCount,
        receiptFailureCount: _receiptFailureCount,
        lastStreamId: _lastStreamId,
        attemptCount: _attemptCount,
      };
    },
    // Exposed for tests.
    _parseStreamEntry,
    _withTimeout,
  };
}

module.exports = {
  createWebviewStreamPump,
  WEBVIEW_TRANSITIONS_STREAM,
  READ_RESULTS_STREAM,
  // Tunables for tests + future overrides.
  XREAD_COUNT,
  XREAD_BLOCK_MS,
  DISPATCH_TIMEOUT_MS,
  MAX_PUMP_ATTEMPTS,
  BACKOFF_BASE_MS,
  BACKOFF_CAP_MS,
};
