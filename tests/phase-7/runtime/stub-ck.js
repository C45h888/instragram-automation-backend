// tests/phase-7/runtime/stub-ck.js
// Minimal CK stub for tests that need to verify dispatch flows without
// the full constitutional kernel.
//
// The real CK dispatches events into a real FSM chain. For unit tests
// we want to:
//   (a) record every dispatch call the stub receives,
//   (b) forward CAPABILITY_DATA_REQUEST to a real graph-capability FSM
//       and emulate a cache-miss so detectDynamic's post-flight
//       DB_WRITE_REQUESTED actually fires, and
//   (c) keep the stub trivially teardownable so per-test state does
//       not leak between tests.
//
// FIX (2026-06-14): default permissive behavior removed. Any event type
// not explicitly handled now returns { allowed: false, reason } so tests
// cannot accidentally pass when CK would have rejected. Tests that need
// permissive stubs for a specific event type must explicitly register
// an override (see HANDLED_EVENTS below).
//
// API:
//   const { stubCk, teardown } = createStubCk({ realFsm });
//   stubCk.dispatch(event)   — records and routes per the table below
//   stubCk.findEvents(type)  — returns all recorded events with that type
//   stubCk.clearEvents()     — wipes the recorded list
//   stubCk.allow(type)       — mark a specific event type as allowed
//   stubCk.allowAll()        — restore the old permissive default (use sparingly)
//   teardown()               — clears any governance ref the FSM stashed

'use strict';

function createStubCk({ realFsm } = {}) {
  const events = [];
  let governanceOwner = null;
  // Explicit allowlist — events not listed here are rejected.
  // Add overrides via stubCk.allow() or stubCk.allowAll().
  const _allowed = new Set();

  // Inner ctx the stub hands back to the FSM. The FSM's CAPABILITY_
  // DATA_REQUEST handler calls ctx.dispatchGlobal to forward a
  // DB_READ_REQUESTED. We route that back through the FSM so the
  // pending read is tracked and the Promise's resolve/reject fires
  // when the real flow completes.
  const innerCtx = {
    dispatchGlobal(sub) {
      events.push({ type: 'CTX_DISPATCH_GLOBAL', sub });
      if (sub && sub.type === 'DB_READ_REQUESTED' && realFsm) {
        return realFsm.dispatch(
          {
            type: 'READ_RESULT_AVAILABLE',
            businessAccountId: sub.accountId,
            accountId: sub.accountId,
            readId: sub.readId,
            readDomain: sub.readDomain,
            data: { scope_cache: null, scope_cache_updated_at: null },
          },
          innerCtx
        );
      }
      return { allowed: false, reason: `stub-ck: dispatchGlobal unhandled type: ${sub?.type}` };
    },
    validate() {
      return { allowed: true };
    },
    getGlobalState() {
      return null;
    },
    sanityCheck() {
      return { allowed: true };
    },
  };

  function wireGovernance() {
    if (realFsm && typeof realFsm.setGovernance === 'function') {
      realFsm.setGovernance(stubCk);
      governanceOwner = realFsm;
    }
  }

  const stubCk = {
    /**
     * Record an event and route it through the inner ctx if the
     * stub is playing the role of the constitutional ingress.
     * FIX (2026-06-14): explicit allowlist instead of permissive default.
     */
    dispatch(event) {
      if (!event || typeof event !== 'object') {
        return { allowed: false, reason: 'invalid_event' };
      }
      events.push(event);

      if (
        realFsm &&
        event.type === 'CAPABILITY_DATA_REQUEST' &&
        typeof realFsm.dispatch === 'function'
      ) {
        return realFsm.dispatch(event, innerCtx);
      }

      if (
        realFsm &&
        event.type === 'DB_READ_REQUESTED' &&
        typeof realFsm.dispatch === 'function'
      ) {
        return realFsm.dispatch(event, innerCtx);
      }

      // Reject unless the event type is explicitly allowlisted.
      // Tests must call stubCk.allow('EVENT_TYPE') for types they need.
      if (!_allowed.has(event.type)) {
        return {
          allowed: false,
          reason: `stub-ck: unhandled event type '${event.type}' — call stubCk.allow('${event.type}') to permit it`,
        };
      }
      return { allowed: true };
    },

    /**
     * Mark an event type as allowed through the stub CK.
     */
    allow(type) {
      if (typeof type === 'string') {
        _allowed.add(type);
      } else if (Array.isArray(type)) {
        type.forEach((t) => _allowed.add(t));
      }
    },

    /**
     * Restore the old permissive default. Use sparingly — defeats the
     * purpose of the explicit allowlist.
     */
    allowAll() {
      _allowed.clear();
    },

    /**
     * Return all recorded events with the given type.
     */
    findEvents(type) {
      return events.filter((e) => e && e.type === type);
    },

    /**
     * Wipe the recorded list. Useful between assertions in a single test.
     */
    clearEvents() {
      events.length = 0;
    },

    /**
     * Convenience handle for tests that want to assert on the raw
     * recorded list (e.g. dispatchCalls.length).
     */
    get dispatchCalls() {
      return events;
    },

    /**
     * Expose the inner ctx so tests that want to drive the FSM
     * directly (e.g. for emulating a cache hit) can reuse it.
     */
    get ctx() {
      return innerCtx;
    },
  };

  wireGovernance();

  function teardown() {
    if (governanceOwner && typeof governanceOwner.setGovernance === 'function') {
      governanceOwner.setGovernance(null);
    }
    events.length = 0;
  }

  return { stubCk, teardown };
}

module.exports = { createStubCk };
