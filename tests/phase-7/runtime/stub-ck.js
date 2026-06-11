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
// API:
//   const { stubCk, teardown } = createStubCk({ realFsm });
//   stubCk.dispatch(event)   — records and routes per the table below
//   stubCk.findEvents(type)  — returns all recorded events with that type
//   stubCk.clearEvents()     — wipes the recorded list
//   teardown()               — clears any governance ref the FSM stashed
//
// Routing table:
//   CAPABILITY_DATA_REQUEST → realFsm.dispatch (the FSM will then call
//                              ctx.dispatchGlobal(DB_READ_REQUESTED);
//                              we forward that back to realFsm which
//                              resolves the pending read with a cache-
//                              miss result, satisfying the Promise).
//   DB_READ_REQUESTED       → realFsm.dispatch (so READ_RESULT_AVAILABLE
//                              is reachable from a real emit).
//   anything else           → record and return { allowed: true }.

'use strict';

function createStubCk({ realFsm } = {}) {
  const events = [];
  let governanceOwner = null;

  // Inner ctx the stub hands back to the FSM. The FSM's CAPABILITY_
  // DATA_REQUEST handler calls ctx.dispatchGlobal to forward a
  // DB_READ_REQUESTED. We route that back through the FSM so the
  // pending read is tracked and the Promise's resolve/reject fires
  // when the real flow completes.
  const innerCtx = {
    dispatchGlobal(sub) {
      events.push({ type: 'CTX_DISPATCH_GLOBAL', sub });
      if (sub && sub.type === 'DB_READ_REQUESTED' && realFsm) {
        // Emulate the persist-telemetry worker returning a cache
        // miss (no data). The real FSM will record this as a
        // "data not yet available" state — the substrate code
        // treats that as a cache miss and proceeds to the worker
        // call, then writes back via DB_WRITE_REQUESTED.
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
      return { allowed: true };
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

  // Defer the governance wiring until after stubCk is constructed
  // (TDZ on the literal otherwise).
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

      // Default permissive result — most tests just want the call to
      // succeed so they can assert on it later.
      return { allowed: true };
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

  // Now that stubCk is fully constructed, attach the governance ref.
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
