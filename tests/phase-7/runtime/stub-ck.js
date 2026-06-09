/**
 * stub-ck.js — Shared Constitutional Kernel stub for Phase 7 kernel tests.
 * ═══════════════════════════════════════════════════════════════════════════════
 *
 * Purpose:
 *   Replace per-test inline mock CK objects with a single, authoritative stub
 *   that models the CK's routing behavior for the two cross-domain coordination
 *   events that flow through it:
 *
 *     F2a — DB_READ_REQUESTED:  stub routes → FSM (READ_RESULT_AVAILABLE)
 *            The FSM issues DB_READ_REQUESTED via ctx.dispatchGlobal().
 *            The stub must replay that back as READ_RESULT_AVAILABLE so the
 *            FSM's pending Promise resolves.
 *
 *     F2b — DB_WRITE_REQUESTED: stub routes → persist-telemetry FSM (writer)
 *            fsm.requestDBWrite() → CK → persist-telemetry FSM → writer.
 *            The stub simulates the persist-telemetry writer returning
 *            DB_WRITE_COMPLETE, which allows requestDBWriteAndAwait to resolve.
 *
 * Usage:
 *   import { createStubCk } from '../runtime/stub-ck.js';
 *
 *   const { stubCk, teardown } = createStubCk({ realFsm });
 *   mockSignalDispatch.getCk.mockReturnValue(stubCk);
 *   // test body
 *   teardown(); // in afterEach
 *
 * The stub models these behaviors:
 *   - CAPABILITY_DATA_REQUEST → routes to real FSM → on DB_READ_REQUESTED
 *     (from ctx.dispatchGlobal), immediately fires READ_RESULT_AVAILABLE back
 *     through the FSM so the pending Promise resolves.
 *   - DB_WRITE_REQUESTED → simulates persist-telemetry FSM + writer,
 *     returns DB_WRITE_COMPLETE so requestDBWriteAndAwait resolves.
 *   - All other events → { allowed: true }.
 */

import { vi } from 'vitest';

/**
 * @param {object} opts
 * @param {object} opts.realFsm — the real graph-capability FSM module
 * @param {object} [opts.persistTelemetryFsm] — optional mock persist-telemetry FSM
 * @param {object} [opts.persistWriter] — optional mock writer (responds to write ops)
 * @returns {{ stubCk: object, teardown: function }}
 */
export function createStubCk({ realFsm, persistTelemetryFsm = null, persistWriter = null } = {}) {
  // Capture all dispatched events for test inspection
  const dispatchedEvents = [];

  /**
   * F2a routing: DB_READ_REQUESTED → READ_RESULT_AVAILABLE loop.
   * When the FSM calls ctx.dispatchGlobal({ type: 'DB_READ_REQUESTED', ... }),
   * the stub intercepts it and immediately dispatches READ_RESULT_AVAILABLE
   * back into the FSM so the pending Promise resolves.
   */
  function routeDbReadToResult(readId, businessAccountId, readDomain, data) {
    if (!realFsm) return;
    realFsm.dispatch(
      {
        type: 'READ_RESULT_AVAILABLE',
        businessAccountId,
        accountId: businessAccountId,
        readId,
        readDomain,
        data: data || null,
        error: data ? null : 'not_found',
      },
      { dispatchGlobal: () => {}, validate: () => ({ allowed: true }) }
    );
  }

  /**
   * F2b routing: DB_WRITE_REQUESTED → simulated persist-telemetry writer.
   *
   * The stub models the full chain:
   *   fsm.requestDBWrite() → CK.dispatch(DB_WRITE_REQUESTED)
   *     → persist-telemetry FSM (stubbed) → writer (stubbed) → DB_WRITE_COMPLETE
   *
   * For fire-and-forget (requestDBWrite): stub immediately returns { success: true }
   * and simulates the writer completing asynchronously.
   *
   * For awaited writes (requestDBWriteAndAwait): simulates the writer completing
   * and dispatches DB_WRITE_COMPLETE → CK → FSM resolves.
   */
  function routeDbWrite(event) {
    const writeId = event.writeId || `w-${Date.now()}`;
    const accountId = event.accountId;

    // Simulate async writer completion
    setImmediate(() => {
      // If persistTelemetryFsm is provided, route DB_WRITE_REQUESTED to it
      if (persistTelemetryFsm && typeof persistTelemetryFsm.dispatch === 'function') {
        persistTelemetryFsm.dispatch(
          {
            type: 'DB_WRITE_REQUESTED',
            domain: event.domain,
            table: event.table,
            operation: event.operation,
            accountId,
            rows: event.rows,
            writeId,
          },
          { dispatchGlobal: () => {}, validate: () => ({ allowed: true }) }
        );
      }

      // Simulate writer completing and CK dispatching DB_WRITE_COMPLETE
      if (realFsm) {
        realFsm.dispatch(
          {
            type: 'DB_WRITE_COMPLETE',
            writeId,
            accountId,
            table: event.table,
            operation: event.operation,
            success: true,
          },
          { dispatchGlobal: () => {}, validate: () => ({ allowed: true }) }
        );
      }
    });

    return { success: true, writeId };
  }

  const stubCk = {
    dispatch: vi.fn((event) => {
      dispatchedEvents.push(event);

      // F2a: CAPABILITY_DATA_REQUEST → route to real FSM
      if (event.type === 'CAPABILITY_DATA_REQUEST') {
        if (!realFsm) return { allowed: true };

        return realFsm.dispatch(event, {
          dispatchGlobal: (sub) => {
            if (sub.type === 'DB_READ_REQUESTED') {
              // F2a routing: immediately replay READ_RESULT_AVAILABLE
              routeDbReadToResult(
                event.readId,
                event.businessAccountId,
                event.readDomain,
                sub.data || null
              );
            }
          },
          validate: () => ({ allowed: true }),
        });
      }

      // F2b: DB_WRITE_REQUESTED → route to persist-telemetry (simulated)
      if (event.type === 'DB_WRITE_REQUESTED') {
        return routeDbWrite(event);
      }

      // Default: allow all other events
      return { allowed: true };
    }),

    // Expose captured events for test inspection
    getDispatchedEvents: () => dispatchedEvents,

    // Convenience: find events by type
    findEvents: (type) => dispatchedEvents.filter((e) => e.type === type),

    // Convenience: check if an event type was dispatched
    wasDispatched: (type) => dispatchedEvents.some((e) => e.type === type),
  };

  function teardown() {
    dispatchedEvents.length = 0;
    vi.restoreAllMocks();
  }

  return { stubCk, teardown };
}