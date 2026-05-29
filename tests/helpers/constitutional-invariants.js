// tests/helpers/constitutional-invariants.js
// Formalized constitutional invariant assertions for test suites.
//
// Extracted from the constitutional laws defined in the architecture:
//   1. A projection reconstructed from lineage must converge identically.
//   2. A replay event may never mutate prior lineage history.
//   3. Cross-domain transitions may never bypass membrane authority.
//   4. Concurrent replay windows must remain causally monotonic.
//   5. A stale authority chain must be rejected constitutionally.
//   6. Duplicate replay injection must remain idempotent.
//
// Each invariant is a pure assertion function that throws on violation.
// All invariants are synchronous and stateless — they operate on provided data.

/**
 * Compute a deterministic structural hash from ledger entries.
 * Captures domain+entity+state transitions, NOT timing — so replay
 * produces the same hash regardless of when it runs.
 *
 * @param {Array<object>} entries — ledger entries
 * @returns {string} hex hash
 */
function deterministicEntryHash(entries) {
  const normalized = entries
    .map(e => ({
      d: e.domain,
      en: e.entity,
      eid: e.entityId,
      p: e.previousState,
      n: e.nextState,
      a: e.authority,
    }))
    .sort((a, b) => {
      if (a.d !== b.d) return a.d.localeCompare(b.d);
      if (a.en !== b.en) return a.en.localeCompare(b.en);
      if (a.eid !== b.eid) return a.eid.localeCompare(b.eid);
      return 0;
    });
  let hash = 0;
  for (const e of normalized) {
    const str = JSON.stringify(e);
    for (let i = 0; i < str.length; i++) {
      hash = (hash * 31 + str.charCodeAt(i)) >>> 0;
    }
  }
  return hash.toString(16);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Law 1: A projection reconstructed from lineage must converge identically.
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Assert that replay reconstruction produces the same structural hash.
 *
 * @param {Array<object>} originalEntries — entries before replay
 * @param {Array<object>} rebuiltEntries — entries after replay reconstruction
 */
function assertReplayConvergence(originalEntries, rebuiltEntries) {
  const originalHash = deterministicEntryHash(originalEntries);
  const rebuiltHash = deterministicEntryHash(rebuiltEntries);
  if (originalHash !== rebuiltHash) {
    throw new Error(
      `[constitutional-invariant] LAW 1 VIOLATION: Replay convergence failed.\n` +
      `  Original hash: ${originalHash}\n` +
      `  Rebuilt hash:  ${rebuiltHash}\n` +
      `  Original entries: ${originalEntries.length}, Rebuilt entries: ${rebuiltEntries.length}`
    );
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Law 2: A replay event may never mutate prior lineage history.
//        Timestamps must never regress within a causal chain.
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Assert that entry timestamps never regress — each entry's timestamp
 * is >= the previous entry's timestamp.
 *
 * @param {Array<object>} entries — ledger entries in chronological order
 * @returns {{ regressions: number, firstViolation: object|null }}
 */
function assertNoTimestampRegression(entries) {
  let regressions = 0;
  let firstViolation = null;
  for (let i = 1; i < entries.length; i++) {
    if (entries[i].timestamp < entries[i - 1].timestamp) {
      regressions++;
      if (!firstViolation) {
        firstViolation = {
          index: i,
          prev: entries[i - 1].timestamp,
          curr: entries[i].timestamp,
          prevId: entries[i - 1].ledgerId,
          currId: entries[i].ledgerId,
        };
      }
    }
  }
  if (regressions > 0) {
    throw new Error(
      `[constitutional-invariant] LAW 2 VIOLATION: ${regressions} timestamp regression(s) detected.\n` +
      `  First violation at index ${firstViolation.index}: ${firstViolation.prev} → ${firstViolation.curr}`
    );
  }
  return { regressions: 0, firstViolation: null };
}

// ═══════════════════════════════════════════════════════════════════════════════
// Law 3: Cross-domain transitions may never bypass membrane authority.
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Assert no entries exist where a source domain's authority mutated a
 * different target domain, which would indicate a membrane bypass.
 *
 * @param {Array<object>} entries — ledger entries
 * @param {Array<{sourceDomain: string, targetDomain: string}>} forbiddenPairs
 */
function assertNoCrossDomainContamination(entries, forbiddenPairs) {
  for (const { sourceDomain, targetDomain } of forbiddenPairs) {
    const violations = entries.filter(
      e =>
        e.domain === targetDomain &&
        e.authority &&
        (e.authority.includes(sourceDomain) || e.authority === sourceDomain)
    );
    if (violations.length > 0) {
      throw new Error(
        `[constitutional-invariant] LAW 3 VIOLATION: Cross-domain membrane bypass detected.\n` +
        `  Source: ${sourceDomain} → Target: ${targetDomain}\n` +
        `  Violations: ${violations.length}\n` +
        `  First violation authority: ${violations[0].authority}`
      );
    }
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Law 4: Concurrent replay windows must remain causally monotonic.
//        Cursor positions must never retreat.
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Assert that cursor positions advance monotonically — never retreat.
 *
 * @param {Array<number>} cursors — cursor values in observation order
 */
function assertMonotonicCursors(cursors) {
  for (let i = 1; i < cursors.length; i++) {
    if (cursors[i] < cursors[i - 1]) {
      throw new Error(
        `[constitutional-invariant] LAW 4 VIOLATION: Cursor regression at index ${i}: ` +
        `${cursors[i - 1]} → ${cursors[i]}`
      );
    }
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Law 5: A stale authority chain must be rejected constitutionally.
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Assert that entries with out-of-order (stale) timestamps are flagged
 * in the ledger, not silently accepted as legitimate authority.
 *
 * @param {Array<object>} entries — ledger entries flagged as outOfOrder
 * @param {string} label — description for error message
 */
function assertStaleEntriesFlagged(entries, label = 'stale entries') {
  if (entries.length === 0) {
    throw new Error(
      `[constitutional-invariant] LAW 5 VIOLATION: No ${label} found in ledger. ` +
      `Expected stale authority entries to be recorded and flagged.`
    );
  }
  const unflagged = entries.filter(e => !e.raw?.raw?.outOfOrder);
  if (unflagged.length > 0) {
    throw new Error(
      `[constitutional-invariant] LAW 5 VIOLATION: ${unflagged.length} ${label} ` +
      `not flagged as outOfOrder.`
    );
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Law 6: Duplicate replay injection must remain idempotent.
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Assert that duplicate replay produces no corruption markers.
 *
 * @param {Array<object>} entries — entries from the duplicate injection window
 * @param {number} expectedCount — how many entries should exist (both emissions)
 */
function assertIdempotentReplay(entries, expectedCount) {
  if (entries.length !== expectedCount) {
    throw new Error(
      `[constitutional-invariant] LAW 6 VIOLATION: Idempotent replay count mismatch.\n` +
      `  Expected: ${expectedCount}, Got: ${entries.length}`
    );
  }
  const corrupted = entries.filter(e => e.raw?.raw?.corrupted);
  if (corrupted.length > 0) {
    throw new Error(
      `[constitutional-invariant] LAW 6 VIOLATION: ${corrupted.length} corruption ` +
      `markers found during idempotent replay.`
    );
  }
}

/**
 * Assert that no entry carries a corruption marker (raw.corrupted === true).
 * Used for broader corruption checks beyond idempotent replay.
 *
 * @param {Array<object>} entries — ledger entries to check
 */
function assertNoSilentCorruption(entries) {
  const corrupted = entries.filter(e => e.raw?.raw?.corrupted === true);
  if (corrupted.length > 0) {
    throw new Error(
      `[constitutional-invariant] LAW 6 VIOLATION: ${corrupted.length} corruption ` +
      `markers found in ledger. First: ${corrupted[0].ledgerId || corrupted[0].entityId}`
    );
  }
}

module.exports = {
  deterministicEntryHash,
  assertReplayConvergence,
  assertNoTimestampRegression,
  assertNoCrossDomainContamination,
  assertMonotonicCursors,
  assertStaleEntriesFlagged,
  assertIdempotentReplay,
  assertNoSilentCorruption,
};
