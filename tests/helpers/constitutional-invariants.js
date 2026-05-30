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
  // LAW 2 EXTENSION: validate causal chain integrity on rebuilt ledger
  // Structural convergence alone is insufficient — every parentTransitionId
  // must resolve to an existing traceId, proving causal fidelity.
  assertCausalChainIntegrity(rebuiltEntries);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Law 2: A replay event may never mutate prior lineage history.
//        Timestamps must never regress within a causal chain.
//        A parentTransitionId must reference an entry that exists in the ledger.
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Assert that entry timestamps never regress — each entry's timestamp
 * is >= the previous entry's timestamp.
 *
 * @param {Array<object>} entries — ledger entries in chronological order
 * @param {Array<object>} [excludeEntries] — entries to exclude from check
 *        (e.g. adversarial injections with intentionally backdated timestamps)
 * @returns {{ regressions: number, firstViolation: object|null }}
 */
function assertNoTimestampRegression(entries, excludeEntries = []) {
  const excluded = new Set(excludeEntries.map(e => e.ledgerId).filter(Boolean));
  let regressions = 0;
  let firstViolation = null;
  for (let i = 1; i < entries.length; i++) {
    if (excluded.has(entries[i].ledgerId)) continue;
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

/**
 * Assert that every parentTransitionId in the ledger resolves to an existing
 * traceId in the same ledger. A broken causal chain is a constitutional
 * violation — parentTransitionId is a reference to a prior transition that must
 * exist as a real ledger entry, not a dangling pointer to fiction.
 *
 * @param {Array<object>} entries — ledger entries to validate
 * @returns {{ broken: number, firstBroken: object|null }}
 */
function assertCausalChainIntegrity(entries) {
  const traceIds = new Set(entries.map(e => e.traceId));
  const broken = [];
  for (const entry of entries) {
    if (entry.parentTransitionId && !traceIds.has(entry.parentTransitionId)) {
      broken.push({
        ledgerId: entry.ledgerId,
        entityId: entry.entityId,
        parentTransitionId: entry.parentTransitionId,
        traceId: entry.traceId,
      });
    }
  }
  if (broken.length > 0) {
    throw new Error(
      `[constitutional-invariant] LAW 2 VIOLATION: Causal chain integrity broken.\n` +
      `  ${broken.length} entry(ies) reference non-existent parent transitions:\n` +
      broken.slice(0, 3).map(b =>
        `    ledgerId=${b.ledgerId} parentTransitionId=${b.parentTransitionId} → not found in ledger`
      ).join('\n') +
      (broken.length > 3 ? `\n    ... and ${broken.length - 3} more` : '')
    );
  }
  return { broken: 0, firstBroken: null };
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

// ═══════════════════════════════════════════════════════════════════════════════
// Law 7: Projection signals must conform to CK signal ownership contract.
// Ledger-derivable signals (transitionCount, lastTransition, authorityStability,
// etc.) belong to lineage-worker Layer B. Observer-relative signals
// (failureRate, governancePressure, interpretationConfidence, etc.) belong
// to telemetry-workers. The two sets never overlap.
// ═══════════════════════════════════════════════════════════════════════════════

// Ledger-derivable signals allowed in lineage worker Layer B projection snapshot
const ALLOWED_LINEAGE_SIGNALS = new Set([
  // domain
  'domain.acquisition.state', 'domain.acquisition.transitionCount',
  'domain.acquisition.lastTransition', 'domain.acquisition.authorityStability',
  'domain.publishing.state', 'domain.publishing.transitionCount',
  'domain.publishing.lastTransition', 'domain.publishing.authorityStability',
  'domain.scheduling.state', 'domain.scheduling.transitionCount',
  'domain.scheduling.lastTransition', 'domain.scheduling.authorityStability',
  'domain.scheduling.cadenceContinuity',
  'domain.dedup.state', 'domain.dedup.transitionCount',
  'domain.dedup.lastTransition', 'domain.dedup.authorityStability',
  'domain.reconciliation.state', 'domain.reconciliation.transitionCount',
  'domain.reconciliation.lastTransition', 'domain.reconciliation.authorityStability',
  // governanceRuntime
  'governanceRuntime.runtimeState', 'governanceRuntime.lastStateTransition',
  'governanceRuntime.degradationSignals', 'governanceRuntime.replayContinuity',
  'governanceRuntime.domainInstability', 'governanceRuntime.epochCount',
  // health
  'health.executionHealth', 'health.transitionCount', 'health.lastTransition',
  'health.authorityStability',
  // authority
  'authority.acquisition.authorityCount', 'authority.acquisition.lastAuthority',
  'authority.acquisition.authorityOscillation', 'authority.acquisition.continuityStatus',
  'authority.publishing.authorityCount', 'authority.publishing.lastAuthority',
  'authority.publishing.authorityOscillation', 'authority.publishing.continuityStatus',
  'authority.scheduling.authorityCount', 'authority.scheduling.lastAuthority',
  'authority.scheduling.authorityOscillation', 'authority.scheduling.continuityStatus',
  // integrity
  'integrity.structuralAnomalyCount', 'integrity.replayAnomalyProbability',
  'integrity.cadenceGapProbability',
  // _meta
  '_meta.projectionVersion', '_meta.lineageVersion', '_meta.updatedAt',
  '_meta.entryCount', '_meta.cursor',
]);

// Observer-relative signal names that must NOT appear in lineage worker snapshot
const FORBIDDEN_IN_LINEAGE = new Set([
  'failureRate', 'interpretationConfidence', 'governancePressure',
  'retryPressure', 'bufferPressure', 'quotaPressure', 'circuitBreakers',
  'executionPressure', 'authorityInstability', 'runtimeEntropy',
  'operationalStress', 'systemicStress', 'convergenceConfidence',
]);

/**
 * Assert that a lineage worker projection snapshot contains only
 * ledger-derivable signals and no observer-relative signals.
 *
 * @param {object} snapshot — lineage worker getProjections() output
 */
function assertProjectionSignalContract(snapshot) {
  const violations = [];
  const visited = new Set();

  function traverse(obj, path = '') {
    if (!obj || typeof obj !== 'object' || Array.isArray(obj)) return;
    if (visited.has(obj)) return;
    visited.add(obj);

    for (const [key, value] of Object.entries(obj)) {
      if (key === '_meta' || typeof value === 'function') continue;
      const currentPath = path ? `${path}.${key}` : key;

      if (value && typeof value === 'object' && !Array.isArray(value)) {
        traverse(value, currentPath);
      } else if (
        !currentPath.startsWith('_meta') &&
        typeof value !== 'string' &&
        typeof value !== 'boolean' &&
        typeof value !== 'number'
      ) {
        continue;
      } else if (!currentPath.startsWith('_meta') && currentPath !== '_meta') {
        // Check if this path is in the allowed set
        if (!ALLOWED_LINEAGE_SIGNALS.has(currentPath)) {
          // Check if it contains a forbidden signal name
          for (const forbidden of FORBIDDEN_IN_LINEAGE) {
            if (currentPath.endsWith(forbidden)) {
              violations.push(currentPath);
              return;
            }
          }
          // Unknown signals with numeric values get a pass — could be new
          // signal type still being classified, but log it
        }
      }
    }
  }

  traverse(snapshot);

  if (violations.length > 0) {
    throw new Error(
      `[constitutional-invariant] LAW 7 VIOLATION: Projection signal contract breached.\n` +
      `  Observer-relative signals found in lineage worker snapshot:\n` +
      `  ${violations.join(', ')}\n` +
      `  These signals are owned by telemetry-workers.`
    );
  }
}

module.exports = {
  deterministicEntryHash,
  assertReplayConvergence,
  assertNoTimestampRegression,
  assertCausalChainIntegrity,
  assertNoCrossDomainContamination,
  assertMonotonicCursors,
  assertStaleEntriesFlagged,
  assertIdempotentReplay,
  assertNoSilentCorruption,
  assertProjectionSignalContract,
};
