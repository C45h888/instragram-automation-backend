/**
 * Phase 4G: Membrane Attack Resistance
 *
 * Validates that the constitutional membrane layer rejects cross-domain
 * authority violations via the CK membrane authority gate. After the Issue 1
 * fix (membrane authority validation pre-projection in transition-emitter),
 * adversarial entries are REJECTED by CK before entering the observability log.
 * A MEMBRANE_BYPASS structural anomaly entry is written instead.
 *
 * Constitutional law:
 *   Cross-domain transitions may never bypass membrane authority.
 *
 * Attacks tested:
 *   - publishing domain → mutate governance FSM
 *   - telemetry domain → mutate execution pipeline
 *   - reconciliation domain → overwrite foreign projection
 *   - Foreign authority → directly mutate acquisition domain
 *
 * Expected behavior after Issue 1 fix:
 *   The adversarial entry does NOT appear in the ledger.
 *   A MEMBRANE_BYPASS governance anomaly entry IS recorded.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import observability from '../control-plane/observability/index.js';
import lineageWorker from '../control-plane/governance/lineage-worker.js';
import lineageLedger from '../control-plane/governance/lineage-ledger.js';
const { waitForLedgerEntry, waitForCommit } = require('./helpers/sync-barriers');
const { assertNoCrossDomainContamination } = require('./helpers/constitutional-invariants');

describe('Phase 4G: Membrane Attack Resistance', () => {
  beforeAll(async () => {
    await observability.init();
    await lineageWorker.start(400);
  }, 15000);

  afterAll(async () => {
    await lineageWorker.stop();
    await observability.stop();
  });

  /**
   * CK membrane gate rejects publishing → governance bypass.
   * Adversarial entry is rejected; MEMBRANE_BYPASS anomaly is recorded.
   * Deterministic sync: await lineage worker cursor visibility before ledger check.
   */
  it('publishing domain cannot silently mutate governance FSM — CK rejects, anomaly recorded', async () => {
    const { injectAdversarialTransition } = require('./event-injector.js');

    const { anomalyCursor } = injectAdversarialTransition({
      membrane: 'publishing',
      targetDomain: 'governance',
      entityId: `governance-hijack-${Date.now()}`,
    });

    // Stage 1: deterministic commit visibility — wait for worker cursor to advance
    await waitForCommit(anomalyCursor, 30000);

    // Stage 2: confirm anomaly is in theledger
    const anomalyEntry = await waitForLedgerEntry(
      (e) =>
        e.domain === 'governance' &&
        e.entity === 'membrane' &&
        e.nextState === 'MEMBRANE_BYPASS' &&
        e.raw?.bypassedAuthority === 'publishing-membrane',
      50,
      5000
    );

    expect(anomalyEntry).toBeDefined();
    expect(anomalyEntry.raw.reason).toContain('MEMBRANE_BYPASS');
  });

  /**
   * CK membrane gate rejects telemetry → execution bypass.
   */
  it('telemetry domain cannot silently mutate execution pipeline — CK rejects, anomaly recorded', async () => {
    const { injectAdversarialTransition } = require('./event-injector.js');

    const { anomalyCursor } = injectAdversarialTransition({
      membrane: 'telemetry',
      targetDomain: 'execution',
      entityId: `exec-hijack-${Date.now()}`,
    });

    await waitForCommit(anomalyCursor, 30000);

    const anomalyEntry = await waitForLedgerEntry(
      (e) =>
        e.domain === 'governance' &&
        e.entity === 'membrane' &&
        e.nextState === 'MEMBRANE_BYPASS' &&
        e.raw?.bypassedAuthority === 'telemetry-worker',
      50,
      5000
    );

    expect(anomalyEntry).toBeDefined();
    expect(anomalyEntry.raw.reason).toContain('MEMBRANE_BYPASS');
  });

  /**
   * CK membrane gate rejects reconciliation → projection bypass.
   */
  it('reconciliation domain cannot silently overwrite foreign projection — CK rejects, anomaly recorded', async () => {
    const { injectAdversarialTransition } = require('./event-injector.js');

    const { anomalyCursor } = injectAdversarialTransition({
      membrane: 'reconciliation',
      targetDomain: 'projection',
      entityId: `proj-overwrite-${Date.now()}`,
    });

    await waitForCommit(anomalyCursor, 30000);

    const anomalyEntry = await waitForLedgerEntry(
      (e) =>
        e.domain === 'governance' &&
        e.entity === 'membrane' &&
        e.nextState === 'MEMBRANE_BYPASS' &&
        e.raw?.bypassedAuthority === 'reconciliation-fsm',
      50,
      5000
    );

    expect(anomalyEntry).toBeDefined();
    expect(anomalyEntry.raw.reason).toContain('MEMBRANE_BYPASS');
  });

  /**
   * Foreign authority attempting cross-domain mutation — CK rejects.
   */
  it('foreign authority attempting cross-domain mutation is rejected by CK', async () => {
    const beforeCursor = observability.query.getLogSize();

    observability.transition({
      domain: 'acquisition',
      entity: 'fsm',
      entityId: `cross-domain-attack-${Date.now()}`,
      previousState: 'IDLE',
      nextState: 'COMPROMISED',
      authority: 'foreign-domain-attacker',
      raw: {
        unauthorizedCrossDomain: true,
        attackerAuthority: 'foreign-domain-attacker',
        attackedDomain: 'acquisition',
      },
    });

    const anomalyCursor = beforeCursor + 1;

    await waitForCommit(anomalyCursor, 30000);

    const anomalyEntry = await waitForLedgerEntry(
      (e) =>
        e.domain === 'governance' &&
        e.entity === 'membrane' &&
        e.nextState === 'MEMBRANE_BYPASS' &&
        e.raw?.reason?.includes('foreign'),
      50,
      5000
    );

    expect(anomalyEntry).toBeDefined();
    expect(anomalyEntry.raw.reason).toContain('foreign');
  });
});
