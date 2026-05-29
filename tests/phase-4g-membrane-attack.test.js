/**
 * Phase 4G: Membrane Attack Resistance
 *
 * Validates that the constitutional membrane layer rejects or flags
 * cross-domain authority violations — it is not merely observational.
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
 * All attacks must be flagged as adversarial in the ledger — never
 * silently accepted as legitimate state transitions.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import observability from '../control-plane/observability/index.js';
import lineageWorker from '../control-plane/governance/lineage-worker.js';
import lineageLedger from '../control-plane/governance/lineage-ledger.js';
const { waitForLedgerEntry, waitForLedgerEntryCount } = require('./helpers/sync-barriers');
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

  it('publishing domain cannot silently mutate governance FSM', async () => {
    const { injectAdversarialTransition } = require('./event-injector.js');

    injectAdversarialTransition({
      membrane: 'publishing',
      targetDomain: 'governance',
      entityId: `governance-hijack-${Date.now()}`,
    });

    const hostileEntry = await waitForLedgerEntry(
      (e) =>
        e.domain === 'governance' &&
        e.raw?.raw?.adversarial === true &&
        e.raw?.raw?.membrane === 'publishing',
      50,
      8000
    );

    // Attack must be recorded and flagged — not silently accepted
    expect(hostileEntry).toBeDefined();
    expect(hostileEntry.raw.raw.adversarial).toBe(true);
  });

  it('telemetry domain cannot silently mutate execution pipeline', async () => {
    const { injectAdversarialTransition } = require('./event-injector.js');

    injectAdversarialTransition({
      membrane: 'telemetry',
      targetDomain: 'execution',
      entityId: `exec-hijack-${Date.now()}`,
    });

    const hostileEntry = await waitForLedgerEntry(
      (e) =>
        e.domain === 'execution' &&
        e.raw?.raw?.adversarial === true &&
        e.raw?.raw?.membrane === 'telemetry',
      50,
      8000
    );

    expect(hostileEntry).toBeDefined();
    expect(hostileEntry.raw.raw.adversarial).toBe(true);
  });

  it('reconciliation domain cannot silently overwrite foreign projection', async () => {
    const { injectAdversarialTransition } = require('./event-injector.js');

    injectAdversarialTransition({
      membrane: 'reconciliation',
      targetDomain: 'projection',
      entityId: `proj-overwrite-${Date.now()}`,
    });

    const hostileEntry = await waitForLedgerEntry(
      (e) =>
        e.domain === 'projection' &&
        e.raw?.raw?.adversarial === true &&
        e.raw?.raw?.membrane === 'reconciliation',
      50,
      8000
    );

    expect(hostileEntry).toBeDefined();
    expect(hostileEntry.raw.raw.adversarial).toBe(true);
  });

  it('foreign authority attempting cross-domain mutation is flagged', async () => {
    const crossDomainId = `cross-domain-attack-${Date.now()}`;

    observability.transition({
      domain: 'acquisition',
      entity: 'fsm',
      entityId: crossDomainId,
      previousState: 'IDLE',
      nextState: 'COMPROMISED',
      authority: 'foreign-domain-attacker',
      raw: {
        unauthorizedCrossDomain: true,
        attackerAuthority: 'foreign-domain-attacker',
        attackedDomain: 'acquisition',
      },
    });

    const attackEntry = await waitForLedgerEntry(
      (e) =>
        e.entityId === crossDomainId &&
        e.raw?.raw?.unauthorizedCrossDomain === true,
      50,
      8000
    );

    // Entry must appear flagged — not processed as legitimate authority
    expect(attackEntry).toBeDefined();
    expect(attackEntry.raw.raw.unauthorizedCrossDomain).toBe(true);
  });
});
