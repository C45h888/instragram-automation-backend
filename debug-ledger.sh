#!/bin/sh
# Minimal debug script — inject into test container to diagnose ledger write path
cd /app

node -e "
const { getRedisClient } = require('./config/redis.js');
const lineageLedger = require('./control-plane/governance/lineage-ledger.js');
const observability = require('./control-plane/observability/index.js');

async function main() {
  const redis = getRedisClient();
  console.log('Redis status:', redis.status);
  console.log('Redis options:', redis.options?.host, redis.options?.port);

  // Flush
  const governanceKeys = await redis.keys('governance:*');
  const lineageKeys = await redis.keys('lineage:*');
  await redis.del(...[...governanceKeys, ...lineageKeys]);
  console.log('Flushed keys');

  // Check initial ledger size
  const beforeSize = await lineageLedger.getLineage(9999).then(l => l.length);
  console.log('Initial ledger size:', beforeSize);

  // Emit one transition
  console.log('Emitting transition...');
  await observability.transition({
    domain: 'runtime',
    entity: 'probe',
    entityId: 'debug-1',
    previousState: 'IDLE',
    nextState: 'EMITTING',
    authority: 'debug-test',
    raw: {
      coordinatedBy: 'telemetry-coordination-fsm',
      transitionType: 'SEMANTIC_PROJECTION_TRANSITION',
    },
  });
  console.log('Transition emitted');

  // Wait and check
  await new Promise(r => setTimeout(r, 1000));
  const afterSize = await lineageLedger.getLineage(9999).then(l => l.length);
  console.log('Ledger size after emit:', afterSize);

  // Check Redis directly
  const directSize = await redis.llen('lineage:ledger:entries');
  console.log('Direct llen of ledger:', directSize);

  // Check transition log entries
  const logEntries = await redis.lrange('lineage:transitionLog:entries', 0, -1);
  console.log('Transition log entries:', logEntries.length);
  if (logEntries.length > 0) {
    console.log('Last 2 log entries:', JSON.parse(logEntries[logEntries.length - 1] || '{}'));
  }

  await redis.quit();
}

main().catch(e => {
  console.error('Error:', e.message);
  process.exit(1);
});
" 2>&1