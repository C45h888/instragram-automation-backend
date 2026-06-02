#!/bin/sh
cd /app
NODE_ENV=test npx vitest run tests/phase-6a-transition-writers-redis.test.js --reporter=verbose 2>&1