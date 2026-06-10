/**
 * Phase 7 test setup — applies to every Phase 7 kernel battery and
 * integration test.
 *
 * Previously hooked Module._load to intercept a missing module path
 * (postgres-telemetry-kernel/readers). That path now resolves to the
 * real file created as fix B-NEW-1 (postgres-telemetry-kernel/readers/
 * index.js re-exports from reading/workers/media-worker.js). This
 * file is kept as a no-op placeholder in case additional Phase 7
 * test-wide setup is needed in the future.
 */
