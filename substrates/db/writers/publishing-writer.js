// substrates/db/writers/publishing-writer.js — REMOVED (migrated to reading substrate)
// See: substrates/db/reading/workers/post-queue-worker.js
// Reason: write-path status mutations conflict with cognition-scanner's Realtime subscriptions.
// Status updates for post_queue and scheduled_posts are now handled by the pull-based reader.
// This file is intentionally empty — kept as tombstone for git history.
