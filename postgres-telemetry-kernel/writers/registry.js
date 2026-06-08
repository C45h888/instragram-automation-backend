// postgres-telemetry-kernel/writers/registry.js
// DB Writers registry: operation → worker module mapping.

const WORKER_MAP = {
  batch_upsert_comments:              './comments-writer',
  batch_upsert_messages:              './messages-writer',
  batch_upsert_conversations:         './conversations-writer',
  batch_upsert_posts:                 './content-writer',
  batch_upsert_insights:              './content-writer',
  batch_upsert_ugc:                   './ugc-writer',
  batch_upsert_media_stubs:           './content-writer',
  batch_fix_message_conversation_ids: './message-fix-writer',
  upsert_credential:                  './credential-store-writer',
  write_scope_cache:                  '../substrates/graph-capability/workers/write-scope-cache-worker',
  update_credential_status:           '../substrates/graph-capability/workers/update-credential-status-worker',
};

function getWriter(operation) {
  const path = WORKER_MAP[operation];
  if (!path) return null;
  return require(path);
}

module.exports = { getWriter };
