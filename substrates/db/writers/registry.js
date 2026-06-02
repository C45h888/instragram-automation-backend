// substrates/db/writers/registry.js
// DB Writers registry: operation → worker module mapping.

const WORKER_MAP = {
  batch_upsert_comments:            './comments-writer',
  batch_upsert_messages:            './messages-writer',
  batch_upsert_conversations:       './conversations-writer',
  batch_upsert_posts:               './content-writer',
  batch_upsert_insights:            './content-writer',
  batch_upsert_ugc:                 './ugc-writer',
  mark_post_queue_sent:             './publishing-writer',
  mark_scheduled_post_published:    './publishing-writer',
  mark_ugc_permission_reposted:     './publishing-writer',
  mark_account_disconnected:        './publishing-writer',
};

function getWriter(operation) {
  const path = WORKER_MAP[operation];
  if (!path) return null;
  return require(path);
}

module.exports = { getWriter };
