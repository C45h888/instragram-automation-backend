// substrates/vault/default-scopes.js
// PAT scope fallback set. Used when /debug_token is unavailable.
// Migrated from services/tokens/base.js.

const PAT_SCOPE_DEFAULTS = [
  'instagram_basic',
  'instagram_manage_comments',
  'instagram_manage_insights',
  'instagram_content_publish',
  'instagram_manage_messages',
  'pages_show_list',
  'pages_read_engagement',
  'pages_manage_metadata',
  'pages_read_user_content',
  'pages_manage_posts',
  'pages_manage_engagement',
];

module.exports = { PAT_SCOPE_DEFAULTS };
