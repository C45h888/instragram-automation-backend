// backend.api/services/tokens/base.js
// Shared constants and axios — zero logic.
// Imported by detection.js, pat.js, uat.js.

const axios = require('axios');

const GRAPH_API_VERSION = 'v23.0';
const GRAPH_API_BASE = `https://graph.facebook.com/${GRAPH_API_VERSION}`;

// v1 time-series metrics (period=day) — returns {name, values:[{value,end_time}]}
const V1_METRICS = ['reach'];

// v2 total_value metrics — requires metric_type=total_value
const V2_METRICS_BASE = ['accounts_engaged', 'profile_views'];

// v2 conditional — only for accounts with a website URL configured
const V2_METRICS_WEBSITE = ['website_clicks'];

// Comprehensive PAT scope fallback — used when /debug_token is unavailable
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
  'pages_manage_engagement'
];

module.exports = {
  axios,
  GRAPH_API_VERSION,
  GRAPH_API_BASE,
  V1_METRICS,
  V2_METRICS_BASE,
  V2_METRICS_WEBSITE,
  PAT_SCOPE_DEFAULTS,
};
