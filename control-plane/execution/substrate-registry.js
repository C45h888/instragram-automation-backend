// control-plane/execution/substrate-registry.js
// Substrate Registry: domain → substrate module lookup.
//
// Owns: mapping domain names to their bounded substrate modules.
// Does NOT own: orchestration, governance, policy, retry, execution flow.
//
// Replaces domain-registry.js. Each substrate exports { fetch, persist }.
// Publish domains kept inline — they use transport/publishing.js (separate concern).

const engagement = require('../../substrates/engagement');
const content     = require('../../substrates/content');
const ugc         = require('../../substrates/ugc');
const insights    = require('../../substrates/insights');

// Publishing — separate transport, kept inline for now
const igFetcherPublish = require('./ig-fetcher-publish');
const dbWorker = require('./db-worker');
const persistence = require('../../substrates/persistence');

const DOMAIN_REGISTRY = {
  comments:  { fetch: engagement.fetch.bind(engagement), persist: engagement.persist.bind(engagement) },
  messages:  { fetch: engagement.fetch.bind(engagement), persist: engagement.persist.bind(engagement) },
  ugc:       { fetch: ugc.fetch.bind(ugc),              persist: ugc.persist.bind(ugc) },
  insights:  { fetch: insights.fetch.bind(insights),     persist: insights.persist.bind(insights) },
  media:     { fetch: content.fetch.bind(content),       persist: content.persist.bind(content) },

  // Publish domains — unresolved, keep domain-registry inline logic
  'publish:media': {
    fetch: async (accountId, params, creds) => {
      const { action_type, payload, scheduled_post_id, intent_type } = params;
      let resolvedPayload = payload || params;
      if (intent_type === 'scheduled_post' && resolvedPayload?.asset_id) {
        const asset = await dbWorker.resolveAsset(resolvedPayload.asset_id);
        if (!asset?.storage_path) {
          return { success: false, count: 0, error: 'Asset not found', retryable: false, error_category: 'permanent' };
        }
        resolvedPayload = { image_url: asset.storage_path, caption: asset.caption || '', media_type: asset.media_type || 'IMAGE', scheduled_post_id };
      }
      return igFetcherPublish.executePublishAction(action_type || 'publish_post', accountId, creds, resolvedPayload);
    },
    persist: async (accountId, rawData, execParams) => {
      const { queue_row_id, scheduled_post_id } = execParams || {};
      if (queue_row_id && rawData.instagram_id) await dbWorker.markPostQueueSent(queue_row_id, rawData.instagram_id);
      if (scheduled_post_id && rawData.instagram_id) await dbWorker.markScheduledPostPublished(scheduled_post_id, rawData.instagram_id);
      return { count: 1 };
    },
  },

  'publish:ugc': {
    fetch: async (accountId, params, creds) => {
      const { payload } = params;
      const permissionId = payload?.permission_id;
      let resolvedPayload = payload;
      if (permissionId) {
        const ugc = await dbWorker.resolveUgcContent(permissionId);
        if (!ugc) return { success: false, count: 0, error: 'UGC media not found', retryable: false, error_category: 'permanent' };
        resolvedPayload = { ...payload, media_url: ugc.media_url, caption: ugc.caption, media_type: ugc.media_type };
      }
      return igFetcherPublish.executePublishAction('repost_ugc', accountId, creds, resolvedPayload);
    },
    persist: async (accountId, rawData, execParams) => {
      const { queue_row_id, payload } = execParams || {};
      if (queue_row_id && rawData.instagram_id) await dbWorker.markPostQueueSent(queue_row_id, rawData.instagram_id);
      if (payload?.permission_id && rawData.instagram_id) await dbWorker.markUgcPermissionReposted(payload.permission_id, rawData.instagram_id);
      return { count: 1 };
    },
  },

  'publish:messaging': {
    fetch: async (accountId, params, creds) => {
      return igFetcherPublish.executePublishAction(params.action_type, accountId, creds, params.payload || params);
    },
    persist: async (accountId, rawData, execParams) => {
      const { queue_row_id } = execParams || {};
      if (queue_row_id && rawData.instagram_id) await dbWorker.markPostQueueSent(queue_row_id, rawData.instagram_id);
      return { count: 1 };
    },
  },
};

function lookup(domain) {
  return DOMAIN_REGISTRY[domain] || null;
}

function domainForAction(actionType) {
  if (actionType === 'publish_post' || actionType === 'publish_media') return 'publish:media';
  if (actionType === 'repost_ugc' || actionType === 'publish_ugc') return 'publish:ugc';
  return 'publish:messaging';
}

function fetchTypeForAction(actionType) {
  if (actionType === 'publish_media' || actionType === 'publish_post') return 'publish_media';
  if (actionType === 'publish_ugc' || actionType === 'repost_ugc') return 'publish_ugc';
  if (actionType === 'publish_messaging') return 'publish_messaging';
  return 'publish_messaging';
}

function allDomains() {
  return Object.keys(DOMAIN_REGISTRY);
}

module.exports = { lookup, domainForAction, fetchTypeForAction, allDomains };
