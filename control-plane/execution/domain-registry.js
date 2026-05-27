// control-plane/execution/domain-registry.js
// Domain Registry: pure lookup mapping domain names to execution modules.
//
// Owns: mapping domain → { fetch, persist }, action → domain routing.
// Does NOT own: orchestration, governance, policy, retry, execution flow.
//
// This is the SINGLE source of truth for domain-to-execution mapping.
// Previously duplicated across orchestrator.js (DOMAIN_ROUTING),
// emission.js (domainForAction/fetchTypeForAction), and
// db-scanner.js (domainForAction/fetchTypeForAction).
//
// Constitutional purity: no orchestration, no governance, no policy.
// Pure mechanical lookup only.

const persistence = require('../../substrates/persistence');
const dbWorker = require('./db-worker');

// IG Fetchers — pure transport, per domain.
const igFetcherComments = require('./ig-fetcher-comments');
const igFetcherMessages = require('./ig-fetcher-messages');
const igFetcherUgc     = require('./ig-fetcher-ugc');
const igFetcherInsights = require('./ig-fetcher-insights');
const igFetcherMedia   = require('./ig-fetcher-media');
const igFetcherPublish = require('./ig-fetcher-publish');

// ═══════════════════════════════════════════════════════════════════════════════
// 1. Domain Execution Registry — domain → { fetch, persist }
// ═══════════════════════════════════════════════════════════════════════════════

const DOMAIN_REGISTRY = {
  comments: {
    fetch: (accountId, params, creds) => {
      if (params.media_id) {
        return igFetcherComments.fetchComments(accountId, params.media_id, params.limit, creds);
      }
      return igFetcherComments.fetchRecentMediaComments(accountId, params.maxPosts, params.limit, creds);
    },
    persist: (accountId, rawData) => {
      if (rawData.batches) {
        return persistence.storeCommentBatches(accountId, rawData.batches);
      }
      if (rawData.records) {
        return persistence.storeCommentBatches(accountId, [{ mediaId: 'direct', comments: rawData.records }]);
      }
      return { count: 0 };
    },
  },
  messages: {
    fetch: (accountId, params, creds) => {
      if (params.conversation_id) {
        return igFetcherMessages.fetchMessages(accountId, params.conversation_id, params.limit, creds);
      }
      return igFetcherMessages.fetchConversations(accountId, params.convLimit || params.limit, creds);
    },
    persist: async (accountId, rawData) => {
      if (rawData.rawMessages) {
        const stored = await persistence.storeMessageBatches(
          accountId, [{ conversationId: rawData.conversationId || 'direct', rawMessages: rawData.rawMessages }],
          rawData.igUserId, rawData.pageId, null
        );
        return { count: stored?.count || rawData.count || 0 };
      }
      if (rawData.rawConversations) {
        const stored = await persistence.storeConversationBatches(
          accountId, rawData.rawConversations, rawData.igUserId, rawData.pageId
        );
        return { count: stored?.count || rawData.count || 0 };
      }
      return { count: 0 };
    },
  },
  ugc: {
    fetch: (accountId, params, creds) => {
      if (params.hashtag) {
        return igFetcherUgc.fetchHashtagMedia(accountId, params.hashtag, params.limit, creds);
      }
      return igFetcherUgc.fetchTaggedMedia(accountId, params.limit, creds);
    },
    persist: async (accountId, rawData) => {
      if (!rawData.records || rawData.records.length === 0) return { count: 0 };
      const { mapRawPostToUgcContent } = require('../../substrates/normalization');
      const source = rawData.cleanHashtag ? 'hashtag' : 'tagged';
      const records = rawData.records
        .filter(p => p.id)
        .map(p => mapRawPostToUgcContent(p, accountId, source, rawData.cleanHashtag || null));
      await persistence.storeUgcContentBatch(records);
      return { count: records.length };
    },
  },
  insights: {
    fetch: async (accountId, params, creds) => {
      const sevenDaysAgo = params.since || Math.floor((Date.now() - 7 * 24 * 3600000) / 1000);
      const now = params.until || Math.floor(Date.now() / 1000);
      const feedResult = await igFetcherInsights.fetchMediaFeed(accountId, sevenDaysAgo, now, creds);
      if (!feedResult.success) return feedResult;
      const insights = await igFetcherInsights.fetchMediaInsightsBatch(feedResult.mediaList, creds.pageToken);
      return { success: true, insights, mediaList: feedResult.mediaList, _usagePct: feedResult._usagePct };
    },
    persist: async (accountId, rawData) => {
      if (!rawData.insights || rawData.insights.length === 0) return { count: 0 };
      const captions = (rawData.mediaList || []).map(m => m.caption).filter(Boolean);
      await persistence.storeMediaInsightsBatch(accountId, rawData.insights, captions);
      return { count: rawData.insights.length };
    },
  },
  media: {
    fetch: (accountId, params) => igFetcherMedia.fetchBusinessPosts(accountId, params.limit),
    persist: (accountId, rawData) => {
      if (!rawData.posts || rawData.posts.length === 0) return { count: 0 };
      return persistence.storeBusinessPosts(accountId, rawData.posts);
    },
  },
  'publish:media': {
    fetch: async (accountId, params, creds) => {
      const { action_type, payload, scheduled_post_id, intent_type } = params;
      const actionType = action_type || 'publish_post';

      let resolvedPayload = payload || params;
      if (intent_type === 'scheduled_post' && resolvedPayload?.asset_id) {
        const asset = await dbWorker.resolveAsset(resolvedPayload.asset_id);
        if (!asset?.storage_path) {
          return { success: false, count: 0, error: 'Asset not found', retryable: false, error_category: 'permanent' };
        }
        resolvedPayload = { image_url: asset.storage_path, caption: asset.caption || '', media_type: asset.media_type || 'IMAGE', scheduled_post_id };
      }

      return igFetcherPublish.executePublishAction(actionType, accountId, creds, resolvedPayload);
    },
    persist: async (accountId, rawData, execParams) => {
      const { queue_row_id, scheduled_post_id } = execParams || {};
      if (queue_row_id && rawData.instagram_id) {
        await dbWorker.markPostQueueSent(queue_row_id, rawData.instagram_id);
      }
      if (scheduled_post_id && rawData.instagram_id) {
        await dbWorker.markScheduledPostPublished(scheduled_post_id, rawData.instagram_id);
      }
      return { count: 1 };
    },
  },
  'publish:ugc': {
    fetch: async (accountId, params, creds) => {
      return igFetcherPublish.executePublishAction('repost_ugc', accountId, creds, params.payload || params);
    },
    persist: async (accountId, rawData, execParams) => {
      const { queue_row_id, payload } = execParams || {};
      if (queue_row_id && rawData.instagram_id) {
        await dbWorker.markPostQueueSent(queue_row_id, rawData.instagram_id);
      }
      if (payload?.permission_id && rawData.instagram_id) {
        await dbWorker.markUgcPermissionReposted(payload.permission_id, rawData.instagram_id);
      }
      return { count: 1 };
    },
  },
  'publish:messaging': {
    fetch: async (accountId, params, creds) => {
      return igFetcherPublish.executePublishAction(params.action_type, accountId, creds, params.payload || params);
    },
    persist: async (accountId, rawData, execParams) => {
      const { queue_row_id } = execParams || {};
      if (queue_row_id && rawData.instagram_id) {
        await dbWorker.markPostQueueSent(queue_row_id, rawData.instagram_id);
      }
      return { count: 1 };
    },
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// 2. Action → Domain Routing (consolidated from emission.js + db-scanner.js)
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Maps a publish action_type to its full routing key (including namespace).
 * Singleton source of truth — no duplication across modules.
 *
 * @param {string} actionType - 'publish_post' | 'repost_ugc' | 'publish_media' | 'publish_ugc' | 'publish_messaging' | etc.
 * @returns {string} routingKey - 'publish:media' | 'publish:ugc' | 'publish:messaging'
 */
function domainForAction(actionType) {
  if (actionType === 'publish_post' || actionType === 'publish_media') return 'publish:media';
  if (actionType === 'repost_ugc' || actionType === 'publish_ugc') return 'publish:ugc';
  return 'publish:messaging';
}

/**
 * Maps a publish action_type to its fetch type (used in Redis queue intents).
 * Singleton source of truth — no duplication across modules.
 *
 * @param {string} actionType
 * @returns {string} fetchType - 'publish_media' | 'publish_ugc' | 'publish_messaging'
 */
function fetchTypeForAction(actionType) {
  if (actionType === 'publish_media') return 'publish_media';
  if (actionType === 'publish_ugc') return 'publish_ugc';
  if (actionType === 'publish_messaging') return 'publish_messaging';
  if (actionType === 'publish_post') return 'publish_media';
  if (actionType === 'repost_ugc') return 'publish_ugc';
  return 'publish_messaging';
}

/**
 * Returns all known domain keys.
 * @returns {string[]}
 */
function allDomains() {
  return Object.keys(DOMAIN_REGISTRY);
}

/**
 * Pure lookup — returns { fetch, persist } for a domain, or null.
 * No orchestration, no policy, no side effects.
 *
 * @param {string} domain - e.g. 'comments', 'messages', 'publish:media'
 * @returns {{ fetch: Function, persist: Function } | null}
 */
function lookup(domain) {
  return DOMAIN_REGISTRY[domain] || null;
}

module.exports = { lookup, domainForAction, fetchTypeForAction, allDomains };
