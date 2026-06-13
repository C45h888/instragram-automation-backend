// postgres-telemetry-kernel/bedrock.js
// Supabase Persistence Bedrock — canonical persistence abstraction layer.
//
// CONSTITUTIONAL POSITION:
//   This is the ONLY file in the entire system that imports `getSupabaseAdmin`.
//   Every domain substrate, every worker, every read/write operation flows
//   through this bedrock. No other file touches Supabase directly.
//
// ARCHITECTURAL CONTRACT (per the Supabase Persistence Substrate spec):
//
//   Owns:
//     - Supabase client lifecycle (one service_role client)
//     - All table names, column names, schema details
//     - All RPC function names
//     - All storage bucket names and paths
//     - All realtime channel and event names
//     - Retry logic with exponential backoff + jitter
//     - Idempotency key generation and enforcement
//     - Transaction coordination (via RPC)
//     - Error classification → platform categories
//     - Observability telemetry (latency, counts, categories)
//     - Connection health checks
//     - Security boundaries (service_role isolation)
//
//   Does NOT own:
//     - State transitions (FSM owns)
//     - Routing decisions (CK owns)
//     - Domain logic (domain substrates own)
//     - Cadence scheduling (retry-cadence-kernel owns)
//     - Recovery worker execution (recovery substrates own)
//
// LAYERING:
//   Supabase (PostgREST / Realtime / Storage)
//          ↑
//   bedrock.js ← THIS FILE (only Supabase-aware module)
//          ↑
//   Domain substrates (ugc, publishing, insights, token)
//          ↑
//   Mature workers (cadence loop, health check, dedup, cursor integrity)
//          ↑
//   FSM → CK → Acquisition / Publishing / Graph-Capability kernels
//
// USAGE (domain substrates):
//   const bedrock = require('../bedrock');
//
//   // UGC substrate
//   await bedrock.ugc.persistCommentEvent(rows, { accountId, intentId, governance });
//   const replies = await bedrock.ugc.getPendingReplies({ accountId, limit: 50 });
//
//   // Publishing substrate
//   await bedrock.publishing.persistPublicationCheckpoint(row, { accountId, intentId, governance });
//
//   // Insights substrate
//   await bedrock.insights.persistSnapshot(rows, { accountId, intentId, governance });
//
//   // Token substrate
//   await bedrock.token.persistCredentialState(row, { accountId, intentId, governance });
//   const health = await bedrock.token.getCredentialHealth({ businessAccountId });
//
// PROHIBITED (workers must never do these):
//   supabase.from("comment_events").insert(...)        ← direct table access
//   supabase.from("publications").select(...)           ← direct query
//   getSupabaseAdmin().from(...)                        ← bypassing bedrock
//   bedrock._upsert(...)                                ← calling internal primitives
//   bedrock._client()                                   ← accessing client directly

const crypto = require('crypto');

// ═══════════════════════════════════════════════════════════════════════════
// LAZY SUPABASE CLIENT — the only import of getSupabaseAdmin in the system
// ═══════════════════════════════════════════════════════════════════════════

let _supabaseAdmin = null;
let _getSupabaseAdmin = null;

function _client() {
  if (_supabaseAdmin) return _supabaseAdmin;
  if (!_getSupabaseAdmin) {
    // Lazy require to avoid circular deps at module load time
    _getSupabaseAdmin = require('../../config/supabase').getSupabaseAdmin;
  }
  _supabaseAdmin = _getSupabaseAdmin();
  if (!_supabaseAdmin) {
    throw new Error('BEDROCK: Supabase admin client unavailable — persistence operations cannot proceed');
  }
  return _supabaseAdmin;
}

// Allow tests to inject a stub
function _injectClient(stub) {
  _supabaseAdmin = stub;
}

// ═══════════════════════════════════════════════════════════════════════════
// ERROR CLASSIFIER — reuses the canonical persistence-failure-substrate
// ═══════════════════════════════════════════════════════════════════════════

let _analyzeFailure = null;

function _classifyError(rawError, operation, context) {
  if (!_analyzeFailure) {
    try {
      const substrate = require('./substrates/persistence-failure-substrate');
      _analyzeFailure = substrate.analyzeFailure;
    } catch (_) {
      // Fallback: minimal classification if substrate unavailable
      return {
        category: 'UNKNOWN_FAILURE',
        subtype: null,
        retryable: true,
        severity: 'MEDIUM',
        severityScore: 50,
        normalized: { message: rawError?.message || 'unknown', httpStatus: null, pgCode: null },
      };
    }
  }
  return _analyzeFailure(rawError, operation, 'supabase', {
    attemptN: context?.attemptN || 1,
    lineageId: context?.intentId || null,
    lineageDomain: 'persist-telemetry',
  });
}

// ═══════════════════════════════════════════════════════════════════════════
// IDEMPOTENCY KEY GENERATION
// ═══════════════════════════════════════════════════════════════════════════

function _generateIdempotencyKey(accountId, intentId, table, rows) {
  const pkField = _TABLE_PK_MAP[table] || 'id';
  const pkValue = rows?.[0]?.[pkField] || rows?.[pkField] || 'unknown';
  const seed = `${accountId}|${intentId}|${table}|${pkField}|${pkValue}`;
  return crypto.createHash('sha256').update(seed).digest('hex').slice(0, 32);
}

// ═══════════════════════════════════════════════════════════════════════════
// TABLE ↔ PRIMARY KEY MAP — bedrock owns all schema knowledge
// ═══════════════════════════════════════════════════════════════════════════

const _TABLE_PK_MAP = {
  instagram_comments:           'instagram_comment_id',
  instagram_dm_messages:        'instagram_message_id',
  instagram_dm_conversations:   'instagram_thread_id',
  instagram_media:              'instagram_media_id',
  ugc_content:                  'id',
  post_queue:                   'id',
  scheduled_posts:              'id',
  ugc_permissions:              'id',
  instagram_credentials:        'id',
  instagram_business_accounts:  'id',
  token_lifecycle_events:       'id',
  system_alerts:                'id',
  user_profiles:                'id',
  api_usage:                    'id',
};

// ═══════════════════════════════════════════════════════════════════════════
// RETRY ENGINE — centralized retry with exponential backoff + jitter
// ═══════════════════════════════════════════════════════════════════════════

const RETRY_POLICY = {
  maxRetries: 3,
  baseMs: 200,
  maxMs: 10000,
  multiplier: 2,
  jitterMs: 100,
  retryableCategories: new Set([
    'TRANSIENT_FAILURE', 'CONNECTION_FAILURE', 'SERIALIZATION_FAILURE',
    'RATE_LIMIT_FAILURE', 'UNKNOWN_FAILURE',
  ]),
  nonRetryableCategories: new Set([
    'CONSTRAINT_FAILURE', 'PERMISSION_FAILURE', 'SCHEMA_FAILURE',
  ]),
};

async function _withRetry(operation, context) {
  const { accountId, intentId, table } = context;
  let lastError = null;
  let lastClassification = null;

  for (let attempt = 1; attempt <= RETRY_POLICY.maxRetries; attempt++) {
    try {
      const result = await operation();
      if (result.error) throw result.error;
      return { success: true, data: result.data, error: null, attempts: attempt };
    } catch (err) {
      lastError = err;
      lastClassification = _classifyError(err, 'write', { attemptN: attempt, intentId });

      // Non-retryable → fail fast
      if (RETRY_POLICY.nonRetryableCategories.has(lastClassification.category)) {
        break;
      }

      // Retryable — backoff
      if (attempt < RETRY_POLICY.maxRetries) {
        const delay = Math.min(
          RETRY_POLICY.baseMs * Math.pow(RETRY_POLICY.multiplier, attempt - 1)
            + Math.random() * RETRY_POLICY.jitterMs,
          RETRY_POLICY.maxMs
        );
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
  }

  return {
    success: false,
    data: null,
    error: lastError?.message || 'retry_exhausted',
    attempts: RETRY_POLICY.maxRetries,
    classification: lastClassification,
  };
}

// ═══════════════════════════════════════════════════════════════════════════
// INTERNAL PRIMITIVES — not exported; domain facades use these
// ═══════════════════════════════════════════════════════════════════════════

/**
 * Atomic upsert — idempotent, with conflict resolution.
 * Workers call this through domain facades, never directly.
 */
async function _upsert(table, rows, opts = {}) {
  const {
    accountId, intentId, governance,
    onConflict,                    // PK column for conflict detection
    ignoreDuplicates = false,      // true = skip on conflict, false = overwrite
    isRetry = false,
  } = opts;

  const supabase = _client();
  const pkField = _TABLE_PK_MAP[table] || 'id';
  const idempotencyKey = _generateIdempotencyKey(accountId, intentId, table, rows);
  const conflictTarget = onConflict || pkField;

  const startMs = Date.now();

  const result = await _withRetry(async () => {
    return supabase
      .from(table)
      .upsert(rows, { onConflict: conflictTarget, ignoreDuplicates });
  }, { accountId, intentId, table });

  const latencyMs = Date.now() - startMs;

  // Emit completion/failure through governance
  if (governance) {
    const dispatch = governance.dispatchGlobal || governance.dispatch;
    if (result.success) {
      dispatch({
        type: 'DB_WRITE_COMPLETE',
        domain: opts.domain || table,
        accountId, intentId, table,
        count: Array.isArray(rows) ? rows.length : 1,
        error: null,
        idempotencyKey, isRetry,
      });
    } else {
      dispatch({
        type: 'DB_WRITE_FAILED',
        domain: opts.domain || table,
        accountId, intentId, table,
        count: 0, rows,
        error: result.error,
        rawError: { message: result.error },
        classification: result.classification,
        workerName: 'bedrock',
        lineageId: intentId,
        primaryKeyField: pkField,
        primaryKeyValue: rows?.[0]?.[pkField] || rows?.[pkField],
        attemptN: result.attempts,
        operation: 'write', source: 'supabase',
        idempotencyKey, isRetry,
      });
    }
  }

  return result;
}

/**
 * Normalized SELECT with pagination, filtering, ordering, projection.
 * Workers call this through domain facades, never directly.
 */
async function _select(table, query = {}) {
  const {
    filters = {},          // { col: value } → .eq(col, value)
    inFilters = {},        // { col: [values] } → .in(col, values)
    notNull = [],          // [col] → .not(col, 'is', null)
    isNull = [],           // [col] → .is(col, null)
    lt = {},               // { col: value } → .lt(col, value)
    gt = {},               // { col: value } → .gt(col, value)
    order,                 // { column: 'col', ascending: true }
    limit = 100,           // default cap
    offset = 0,
    single = false,        // .single()
    maybeSingle = false,   // .maybeSingle()
    select = '*',          // column projection
  } = query;

  const supabase = _client();
  const startMs = Date.now();

  try {
    let q = supabase.from(table).select(select);

    // Apply filters
    for (const [col, val] of Object.entries(filters)) {
      q = q.eq(col, val);
    }
    for (const [col, vals] of Object.entries(inFilters)) {
      q = q.in(col, vals);
    }
    for (const col of notNull) {
      q = q.not(col, 'is', null);
    }
    for (const col of isNull) {
      q = q.is(col, null);
    }
    for (const [col, val] of Object.entries(lt)) {
      q = q.lt(col, val);
    }
    for (const [col, val] of Object.entries(gt)) {
      q = q.gt(col, val);
    }

    // Ordering
    if (order) {
      q = q.order(order.column, { ascending: order.ascending !== false });
    }

    // Pagination
    q = q.range(offset, offset + limit - 1);

    // Single-row modifiers
    if (single) q = q.single();
    if (maybeSingle) q = q.maybeSingle();

    const { data, error } = await q;

    const latencyMs = Date.now() - startMs;

    if (error) {
      return { success: false, data: null, error: error.message, latencyMs };
    }

    return { success: true, data, error: null, latencyMs };
  } catch (err) {
    const latencyMs = Date.now() - startMs;
    return { success: false, data: null, error: err.message, latencyMs };
  }
}

/**
 * Targeted UPDATE with retry.
 */
async function _update(table, updates, filters, opts = {}) {
  const { accountId, intentId, governance } = opts;
  const supabase = _client();
  const startMs = Date.now();

  const result = await _withRetry(async () => {
    let q = supabase.from(table).update(updates);
    for (const [col, val] of Object.entries(filters)) {
      q = q.eq(col, val);
    }
    return q;
  }, { accountId, intentId, table });

  const latencyMs = Date.now() - startMs;

  if (governance) {
    const dispatch = governance.dispatchGlobal || governance.dispatch;
    if (result.success) {
      dispatch({
        type: 'DB_WRITE_COMPLETE',
        domain: opts.domain || table,
        accountId, intentId, table,
        count: 1, error: null,
      });
    }
  }

  return { ...result, latencyMs };
}

/**
 * Pure INSERT — for append-only tables (time-series metrics).
 * Not idempotent — each call inserts new rows.
 * Workers call this through domain facades, never directly.
 */
async function _insert(table, rows, opts = {}) {
  const { accountId, intentId, governance } = opts;
  const supabase = _client();
  const startMs = Date.now();

  try {
    const { data, error } = await supabase.from(table).insert(rows);
    const latencyMs = Date.now() - startMs;

    if (error) {
      if (governance) {
        const dispatch = governance.dispatchGlobal || governance.dispatch;
        dispatch({ type: 'DB_WRITE_FAILED', domain: opts.domain || table, accountId, intentId, table, count: 0, rows, error: error.message, rawError: { message: error.message }, workerName: 'bedrock', lineageId: intentId, attemptN: 1, operation: 'write', source: 'supabase' });
      }
      return { success: false, data: null, error: error.message, latencyMs };
    }

    if (governance) {
      const dispatch = governance.dispatchGlobal || governance.dispatch;
      dispatch({ type: 'DB_WRITE_COMPLETE', domain: opts.domain || table, accountId, intentId, table, count: Array.isArray(rows) ? rows.length : 1, error: null });
    }
    return { success: true, data, error: null, latencyMs };
  } catch (err) {
    const latencyMs = Date.now() - startMs;
    return { success: false, data: null, error: err.message, latencyMs };
  }
}

/**\n * RPC call — for atomic multi-table operations and complex validation.\n * Workers never invoke RPC directly.\n */
async function _rpc(fnName, args = {}, opts = {}) {
  const { get = false } = opts; // get:true for read-only functions
  const supabase = _client();
  const startMs = Date.now();

  try {
    const { data, error } = await supabase.rpc(fnName, args, { get });
    const latencyMs = Date.now() - startMs;

    if (error) {
      return { success: false, data: null, error: error.message, latencyMs };
    }
    return { success: true, data, error: null, latencyMs };
  } catch (err) {
    const latencyMs = Date.now() - startMs;
    return { success: false, data: null, error: err.message, latencyMs };
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// STORAGE — bedrock owns all bucket names and paths
// ═══════════════════════════════════════════════════════════════════════════

const _STORAGE_BUCKETS = {
  MEDIA: 'publications',
  ASSETS: 'assets',
};

const _storage = {
  async upload(bucket, path, file, opts = {}) {
    const supabase = _client();
    const { upsert = true } = opts;
    const { data, error } = await supabase.storage.from(bucket).upload(path, file, { upsert });
    if (error) return { success: false, path: null, error: error.message };
    return { success: true, path: data?.path || path, error: null };
  },

  async getPublicUrl(bucket, path) {
    const supabase = _client();
    const { data } = supabase.storage.from(bucket).getPublicUrl(path);
    return data?.publicUrl || null;
  },

  async createSignedUrl(bucket, path, ttlSeconds = 3600) {
    const supabase = _client();
    const { data, error } = await supabase.storage.from(bucket).createSignedUrl(path, ttlSeconds);
    if (error) return { success: false, url: null, error: error.message };
    return { success: true, url: data?.signedUrl || null, error: null };
  },

  async remove(bucket, paths) {
    const supabase = _client();
    const { data, error } = await supabase.storage.from(bucket).remove(paths);
    if (error) return { success: false, error: error.message };
    return { success: true, error: null };
  },
};

// ═══════════════════════════════════════════════════════════════════════════
// REALTIME — bedrock owns all channel/event names
// ═══════════════════════════════════════════════════════════════════════════

const _REALTIME_CHANNELS = {
  COMMENTS:    'db-changes',
  MESSAGES:    'db-changes',
  PUBLISHING:  'publishing-events',
  INSIGHTS:    'insights-events',
  TOKEN:       'token-events',
};

const _REALTIME_EVENTS = {
  NEW_COMMENT:          'NEW_COMMENT',
  NEW_MESSAGE:          'NEW_MESSAGE',
  PUBLICATION_UPDATED:  'PUBLICATION_UPDATED',
  INSIGHT_REFRESHED:    'INSIGHT_REFRESHED',
  TOKEN_UPDATED:        'TOKEN_UPDATED',
};

const _realtime = {
  /**
   * Publish a normalized realtime event.
   * Workers never publish directly — domain facades call this.
   */
  async publish(channel, event, payload) {
    const supabase = _client();
    try {
      await supabase.channel(channel).send({
        type: 'broadcast',
        event,
        payload,
      });
      return { success: true, error: null };
    } catch (err) {
      return { success: false, error: err.message };
    }
  },

  /**
   * Subscribe to Postgres changes on a table.
   * Returns the channel object for the consumer to attach callbacks.
   */
  subscribeToTable(table, opts = {}) {
    const supabase = _client();
    const {
      event = '*',            // INSERT | UPDATE | DELETE | *
      schema = 'public',
      filter,                 // 'col=eq.val'
    } = opts;

    return supabase
      .channel(_REALTIME_CHANNELS.COMMENTS)
      .on('postgres_changes', { event, schema, table, filter }, (payload) => {
        // Consumer attaches their own callback via .on() chaining
      });
  },
};

// ═══════════════════════════════════════════════════════════════════════════
// HEALTH CHECK
// ═══════════════════════════════════════════════════════════════════════════

async function _healthCheck() {
  try {
    const supabase = _client();
    const startMs = Date.now();
    const { error } = await supabase
      .from('user_profiles')
      .select('count', { count: 'exact', head: true });
    const latencyMs = Date.now() - startMs;
    return {
      healthy: !error,
      latencyMs,
      error: error?.message || null,
      timestamp: new Date().toISOString(),
    };
  } catch (err) {
    return { healthy: false, latencyMs: 0, error: err.message, timestamp: new Date().toISOString() };
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// DOMAIN FACADES — the only exported surface workers may touch
// ═══════════════════════════════════════════════════════════════════════════

// ── UGC Substrate ──────────────────────────────────────────────────────────

const ugc = {
  /**
   * Persist a comment event (parent comment or reply).
   * Idempotent on instagram_comment_id.
   */
  async persistCommentEvent(rows, opts = {}) {
    return _upsert('instagram_comments', rows, {
      ...opts,
      domain: 'ugc',
      onConflict: 'instagram_comment_id',
    });
  },

  /**
   * Persist a DM message event.
   * Idempotent on instagram_message_id.
   */
  async persistMessageEvent(rows, opts = {}) {
    return _upsert('instagram_dm_messages', rows, {
      ...opts,
      domain: 'ugc',
      onConflict: 'instagram_message_id',
    });
  },

  /**
   * Persist a DM conversation.
   * Idempotent on instagram_thread_id.
   */
  async persistConversation(rows, opts = {}) {
    return _upsert('instagram_dm_conversations', rows, {
      ...opts,
      domain: 'ugc',
      onConflict: 'instagram_thread_id',
    });
  },

  /**
   * Persist UGC content (user-generated content posts).
   * Composite conflict on business_account_id + visitor_post_id.
   */
  async persistUgcContent(rows, opts = {}) {
    return _upsert('ugc_content', rows, {
      ...opts,
      domain: 'ugc',
      onConflict: 'business_account_id,visitor_post_id',
    });
  },

  /**
   * Mark DM messages as seen (batch UPDATE on seen_at/seen_by).
   * Each row: { messageId, seenAt, senderId }
   */
  async markMessagesSeen(seenRows, opts = {}) {
    const { governance, accountId, intentId, domain } = opts;
    if (!seenRows || seenRows.length === 0) {
      if (governance) {
        const dispatch = governance.dispatchGlobal || governance.dispatch;
        dispatch({ type: 'DB_WRITE_COMPLETE', domain: domain || 'ugc', accountId, intentId, table: 'instagram_dm_messages', count: 0, error: null });
      }
      return { success: true, count: 0 };
    }

    let successCount = 0;
    let lastError = null;

    for (const r of seenRows) {
      const result = await _update('instagram_dm_messages',
        {
          seen_at: r.seenAt || new Date().toISOString(),
          seen_by: r.senderId || null,
        },
        { instagram_message_id: r.messageId || r.instagram_message_id },
        { ...opts, domain: 'ugc' }
      );
      if (result.success) successCount++;
      else lastError = result.error;
    }

    if (governance) {
      const dispatch = governance.dispatchGlobal || governance.dispatch;
      if (lastError) {
        dispatch({ type: 'DB_WRITE_FAILED', domain: domain || 'ugc', accountId, intentId, table: 'instagram_dm_messages', count: successCount, rows: seenRows, error: lastError, rawError: { message: lastError }, workerName: 'bedrock', lineageId: intentId, primaryKeyField: 'instagram_message_id', primaryKeyValue: seenRows[0]?.messageId, attemptN: 1, operation: 'write', source: 'supabase' });
      } else {
        dispatch({ type: 'DB_WRITE_COMPLETE', domain: domain || 'ugc', accountId, intentId, table: 'instagram_dm_messages', count: successCount, error: null });
      }
    }

    return { success: !lastError, count: successCount, error: lastError };
  },

  /**
   * Get pending replies that need processing.
   */
  async getPendingReplies(opts = {}) {
    return _select('instagram_comments', {
      filters: { is_reply: true },
      notNull: ['parent_comment_id'],
      order: { column: 'timestamp', ascending: false },
      limit: opts.limit || 50,
    });
  },

  /**
   * Get conversation history for a thread.
   */
  async getConversationHistory(threadId, opts = {}) {
    return _select('instagram_dm_messages', {
      filters: { instagram_thread_id: threadId },
      order: { column: 'timestamp', ascending: true },
      limit: opts.limit || 100,
    });
  },

  /**
   * Get thread metadata.
   */
  async getConversation(threadId) {
    return _select('instagram_dm_conversations', {
      filters: { instagram_thread_id: threadId },
      single: true,
    });
  },

  /**
   * Resolve instagram_thread_id to UUID (batch).
   */
  async resolveThreadIds(threadIds) {
    if (!threadIds || threadIds.length === 0) return { success: true, data: [], error: null, latencyMs: 0 };
    return _select('instagram_dm_conversations', {
      inFilters: { instagram_thread_id: [...new Set(threadIds)] },
      select: 'id, instagram_thread_id',
      limit: threadIds.length,
    });
  },

  /**
   * Fix message conversation IDs (batch update).
   */
  async fixMessageConversationIds(updates, opts = {}) {
    const results = [];
    for (const { messageId, conversationId } of updates) {
      const r = await _update('instagram_dm_messages',
        { instagram_thread_id: conversationId },
        { instagram_message_id: messageId },
        { ...opts, domain: 'ugc' }
      );
      results.push(r);
    }
    return results;
  },
};

// ── Publishing Substrate ───────────────────────────────────────────────────

const publishing = {
  /**
   * Persist a publication checkpoint (post_queue entry).
   * Checkpoint recovery must be restart-safe.
   */
  async persistPublicationCheckpoint(row, opts = {}) {
    return _upsert('post_queue', row, {
      ...opts,
      domain: 'publishing',
      onConflict: 'id',
    });
  },

  /**
   * Persist a scheduled post checkpoint.
   */
  async persistScheduledPost(row, opts = {}) {
    return _upsert('scheduled_posts', row, {
      ...opts,
      domain: 'publishing',
      onConflict: 'id',
    });
  },

  /**
   * Persist a media stub (instagram_media).
   */
  async persistMediaStub(rows, opts = {}) {
    return _upsert('instagram_media', rows, {
      ...opts,
      domain: 'publishing',
      onConflict: 'instagram_media_id',
    });
  },

  /**
   * Get pending publications from the queue.
   */
  async getPendingPublications(opts = {}) {
    return _select('post_queue', {
      filters: { status: 'pending' },
      order: { column: 'created_at', ascending: true },
      limit: opts.limit || 50,
    });
  },

  /**
   * Get publication state by ID.
   */
  async getPublicationState(postId) {
    return _select('post_queue', {
      filters: { id: postId },
      single: true,
    });
  },

  /**
   * Get scheduled posts for an account.
   */
  async getScheduledPosts(accountId, opts = {}) {
    return _select('scheduled_posts', {
      filters: { account_id: accountId },
      order: { column: 'scheduled_at', ascending: true },
      limit: opts.limit || 50,
    });
  },

  /**
   * Upload publication media to storage.
   */
  async uploadMedia(file, filename, opts = {}) {
    return _storage.upload(_STORAGE_BUCKETS.MEDIA, filename, file, opts);
  },

  /**
   * Get a signed URL for a media asset.
   */
  async getMediaSignedUrl(path, ttlSeconds = 3600) {
    return _storage.createSignedUrl(_STORAGE_BUCKETS.MEDIA, path, ttlSeconds);
  },

  /**
   * Get public URL for a media asset.
   */
  getMediaPublicUrl(path) {
    return _storage.getPublicUrl(_STORAGE_BUCKETS.MEDIA, path);
  },
};

// ── Insights Substrate ─────────────────────────────────────────────────────

const insights = {
  /**
   * Persist an insight snapshot.
   */
  async persistSnapshot(rows, opts = {}) {
    return _upsert('ugc_content', rows, {
      ...opts,
      domain: 'insights',
      onConflict: 'id',
    });
  },

  /**
   * Get insight snapshots for an account.
   */
  async getInsightSnapshots(accountId, opts = {}) {
    return _select('ugc_content', {
      filters: { account_id: accountId },
      order: { column: 'created_at', ascending: false },
      limit: opts.limit || 50,
    });
  },

  /**
   * Get recent media for an account.
   */
  async getRecentMedia(accountId, opts = {}) {
    return _select('instagram_media', {
      filters: { business_account_id: accountId },
      order: { column: 'published_at', ascending: false },
      limit: opts.limit || 10,
      select: opts.select || 'instagram_media_id',
    });
  },

  /**
   * Get monitored hashtags for an account.
   */
  async getMonitoredHashtags(accountId) {
    return _select('ugc_monitored_hashtags', {
      filters: { business_account_id: accountId, is_active: true },
      select: 'hashtag',
      limit: 200,
    });
  },

  /**
   * Resolve instagram_media_id to UUID (batch).
   */
  async resolveMediaIds(mediaIds) {
    if (!mediaIds || mediaIds.length === 0) return { success: true, data: [], error: null, latencyMs: 0 };
    return _select('instagram_media', {
      inFilters: { instagram_media_id: [...new Set(mediaIds)] },
      select: 'id, instagram_media_id',
      limit: mediaIds.length,
    });
  },

  /**
   * Persist insight media metadata (UPSERT instagram_media).
   */
  async persistInsightMedia(metadata, opts = {}) {
    return _upsert('instagram_media', metadata, {
      ...opts,
      domain: 'insights',
      onConflict: 'instagram_media_id',
    });
  },

  /**
   * Persist insight metric rows (INSERT into instagram_media_insights).
   * Append-only time-series — not idempotent, not upserted.
   */
  async persistInsightMetrics(metricRows, opts = {}) {
    return _insert('instagram_media_insights', metricRows, {
      ...opts,
      domain: 'insights',
    });
  },
};

// ── Token Substrate ────────────────────────────────────────────────────────

const token = {
  /**
   * Persist credential state (token upsert).
   * This is a multi-step operation — for complex flows, use credential-store substrate
   * which orchestrates key provision → encrypt → business account upsert → credential upsert.
   * This method covers simple credential state updates.
   */
  async persistCredentialState(row, opts = {}) {
    return _upsert('instagram_credentials', row, {
      ...opts,
      domain: 'token',
      onConflict: 'user_id,business_account_id,token_type',
    });
  },

  /**
   * Persist business account record.
   */
  async persistBusinessAccount(row, opts = {}) {
    return _upsert('instagram_business_accounts', row, {
      ...opts,
      domain: 'token',
      onConflict: 'user_id,instagram_business_id',
    });
  },

  /**
   * Update credential status (e.g. debug_token_checked, is_active).
   */
  async updateCredentialStatus(credentialId, updates, opts = {}) {
    return _update('instagram_credentials', updates, { id: credentialId }, {
      ...opts,
      domain: 'token',
    });
  },

  /**
   * Get credential health for an account.
   */
  async getCredentialHealth(businessAccountId) {
    return _select('instagram_credentials', {
      filters: { business_account_id: businessAccountId, is_active: true },
      select: 'id, user_id, token_type, expires_at, data_access_expires_at, debug_token_checked_at, issued_at',
      limit: 20,
    });
  },

  /**
   * Get active page credentials (batch scan).
   */
  async getActivePageCredentials(opts = {}) {
    const filters = { token_type: 'page', is_active: true };
    if (opts.businessAccountId) filters.business_account_id = opts.businessAccountId;
    return _select('instagram_credentials', {
      filters,
      select: 'id, user_id, business_account_id, debug_token_checked_at, issued_at',
      limit: opts.limit || 200,
    });
  },

  /**
   * Get expiring user access tokens within a window.
   */
  async getExpiringUATs(windowDays = 14, opts = {}) {
    const cutoff = new Date(Date.now() + windowDays * 24 * 60 * 60 * 1000).toISOString();
    const filters = { token_type: 'user', is_active: true };
    if (opts.businessAccountId) filters.business_account_id = opts.businessAccountId;
    return _select('instagram_credentials', {
      filters,
      notNull: ['expires_at'],
      lt: { expires_at: cutoff },
      select: 'id, user_id, business_account_id, expires_at',
      limit: opts.limit || 200,
    });
  },

  /**
   * Persist a token lifecycle event.
   */
  async persistLifecycleEvent(row, opts = {}) {
    return _upsert('token_lifecycle_events', row, {
      ...opts,
      domain: 'token',
      onConflict: 'id',
    });
  },

  /**
   * Persist a system alert.
   */
  async persistAlert(row, opts = {}) {
    return _upsert('system_alerts', row, {
      ...opts,
      domain: 'token',
      onConflict: 'id',
    });
  },

  /**
   * Get unresolved alerts for an account.
   */
  /**
   * Get active connected business accounts.
   */
  async getActiveBusinessAccounts() {
    return _select('instagram_business_accounts', {
      filters: { is_connected: true, connection_status: 'active' },
      select: 'id, instagram_business_id, user_id',
      limit: 500,
    });
  },

  /**
   * Resolve instagram_business_id to user_id (batch).
   */
  async resolveBusinessAccountIds(igIds) {
    if (!igIds || igIds.length === 0) return { success: true, data: [], error: null, latencyMs: 0 };
    return _select('instagram_business_accounts', {
      inFilters: { instagram_business_id: [...new Set(igIds)] },
      select: 'instagram_business_id, user_id',
      limit: igIds.length,
    });
  },

  /**
   * Get a single business account by UUID.
   */
  async getBusinessAccount(businessAccountId) {
    if (!businessAccountId) return { success: false, data: null, error: 'businessAccountId required', latencyMs: 0 };
    return _select('instagram_business_accounts', {
      filters: { id: businessAccountId },
      single: true,
    });
  },

  /**
   * Get unresolved alerts for an account.
   */
  async getAlerts(accountId, opts = {}) {
    return _select('system_alerts', {
      filters: { account_id: accountId, resolved: false },
      order: { column: 'created_at', ascending: false },
      limit: opts.limit || 50,
    });
  },

  /**
   * Check if a specific alert type already exists unresolved.
   */
  async checkExistingWarning(businessAccountId, alertType) {
    return _select('system_alerts', {
      filters: { business_account_id: businessAccountId, alert_type: alertType, resolved: false },
      maybeSingle: true,
    });
  },

  /**
   * Get scope cache for a credential.
   */
  async getScopeCache(credentialId) {
    if (!credentialId) return { success: false, data: null, error: 'credentialId required', latencyMs: 0 };
    return _select('instagram_credentials', {
      filters: { id: credentialId },
      select: 'scope_cache, scope_cache_updated_at',
      single: true,
    });
  },

  /**
   * Update scope cache for a credential.
   */
  async updateScopeCache(credentialId, scopes, opts = {}) {
    if (!credentialId) return { success: false, error: 'credentialId required' };
    return _update('instagram_credentials',
      { scope_cache: scopes, scope_cache_updated_at: new Date().toISOString() },
      { id: credentialId },
      { ...opts, domain: 'token' }
    );
  },

  /**
   * Get encryption key for a business account.
   */
  async getEncryptionKey(userId, businessAccountId) {
    if (!userId || !businessAccountId) return { success: false, data: null, error: 'userId and businessAccountId required', latencyMs: 0 };
    return _select('instagram_business_accounts', {
      filters: { user_id: userId, id: businessAccountId },
      select: 'encryption_key_id',
      maybeSingle: true,
    });
  },

  /**
   * Provision an encryption key (create vault secret if none exists).
   * Returns { encryptionKeyId } or null if using shared key.
   */
  async provisionEncryptionKey(userId, igBusinessAccountId) {
    const supabase = _client();
    const crypto = require('crypto');

    // Check existing key on business account
    const existing = await _select('instagram_business_accounts', {
      filters: { user_id: userId, instagram_business_id: igBusinessAccountId },
      select: 'encryption_key_id',
      maybeSingle: true,
    });

    if (existing.success && existing.data?.encryption_key_id) {
      return { success: true, encryptionKeyId: existing.data.encryption_key_id };
    }

    // No existing key — create one in vault
    try {
      const userKey = crypto.randomBytes(32).toString('hex');
      const { data: vaultSecret, error: vaultError } = await supabase
        .schema('vault')
        .from('secrets')
        .insert({
          name: `instagram_token_key_${userId}`,
          secret: userKey,
          description: `Per-user Instagram token encryption key for user ${userId}`,
        })
        .select('id')
        .single();

      if (vaultError) {
        console.warn('Key provisioning error, using shared key:', vaultError.message);
        return { success: true, encryptionKeyId: null };
      }

      return { success: true, encryptionKeyId: vaultSecret.id };
    } catch (err) {
      console.warn('Key provisioning error, using shared key:', err.message);
      return { success: true, encryptionKeyId: null };
    }
  },

  /**
   * Count user profiles (boot health check).
   */
  async countUserProfiles() {
    const supabase = _client();
    const startMs = Date.now();
    try {
      const { count, error } = await supabase
        .from('user_profiles')
        .select('*', { count: 'exact', head: true });
      const latencyMs = Date.now() - startMs;
      if (error) return { success: false, data: null, error: error.message, latencyMs };
      return { success: true, data: count || 0, error: null, latencyMs };
    } catch (err) {
      return { success: false, data: null, error: err.message, latencyMs: Date.now() - startMs };
    }
  },

  /**
   * Check API usage for rate limiting.
   */
  async checkApiUsage(userId, hourLimit = 200) {
    const now = new Date();
    const hourBucket = new Date(now);
    hourBucket.setMinutes(0, 0, 0);

    const result = await _select('api_usage', {
      filters: { user_id: userId },
      gt: { hour_bucket: hourBucket.toISOString() },
      select: 'request_count',
      limit: 1000,
    });

    if (!result.success) return { success: false, data: null, error: result.error, latencyMs: result.latencyMs };

    const current = (result.data || []).reduce((sum, row) => sum + (row.request_count || 0), 0);
    return {
      success: true,
      data: { current, limit: hourLimit, remaining: Math.max(0, hourLimit - current) },
      error: null,
      latencyMs: result.latencyMs,
    };
  },

  /**
   * Persist API usage record.
   */
  async persistApiUsage(row, opts = {}) {
    return _upsert('api_usage', row, {
      ...opts,
      domain: 'token',
      onConflict: 'user_id,business_account_id,endpoint,hour_bucket',
    });
  },

  /**
   * Get lifecycle events (filtered).
   */
  async getLifecycleEvents(opts = {}) {
    const filters = {};
    if (opts.accountId) filters.business_account_id = opts.accountId;
    if (opts.credentialId) filters.credential_id = opts.credentialId;
    if (opts.eventType) filters.event_type = opts.eventType;
    return _select('token_lifecycle_events', {
      filters: Object.keys(filters).length > 0 ? filters : undefined,
      order: { column: 'created_at', ascending: false },
      limit: opts.limit || 50,
    });
  },

  /**
   * Get alerts by type.
   */
  async getAlertsByType(alertType, opts = {}) {
    const filters = { alert_type: alertType };
    if (opts.accountId) filters.business_account_id = opts.accountId;
    return _select('system_alerts', {
      filters,
      order: { column: 'created_at', ascending: false },
      limit: opts.limit || 50,
    });
  },

  /**
   * Get unresolved alerts.
   */
  async getUnresolvedAlerts(opts = {}) {
    const filters = { resolved: false };
    if (opts.accountId) filters.business_account_id = opts.accountId;
    return _select('system_alerts', {
      filters,
      order: { column: 'created_at', ascending: false },
      limit: opts.limit || 50,
    });
  },
};

// ── RPC Operations (atomic multi-table) ────────────────────────────────────

const rpc = {
  /**
   * Encrypt an Instagram token via RPC.
   */
  async encryptToken(token, encryptionKeyId) {
    return _rpc('encrypt_instagram_token', {
      p_token: token,
      p_encryption_key_id: encryptionKeyId,
    });
  },

  /**
   * Decrypt an Instagram token via RPC.
   */
  async decryptToken(encryptedToken, encryptionKeyId) {
    return _rpc('decrypt_instagram_token', {
      p_encrypted_token: encryptedToken,
      p_encryption_key_id: encryptionKeyId,
    });
  },

  /**
   * Generic RPC call — for future atomic multi-table operations.
   * Workers should prefer domain-specific facades; use this for one-off
   * operations that don't yet have a domain facade.
   */
  async call(fnName, args, opts = {}) {
    return _rpc(fnName, args, opts);
  },
};

// ── Realtime Operations ────────────────────────────────────────────────────

const realtime = {
  /**
   * Publish a normalized event to a realtime channel.
   */
  async publishEvent(eventType, payload) {
    const channel = eventType.startsWith('PUBLICATION')
      ? _REALTIME_CHANNELS.PUBLISHING
      : eventType.startsWith('INSIGHT')
        ? _REALTIME_CHANNELS.INSIGHTS
        : eventType.startsWith('TOKEN')
          ? _REALTIME_CHANNELS.TOKEN
          : _REALTIME_CHANNELS.COMMENTS;

    return _realtime.publish(channel, eventType, payload);
  },

  /**
   * Subscribe to Postgres changes on a table.
   */
  subscribeToTable(table, opts = {}) {
    return _realtime.subscribeToTable(table, opts);
  },
};

// ── Storage Operations ─────────────────────────────────────────────────────

const storage = {
  async upload(bucket, path, file, opts) {
    return _storage.upload(bucket, path, file, opts);
  },
  getPublicUrl(bucket, path) {
    return _storage.getPublicUrl(bucket, path);
  },
  async createSignedUrl(bucket, path, ttlSeconds) {
    return _storage.createSignedUrl(bucket, path, ttlSeconds);
  },
  async remove(bucket, paths) {
    return _storage.remove(bucket, paths);
  },
};

// ═══════════════════════════════════════════════════════════════════════════
// EXPORTS
// ═══════════════════════════════════════════════════════════════════════════

module.exports = {
  // Domain facades (the ONLY surface workers should touch)
  ugc,
  publishing,
  insights,
  token,

  // Infrastructure facades (for domain substrates that need them)
  rpc,
  realtime,
  storage,

  // Health
  healthCheck: _healthCheck,

  // Constants (read-only, for reference)
  TABLE_PK_MAP: _TABLE_PK_MAP,
  STORAGE_BUCKETS: _STORAGE_BUCKETS,
  REALTIME_EVENTS: _REALTIME_EVENTS,

  // Test-only injection
  _injectClient,
};
