# persistence.js Decomposition Issues

Source file (DELETED): `substrates/persistence.js` — 8 functions, ~230 lines.
Decomposed across 4 kernels + 3 hydrators + 5 writers. 6 issues found.

---

## Architecture Summary

The old file conflated three concerns in one function per domain:
1. Normalization (field mapping, FK resolution)
2. Hydration (media UUID lookup, customer_user_id resolution, stub creation)
3. Upsert (raw Supabase call)

Post-decomposition: hydration lives in `engagement/hydrators/`, normalization lives in domain normalizers, upsert lives in `postgres-telemetry-kernel/writers/`. The `persist()` functions in each substrate index.js are supposed to sequence these three layers.

---

## Issue 1 [LOAD-BEARING]: `batch_upsert_media_stubs` not registered

**Severity:** HIGH — silent data loss on all media stub creation

**Where:**
- `acquisition-kernel/substrates/engagement/index.js:103`
- `acquisition-kernel/parsing/workers/comments-worker.js:42`

Both dispatch `batch_upsert_media_stubs` but NO writer is registered for this operation in `postgres-telemetry-kernel/writers/registry.js`. The registry maps:

```
batch_upsert_comments       → comments-writer
batch_upsert_messages       → messages-writer
batch_upsert_conversations  → conversations-writer
batch_upsert_posts          → content-writer
batch_upsert_insights       → content-writer
batch_upsert_ugc            → ugc-writer
batch_fix_message_conversation_ids → message-fix-writer
```

`batch_upsert_media_stubs` is missing. Result: `registry.getWriter('batch_upsert_media_stubs')` returns null → `dispatchWrite` emits `DB_WRITE_COMPLETE` with `error: 'unknown_operation'`. Stubs are never created. Comments referencing unknown media IDs fail FK constraints.

**Fix:** Add `batch_upsert_media_stubs: './content-writer'` to registry (same table as posts/insights, same onConflict: 'instagram_media_id').

**Affected agents:** Any agent doing engagement fetch on posts not yet in `instagram_media`.

---

## Issue 2 [GOVERNANCE]: engagement/index.js bypasses CK for stub writes

**Severity:** MEDIUM — bypasses persist-telemetry-fsm gate

**Where:** `acquisition-kernel/substrates/engagement/index.js:103-106`

```js
dispatchWrite('batch_upsert_media_stubs', {
  domain: 'media', accountId, intentId: null, table: 'instagram_media',
  rows: stubs,
});
```

This calls `dispatchWrite()` directly — fire-and-forget via `setImmediate`. It does NOT dispatch `DB_WRITE_REQUESTED` through governance. Compare with the correct path in `comments-worker.js:62-72`:

```js
governance.dispatch({
  type: 'DB_WRITE_REQUESTED',
  domain: 'comments',
  accountId, intentId,
  table: 'instagram_comments',
  operation: 'batch_upsert_comments',
  rows,
});
```

The direct path means the persist-telemetry-fsm never sees the write, never gates it, never emits DB_WRITE_COMPLETE properly. Any downstream subscriber (orphan repair, telemetry) never learns about it.

**Fix:** Replace direct `dispatchWrite` calls in `engagement/index.js` persist() with `governance.dispatch({type: 'DB_WRITE_REQUESTED', ...})`. The function already has access to `extra._governance`.

---

## Issue 3 [DUPLICATION]: message transform is inline, ignores normalizer

**Severity:** MEDIUM — divergence between normalize+write paths

**Where:** `acquisition-kernel/substrates/engagement/index.js:151-177`

The file imports `transformMessage` at line 15 but lines 151-177 contain inline field mapping for messages instead:

```js
rows.push({
  instagram_message_id: m.id,
  message_text: m.message || null,
  message_type: messageType,
  media_url: mediaUrl,
  // ... 10+ fields mapped manually
});
```

The `transformMessage()` function in `engagement/normalizer.js` already does this mapping and adds `pageId`, `customerIgId`, and other fields. The inline version misses those. Two paths produce differently-shaped rows for the same `instagram_dm_messages` table.

**Fix:** Replace inline mapping with `transformMessage(m, conversationUUID, accountId, igUserId, pageId, customerIgId)`.

---

## Issue 4 [DATA INTEGRITY]: messages persist uses raw Instagram thread ID as conversation_id FK

**Severity:** HIGH — foreign key corruption

**Where:** `acquisition-kernel/substrates/engagement/index.js:164-170`

```js
conversation_id: rawData.conversationId || 'direct',
```

`rawData.conversationId` is the Instagram thread ID (string like `aWQ6MTIz...`), NOT the DB UUID from `instagram_dm_conversations.id`. The old `storeMessageBatches` resolved conversation UUIDs via a SELECT on `instagram_dm_conversations` and called `ensureConversationRows` for missing ones BEFORE inserting messages.

The new code does neither. Messages land with conversation_id pointing to Instagram thread IDs, not UUIDs. Any JOIN or FK constraint expecting a UUID will fail.

**Fix:** Before message upsert, batch-resolve conversation UUIDs from thread IDs. Pattern already exists: `conversations-writer.js` + `accounts-worker.js` have both `igThreadIdToUuid` and `ensureConversationRows`. Either governed-read the UUIDs OR call ensureConversationRows from agent-helpers (still exists, just unused).

---

## Issue 5 [DUPLICATION]: content and insights persist duplicate normalizer logic

**Severity:** LOW — drift risk

**Where:**
- `acquisition-kernel/substrates/content/index.js:34-47` — inline field mapping for posts
- `acquisition-kernel/substrates/insights/index.js:34-53` — inline field mapping for insights

The old `storeBusinessPosts` called `normalizeBusinessPost(p, businessAccountId)` from `substrates/content/normalizer.js`. The old `storeMediaInsightsBatch` called `normalizeMediaInsight(m, businessAccountId)` from `substrates/insights/normalizer.js`.

New code maps fields inline. If the normalizer changes (adds a field, changes a default), the inline paths won't pick it up. Currently harmless since the inline mapping produces identical output, but it's a future divergence point.

**Fix:** Call `normalizeBusinessPost()` and `normalizeMediaInsight()` from their respective normalizer modules.

---

## Issue 6 [MISSING FEATURE]: syncHashtagsFromCaptions is absent, not deferred

**Severity:** MEDIUM — feature gap

**Where:**
- `acquisition-kernel/substrates/content/index.js:30` — comment: "deferred to Phase 4 enrichment membrane"
- `acquisition-kernel/substrates/insights/index.js:30` — same comment

`syncHashtagsFromCaptions` still exists in `helpers/agent-helpers.js:173-193`. It extracts hashtags from post/insight captions and upserts into `ugc_monitored_hashtags`. The old persistence.js called it after every media insights and business posts upsert.

No Phase 4 membrane exists. The function is simply never called. Hashtag discovery from own-brand posts is dead.

**Fix:** Two options:
1. Call `syncHashtagsFromCaptions` inline after dispatchWrite in content+insights persist() (simple, but bypasses CK)
2. Emit a `HASHTAGS_EXTRACTED` event after media upsert, have a subscriber call syncHashtagsFromCaptions (constitutional, more work)

---

## Dual Persist Paths (by design, not a bug)

Two paths can persist comments:
- **Path A (CK):** `comments-worker.js` → `governance.dispatch(DB_WRITE_REQUESTED)` → FSM → writer
- **Path B (direct):** `engagement/index.js` persist() → `dispatchWrite()` directly

Path A is the canonical production path. Path B is the legacy path triggered by the old `{fetch, persist}` contract used by retry workers. The skill memory documents this as: "Path A (PRODUCTION) = retry-worker → parsing workers → CK(DB_WRITE_REQUESTED) → writers. Path B (DEAD) = engagement/index.js persist()."

Not a bug — but Issue 2 means Path B's writes are invisible to governance.

---

## Fix Priority

| # | Priority | What | Where |
|---|----------|------|-------|
| 1 | P0 | Register batch_upsert_media_stubs in writer registry | `postgres-telemetry-kernel/writers/registry.js` |
| 4 | P0 | Resolve conversation UUIDs before message upsert | `engagement/index.js` persist() |
| 2 | P1 | Route engagement stub writes through CK | `engagement/index.js` persist() |
| 3 | P1 | Use transformMessage instead of inline mapping | `engagement/index.js` persist() |
| 6 | P2 | Restore syncHashtagsFromCaptions | `content/index.js` + `insights/index.js` persist() |
| 5 | P3 | Use normalizers instead of inline mapping | `content/index.js` + `insights/index.js` persist() |

---

## Agent Coordination Notes

Issues 1-4 all touch `engagement/index.js` persist() — single agent can handle all four.
Issue 5 touches content+insights persist() — separate or same agent.
Issue 6 touches content+insights persist() — pair with Issue 5.

After fixing, smoke-test:
```bash
node -e "require('./acquisition-kernel/substrates/engagement/index')"
node -e "require('./acquisition-kernel/substrates/content/index')"
node -e "require('./acquisition-kernel/substrates/insights/index')"
node -e "require('./postgres-telemetry-kernel/writers/registry')"
```
