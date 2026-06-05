 
    DECOMPOSITION REPORT: substrates/normalization.js
    
    STATUS: DEAD CODE — orphaned. Zero require() calls to this file anywhere in the codebase. All 5 functions were extracted into kernel-specific normalizer files. The file can be deleted immediately with no consumer impact.
    
    ============================================================
    1. FUNCTION-TO-KERNEL MAPPING
    ============================================================
    
      normalizeBusinessPost (line 103)
        → acquisition-kernel/substrates/content/normalizer.js:16
        → verbatim copy, imported by content/index.js:11
        → called in content/index.js persist() line 41
    
      normalizeComment (line 22)
        → acquisition-kernel/substrates/engagement/normalizer.js:18
        → verbatim copy, imported by engagement/index.js:15
        → called in engagement/index.js persist() lines 121, 141
    
      transformMessage (line 52)
        → acquisition-kernel/substrates/engagement/normalizer.js:44
        → verbatim copy, imported by engagement/index.js:15
        → called in engagement/index.js persist() line 179
    
      mapRawPostToUgcContent (line 169)
        → acquisition-kernel/substrates/ugc/normalizer.js:28
        → imported by ugc/index.js:12
        → called in ugc/index.js persist() line 25
        → BUG DIVERGENCE (see §5)
    
      normalizeMediaInsight (line 130)
        → acquisition-kernel/substrates/insights/normalizer.js:18
        → verbatim copy, imported by insights/index.js:11
        → called in insights/index.js persist() line 41
    
      syncHashtagsFromCaptions (re-export, line 8/199)
        → legacy re-exported from helpers/agent-helpers.js
        → kernel files import directly: content/index.js:12, insights/index.js:12
        → no consumer of this re-export remains
    
    ============================================================
    2. MISSING NORMALIZER: CONVERSATIONS
    ============================================================
    
    The engagement/index.js persist() path for conversations (lines 206-216) uses INLINE field mapping — no normalizer function exists:
    
        const rows = records.map(r => ({
          instagram_thread_id: r.id,
          customer_instagram_id: r.customer_instagram_id,
          customer_username: r.customer_username,
          business_account_id: accountId,
          customer_user_id: r.customer_user_id || null,
          last_message_at: r.updated_time,
          last_user_message_at: r.last_customer_message_at,
          message_count: r.message_count,
          conversation_status: 'active',
        }));
    
    This is a direct violation of the constitutional persist() pattern (no inline field mapping). A normalizeConversation() function does not exist in any normalizer file — it would be the 6th function to extract.
    
    ============================================================
    3. ARCHITECTURAL ISSUES (POST-EXTRACTION)
    ============================================================
    
    All 5 kernel normalizers are IDENTICAL in structure to the legacy file. The extraction was mechanical — it moved functions into kernel-namespaced files without addressing the architectural problems.
    
    3a. No canonical domain model exists.
    
    Every function produces Supabase-specific row shapes. Examples:
    
      normalizeBusinessPost → { instagram_media_id, business_account_id, media_type, caption, ... }
      normalizeComment → { instagram_comment_id, media_id, business_account_id, ... }
      transformMessage → { instagram_message_id, conversation_id, business_account_id, ... }
      normalizeMediaInsight → { instagram_media_id, business_account_id, reach, impressions, saves, ... }
      mapRawPostToUgcContent → { business_account_id, visitor_post_id, quality_score, quality_tier, ... }
    
    No CanonicalMessage, CanonicalComment, or CanonicalUgcPost exists. This means:
    
    - The normalization layer depends on the persistence schema
    - If Supabase columns change, normalizers change
    - If storage changes (Postgres→DynamoDB), every normalizer must be rewritten
    - Normalization cannot be tested independently of the database schema
    
    3b. transformMessage is a classifier, not a normalizer.
    
    Lines 44-81 of the engagement normalizer perform domain classification:
    
      fromBusiness determination (line 45) — policy decision
      messageType classification (lines 65-70) — semantic classification
      send_status derivation (line 88) — interaction-state calculation
      recipient fallback resolution (lines 83-85) — routing logic
    
    These are NOT format translations. They are domain semantics. A proper decomposition would split transformMessage into:
    
      1. normalizeMessage() — raw IG payload → CanonicalMessage (pure mapping)
      2. classifyMessage() — CanonicalMessage → classified enrichment (message_type, is_from_business)
      3. messageToDbRow() — enriched CanonicalMessage → instagram_dm_messages row (persistence adapter)
    
    Currently all three responsibilities are fused in one function.
    
    3c. UUID leakage into normalization.
    
    All 5 functions accept database identifiers as parameters and embed them directly into output objects:
    
      normalizeComment(comment, mediaUUID, ...)
      transformMessage(m, conversationUUID, ...)
      normalizeBusinessPost(post, businessAccountId)
      normalizeMediaInsight(item, businessAccountId)
      mapRawPostToUgcContent(post, businessAccountId, ...)
    
    A normalizer should not know about Supabase UUIDs. It should produce canonical entities. Persistence adapters should inject storage-specific identifiers later.
    
    3d. mapRawPostToUgcContent creates domain records.
    
    Lines 43-44 of the UGC normalizer set:
    
      quality_score: null,
      quality_tier: null,
    
    These are NOT from the Instagram payload. They are system-invented fields belonging to a UGC quality projection layer. Setting them to null during normalization creates a false dependency — the normalizer now owns quality scoring schema, and any quality pipeline must know that normalization initialized these fields.
    
    3e. syncHashtagsFromCaptions is semantically misplaced.
    
    The legacy file imported and re-exported syncHashtagsFromCaptions, a side-effect function that writes to ugc_monitored_hashtags. This created a false ownership boundary — normalization appears to own hashtag sync. Kernel files fixed this by importing directly from helpers/agent-helpers.js, but the function is still called from within content/index.js and insights/index.js persist() paths as a fire-and-forget side-effect. This is enrichment/hydration, not normalization.
    
    ============================================================
    4. CALL SITE ANALYSIS
    ============================================================
    
    4a. CONTENT (content/index.js)
    
      normalizeBusinessPost(p, accountId) → single call, line 41
      Called in: persist() after filter(p => p.id)
      Governance: CK(DB_WRITE_REQUESTED) with table: instagram_media
      Side-effect: syncHashtagsFromCaptions after dispatch (lines 54-58)
      Assessment: clean path. One function → one dispatch. No issues beyond architectural.
    
    4b. ENGAGEMENT — COMMENTS (engagement/index.js)
    
      normalizeComment(c, uuid, accountId) → two call sites
      Line 121: inside batches loop with resolved media UUID
      Line 141: direct comments with 'direct' fallback UUID
      Hydration precedes normalization: mediaHydrator.hydrate() resolves IG IDs → DB UUIDs
      Governance: CK(DB_WRITE_REQUESTED) with table: instagram_comments
      Assessment: hydrate → normalize → dispatch pattern is correct.
    
    4c. ENGAGEMENT — MESSAGES (engagement/index.js)
    
      transformMessage(m, conversationUUID, accountId, igUserId, pageId, null)
      Line 179: inside rawMessages loop
      Hydration: governedRead('db.accounts', {query:'igThreadIdToUuid'}) resolves thread ID → DB UUID (lines 162-170)
      Note: customerIgId passed as null — recipient fallback uses only igUserId
      Assessment: hydrate → normalize → dispatch pattern is correct. But null customerIgId means recipient resolution is degraded.
    
    4d. ENGAGEMENT — CONVERSATIONS (engagement/index.js)
    
      NO normalizer function used. Inline .map() at lines 206-216.
      Hydration: conversationHydrator.hydrate() for customer_user_id resolution
      Governance: CK(DB_WRITE_REQUESTED) with table: instagram_dm_conversations
      Assessment: violation of constitutional persist() pattern. Missing normalizeConversation().
    
    4e. UGC (ugc/index.js)
    
      mapRawPostToUgcContent(p, accountId, source, rawData.cleanHashtag)
      Line 25: inside records.map()
      No hydration step. No FK resolution.
      Governance: CK(DB_WRITE_REQUESTED) with table: ugc_content
      Assessment: clean path. No hydration needed (UGC is self-contained).
    
    4f. INSIGHTS (insights/index.js)
    
      normalizeMediaInsight(item, accountId)
      Line 41: inside insights.map()
      No hydration step.
      Governance: CK(DB_WRITE_REQUESTED) with table: instagram_media
      Side-effect: syncHashtagsFromCaptions after dispatch (lines 54-58)
      Assessment: clean path. But shares the instagram_media table with normalizeBusinessPost — two normalizers targeting the same table with different field subsets.
    
    ============================================================
    5. BUG DIVERGENCE IN KERNEL EXTRACTION
    ============================================================
    
    UGC normalizer — author_id field:
    
      LEGACY (normalization.js:173):
        author_id: post.owner?.id || post.owner_id || null,
    
      KERNEL (ugc/normalizer.js:32):
        author_id: post.owner_id || null,
    
    The post.owner?.id fallback path was dropped during extraction. This could cause null author_id for posts where owner_id is empty but owner.id is populated (depends on IG API response shape for hashtag vs tagged endpoints).
    
    ============================================================
    6. DEPENDENCY GRAPH SUMMARY
    ============================================================
    
      substrates/normalization.js  ← ZERO imports (DEAD)
        ↑ (formerly imported by — all now use kernel normalizers)
    
      acquisition-kernel/substrates/content/normalizer.js
        ↑ content/index.js (persist)
        ↑ content/index.js imports normalizeBusinessPost
    
      acquisition-kernel/substrates/engagement/normalizer.js
        ↑ engagement/index.js (persist)
        ↑ engagement/index.js imports normalizeComment, transformMessage
    
      acquisition-kernel/substrates/ugc/normalizer.js
        ↑ ugc/index.js (persist)
        ↑ ugc/index.js imports mapRawPostToUgcContent
    
      acquisition-kernel/substrates/insights/normalizer.js
        ↑ insights/index.js (persist)
        ↑ insights/index.js imports normalizeMediaInsight
    
      External dependency shared by all normalizer callers:
        helpers/agent-helpers.js → syncHashtagsFromCaptions (side-effect)
    
    ============================================================
    7. RECOMMENDED DECOMPOSITION PATH
    ============================================================
    
    IMMEDIATE (safe, no breaking changes):
    
      A) Delete substrates/normalization.js — orphaned, zero consumers.
    
      B) Extract normalizeConversation from engagement/index.js:206-216
         into acquisition-kernel/substrates/engagement/normalizer.js.
         Fixes the only remaining inline field mapping.
    
      C) Fix author_id divergence in ugc/normalizer.js — restore
         post.owner?.id fallback to match legacy behavior.
    
      D) Update skill doc — normalization.js listed as "deleted" in
         the Deleted Modules section but file still exists on disk.
    
    ARCHITECTURAL (introduce canonical model):
    
      E) Define CanonicalMessage, CanonicalComment, CanonicalPost,
         CanonicalUgcPost, CanonicalConversation — plain objects with
         no database field names, no UUIDs.
    
      F) Split transformMessage into:
         - normalizeMessage(m) → CanonicalMessage (pure format translation)
         - classifyMessage(canonical) → enriched CanonicalMessage (type, fromBusiness, status)
         - messageToDbRow(enriched, conversationUUID, accountId) → DB row (persistence adapter)
    
      G) Split mapRawPostToUgcContent similarly:
         - normalizeUgcPost(p, source, sourceHashtag) → CanonicalUgcPost
         - ugcPostToDbRow(canonical, accountId) → DB row
         - Move quality_score/quality_tier initialization to a UGC quality projection worker
    
      H) Remove businessAccountId/UUID parameters from all normalizers.
         Pass them to persistence adapters instead.
    
      I) Move syncHashtagsFromCaptions side-effect call from persist()
         into a HASHTAGS_EXTRACTED subscriber — decouples normalization
         from enrichment.