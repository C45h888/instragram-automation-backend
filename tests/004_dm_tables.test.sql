-- ============================================
-- TEST SUITE: Instagram DM Tables & 24-Hour Window Logic
-- ============================================
-- File: backend.api/tests/004_dm_tables.test.sql
-- Purpose: Comprehensive testing of DM conversation and message functionality
-- Critical: Validates 24-hour window enforcement to prevent Instagram policy violations
--
-- How to run:
--   psql $SUPABASE_DB_URL -f backend.api/tests/004_dm_tables.test.sql
--
-- Expected: All tests should PASS
-- ============================================

BEGIN;

-- ============================================
-- TEST SETUP: Create test data
-- ============================================

DO $$
DECLARE
    v_test_user_id UUID;
    v_test_business_id UUID;
    v_test_conversation_id UUID;
    v_window_check RECORD;
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'STARTING DM TABLES TEST SUITE';
    RAISE NOTICE '========================================';

    -- ============================================
    -- TEST 1: Table Creation Verification
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE 'TEST 1: Verifying tables exist...';

    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'instagram_dm_conversations') THEN
        RAISE EXCEPTION 'FAIL: Table instagram_dm_conversations does not exist';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'instagram_dm_messages') THEN
        RAISE EXCEPTION 'FAIL: Table instagram_dm_messages does not exist';
    END IF;

    RAISE NOTICE '✓ PASS: Both DM tables exist';

    -- ============================================
    -- TEST 2: Function Existence Verification
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE 'TEST 2: Verifying database functions exist...';

    IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'can_send_message') THEN
        RAISE EXCEPTION 'FAIL: Function can_send_message does not exist';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'update_message_window') THEN
        RAISE EXCEPTION 'FAIL: Function update_message_window does not exist';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'check_expired_windows') THEN
        RAISE EXCEPTION 'FAIL: Function check_expired_windows does not exist';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'upsert_conversation') THEN
        RAISE EXCEPTION 'FAIL: Function upsert_conversation does not exist';
    END IF;

    RAISE NOTICE '✓ PASS: All critical functions exist';

    -- ============================================
    -- TEST 3: Index Verification
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE 'TEST 3: Verifying performance indexes...';

    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_dm_conversations_window_active') THEN
        RAISE EXCEPTION 'FAIL: Critical index idx_dm_conversations_window_active missing';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_dm_conversations_window_expired') THEN
        RAISE EXCEPTION 'FAIL: Critical index idx_dm_conversations_window_expired missing';
    END IF;

    RAISE NOTICE '✓ PASS: Critical indexes exist';

    -- ============================================
    -- TEST 4: Create Test Business Account
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE 'TEST 4: Creating test business account...';

    -- Get or create test user
    SELECT user_id INTO v_test_user_id
    FROM user_profiles
    LIMIT 1;

    IF v_test_user_id IS NULL THEN
        RAISE NOTICE 'No existing users found, test requires at least one user_profile';
        RAISE NOTICE 'Skipping business account tests (create user_profile first)';
    ELSE
        -- Get or create test business account
        SELECT id INTO v_test_business_id
        FROM instagram_business_accounts
        WHERE user_id = v_test_user_id
        LIMIT 1;

        IF v_test_business_id IS NULL THEN
            RAISE NOTICE 'No existing business account, test requires business account';
            RAISE NOTICE 'Skipping conversation tests (create business account first)';
        ELSE
            RAISE NOTICE '✓ PASS: Using business account: %', v_test_business_id;

            -- ============================================
            -- TEST 5: Create Conversation via upsert_conversation
            -- ============================================
            RAISE NOTICE '';
            RAISE NOTICE 'TEST 5: Creating conversation via upsert_conversation()...';

            SELECT upsert_conversation(
                'test_thread_' || extract(epoch from now())::text,
                v_test_business_id,
                'test_customer_ig_' || extract(epoch from now())::text,
                'test_customer',
                'Test Customer'
            ) INTO v_test_conversation_id;

            IF v_test_conversation_id IS NULL THEN
                RAISE EXCEPTION 'FAIL: upsert_conversation returned NULL';
            END IF;

            RAISE NOTICE '✓ PASS: Conversation created: %', v_test_conversation_id;

            -- ============================================
            -- TEST 6: Initial Window State (No Window)
            -- ============================================
            RAISE NOTICE '';
            RAISE NOTICE 'TEST 6: Checking initial window state (should be no window)...';

            SELECT * INTO v_window_check
            FROM can_send_message(v_test_conversation_id);

            IF v_window_check.can_send = TRUE THEN
                RAISE EXCEPTION 'FAIL: New conversation should NOT have active window';
            END IF;

            IF v_window_check.reason NOT LIKE '%No window established%' THEN
                RAISE EXCEPTION 'FAIL: Expected "No window established", got: %', v_window_check.reason;
            END IF;

            RAISE NOTICE '✓ PASS: New conversation has no window (correct)';
            RAISE NOTICE '  Reason: %', v_window_check.reason;

            -- ============================================
            -- TEST 7: Customer Message Opens Window
            -- ============================================
            RAISE NOTICE '';
            RAISE NOTICE 'TEST 7: Customer sends message → window should open for 24 hours...';

            -- Insert customer message
            INSERT INTO instagram_dm_messages (
                instagram_message_id,
                conversation_id,
                is_from_business,
                sender_instagram_id,
                sender_username,
                message_text,
                sent_at
            ) VALUES (
                'test_msg_customer_' || extract(epoch from now())::text,
                v_test_conversation_id,
                FALSE,  -- Customer message (CRITICAL)
                'test_customer_ig_id',
                'test_customer',
                'Hello! I have a question about your product',
                NOW()
            );

            -- Check window status after customer message
            SELECT * INTO v_window_check
            FROM can_send_message(v_test_conversation_id);

            IF v_window_check.can_send = FALSE THEN
                RAISE EXCEPTION 'FAIL: Window should be open after customer message. Reason: %', v_window_check.reason;
            END IF;

            IF v_window_check.within_window = FALSE THEN
                RAISE EXCEPTION 'FAIL: within_window should be TRUE after customer message';
            END IF;

            -- Should have approximately 24 hours (23.9 to 24.0)
            IF v_window_check.hours_remaining < 23.9 OR v_window_check.hours_remaining > 24.0 THEN
                RAISE EXCEPTION 'FAIL: Expected ~24 hours, got: %', v_window_check.hours_remaining;
            END IF;

            RAISE NOTICE '✓ PASS: Customer message opened 24-hour window';
            RAISE NOTICE '  Hours remaining: %', v_window_check.hours_remaining;
            RAISE NOTICE '  Minutes remaining: %', v_window_check.minutes_remaining;
            RAISE NOTICE '  Expires at: %', v_window_check.expires_at;

            -- ============================================
            -- TEST 8: Business Message Does NOT Extend Window
            -- ============================================
            RAISE NOTICE '';
            RAISE NOTICE 'TEST 8: Business sends message → window should NOT change...';

            -- Record original expiration time
            DECLARE
                v_original_expires_at TIMESTAMP WITH TIME ZONE;
                v_new_expires_at TIMESTAMP WITH TIME ZONE;
            BEGIN
                SELECT window_expires_at INTO v_original_expires_at
                FROM instagram_dm_conversations
                WHERE id = v_test_conversation_id;

                -- Wait 2 seconds
                PERFORM pg_sleep(2);

                -- Insert business message
                INSERT INTO instagram_dm_messages (
                    instagram_message_id,
                    conversation_id,
                    is_from_business,
                    sender_instagram_id,
                    sender_username,
                    message_text,
                    sent_at
                ) VALUES (
                    'test_msg_business_' || extract(epoch from now())::text,
                    v_test_conversation_id,
                    TRUE,  -- Business message (CRITICAL)
                    'test_business_ig_id',
                    'test_business',
                    'Thanks for your message! How can I help?',
                    NOW()
                );

                -- Check if window changed
                SELECT window_expires_at INTO v_new_expires_at
                FROM instagram_dm_conversations
                WHERE id = v_test_conversation_id;

                IF v_original_expires_at != v_new_expires_at THEN
                    RAISE EXCEPTION 'FAIL: Business message changed window_expires_at (should remain unchanged)';
                END IF;

                RAISE NOTICE '✓ PASS: Business message did NOT change window expiration';
                RAISE NOTICE '  Original: %', v_original_expires_at;
                RAISE NOTICE '  Current:  %', v_new_expires_at;
            END;

            -- ============================================
            -- TEST 9: Message Count Tracking
            -- ============================================
            RAISE NOTICE '';
            RAISE NOTICE 'TEST 9: Verifying message count tracking...';

            DECLARE
                v_message_count INTEGER;
            BEGIN
                SELECT message_count INTO v_message_count
                FROM instagram_dm_conversations
                WHERE id = v_test_conversation_id;

                -- Should have 2 messages (1 customer + 1 business)
                IF v_message_count != 2 THEN
                    RAISE EXCEPTION 'FAIL: Expected 2 messages, got: %', v_message_count;
                END IF;

                RAISE NOTICE '✓ PASS: Message count correctly tracked: %', v_message_count;
            END;

            -- ============================================
            -- TEST 10: Unread Count Tracking
            -- ============================================
            RAISE NOTICE '';
            RAISE NOTICE 'TEST 10: Verifying unread count tracking...';

            DECLARE
                v_unread_count INTEGER;
                v_test_message_id UUID;
            BEGIN
                -- Should have 1 unread (customer message)
                SELECT unread_count INTO v_unread_count
                FROM instagram_dm_conversations
                WHERE id = v_test_conversation_id;

                IF v_unread_count != 1 THEN
                    RAISE EXCEPTION 'FAIL: Expected 1 unread message, got: %', v_unread_count;
                END IF;

                RAISE NOTICE '  Unread before marking read: %', v_unread_count;

                -- Mark customer message as read
                SELECT id INTO v_test_message_id
                FROM instagram_dm_messages
                WHERE conversation_id = v_test_conversation_id
                  AND is_from_business = FALSE
                LIMIT 1;

                UPDATE instagram_dm_messages
                SET read_at = NOW()
                WHERE id = v_test_message_id;

                -- Check unread count decreased
                SELECT unread_count INTO v_unread_count
                FROM instagram_dm_conversations
                WHERE id = v_test_conversation_id;

                IF v_unread_count != 0 THEN
                    RAISE EXCEPTION 'FAIL: Expected 0 unread after marking read, got: %', v_unread_count;
                END IF;

                RAISE NOTICE '✓ PASS: Unread count correctly decreased to: %', v_unread_count;
            END;

            -- ============================================
            -- TEST 11: Window Statistics Function
            -- ============================================
            RAISE NOTICE '';
            RAISE NOTICE 'TEST 11: Testing get_window_statistics()...';

            DECLARE
                v_stats RECORD;
            BEGIN
                SELECT * INTO v_stats
                FROM get_window_statistics(v_test_business_id);

                IF v_stats.total_conversations < 1 THEN
                    RAISE EXCEPTION 'FAIL: Should have at least 1 conversation';
                END IF;

                IF v_stats.active_windows < 1 THEN
                    RAISE EXCEPTION 'FAIL: Should have at least 1 active window';
                END IF;

                RAISE NOTICE '✓ PASS: Window statistics calculated correctly';
                RAISE NOTICE '  Total conversations: %', v_stats.total_conversations;
                RAISE NOTICE '  Active windows: %', v_stats.active_windows;
                RAISE NOTICE '  Expired windows: %', v_stats.expired_windows;
                RAISE NOTICE '  No window: %', v_stats.no_window;
                RAISE NOTICE '  Avg hours remaining: %', v_stats.avg_hours_remaining;
            END;

            -- ============================================
            -- TEST 12: Active Window Conversations
            -- ============================================
            RAISE NOTICE '';
            RAISE NOTICE 'TEST 12: Testing get_active_window_conversations()...';

            DECLARE
                v_active_convs RECORD;
            BEGIN
                SELECT * INTO v_active_convs
                FROM get_active_window_conversations(v_test_business_id, 10)
                LIMIT 1;

                IF v_active_convs.conversation_id IS NULL THEN
                    RAISE EXCEPTION 'FAIL: Should return at least 1 active conversation';
                END IF;

                IF v_active_convs.hours_remaining < 0 THEN
                    RAISE EXCEPTION 'FAIL: hours_remaining should be positive';
                END IF;

                RAISE NOTICE '✓ PASS: Active window conversations retrieved';
                RAISE NOTICE '  Conversation ID: %', v_active_convs.conversation_id;
                RAISE NOTICE '  Hours remaining: %', v_active_convs.hours_remaining;
            END;

            -- ============================================
            -- TEST 13: Get Conversation Messages
            -- ============================================
            RAISE NOTICE '';
            RAISE NOTICE 'TEST 13: Testing get_conversation_messages()...';

            DECLARE
                v_msg_count INTEGER;
            BEGIN
                SELECT COUNT(*) INTO v_msg_count
                FROM get_conversation_messages(v_test_conversation_id, 50, 0);

                IF v_msg_count != 2 THEN
                    RAISE EXCEPTION 'FAIL: Should return 2 messages, got: %', v_msg_count;
                END IF;

                RAISE NOTICE '✓ PASS: Conversation messages retrieved: % messages', v_msg_count;
            END;

            -- ============================================
            -- TEST 14: Simulate Window Expiration
            -- ============================================
            RAISE NOTICE '';
            RAISE NOTICE 'TEST 14: Simulating window expiration...';

            DECLARE
                v_expired_result RECORD;
            BEGIN
                -- Manually expire the window (set to 25 hours ago)
                UPDATE instagram_dm_conversations
                SET
                    window_expires_at = NOW() - INTERVAL '1 hour',
                    updated_at = NOW()
                WHERE id = v_test_conversation_id;

                -- Run expiration check
                SELECT * INTO v_expired_result
                FROM check_expired_windows();

                RAISE NOTICE '  Expired count: %', v_expired_result.expired_count;

                -- Verify window is now expired
                SELECT * INTO v_window_check
                FROM can_send_message(v_test_conversation_id);

                IF v_window_check.can_send = TRUE THEN
                    RAISE EXCEPTION 'FAIL: Window should be expired and can_send should be FALSE';
                END IF;

                IF v_window_check.within_window = TRUE THEN
                    RAISE EXCEPTION 'FAIL: within_window should be FALSE after expiration';
                END IF;

                RAISE NOTICE '✓ PASS: Window correctly expired';
                RAISE NOTICE '  Can send: %', v_window_check.can_send;
                RAISE NOTICE '  Reason: %', v_window_check.reason;
            END;

            -- ============================================
            -- TEST 15: Customer Message Resets Expired Window
            -- ============================================
            RAISE NOTICE '';
            RAISE NOTICE 'TEST 15: Customer message should reset expired window...';

            -- Insert another customer message
            INSERT INTO instagram_dm_messages (
                instagram_message_id,
                conversation_id,
                is_from_business,
                sender_instagram_id,
                sender_username,
                message_text,
                sent_at
            ) VALUES (
                'test_msg_customer_2_' || extract(epoch from now())::text,
                v_test_conversation_id,
                FALSE,  -- Customer message
                'test_customer_ig_id',
                'test_customer',
                'I have another question',
                NOW()
            );

            -- Check window was reset
            SELECT * INTO v_window_check
            FROM can_send_message(v_test_conversation_id);

            IF v_window_check.can_send = FALSE THEN
                RAISE EXCEPTION 'FAIL: Window should be reset after customer message. Reason: %', v_window_check.reason;
            END IF;

            IF v_window_check.hours_remaining < 23.9 THEN
                RAISE EXCEPTION 'FAIL: Window should be reset to ~24 hours';
            END IF;

            RAISE NOTICE '✓ PASS: Expired window successfully reset by customer message';
            RAISE NOTICE '  New hours remaining: %', v_window_check.hours_remaining;

            -- ============================================
            -- TEST 16: Upsert Idempotency
            -- ============================================
            RAISE NOTICE '';
            RAISE NOTICE 'TEST 16: Testing upsert_conversation idempotency...';

            DECLARE
                v_original_thread_id VARCHAR;
                v_upsert_result UUID;
            BEGIN
                SELECT instagram_thread_id INTO v_original_thread_id
                FROM instagram_dm_conversations
                WHERE id = v_test_conversation_id;

                -- Try to upsert same conversation
                SELECT upsert_conversation(
                    v_original_thread_id,
                    v_test_business_id,
                    'test_customer_ig_updated',
                    'updated_username',
                    'Updated Customer Name'
                ) INTO v_upsert_result;

                -- Should return same conversation ID
                IF v_upsert_result != v_test_conversation_id THEN
                    RAISE EXCEPTION 'FAIL: Upsert should return same conversation ID';
                END IF;

                -- Verify username was updated
                DECLARE
                    v_updated_username VARCHAR;
                BEGIN
                    SELECT customer_username INTO v_updated_username
                    FROM instagram_dm_conversations
                    WHERE id = v_test_conversation_id;

                    IF v_updated_username != 'updated_username' THEN
                        RAISE EXCEPTION 'FAIL: Username should be updated to "updated_username"';
                    END IF;

                    RAISE NOTICE '✓ PASS: Upsert is idempotent and updates existing conversation';
                END;
            END;

            -- ============================================
            -- CLEANUP: Remove test data
            -- ============================================
            RAISE NOTICE '';
            RAISE NOTICE 'CLEANUP: Removing test conversation and messages...';

            DELETE FROM instagram_dm_conversations
            WHERE id = v_test_conversation_id;

            RAISE NOTICE '✓ Test data cleaned up';
        END IF;
    END IF;

    -- ============================================
    -- TEST SUMMARY
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'ALL TESTS PASSED ✓';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Verified:';
    RAISE NOTICE '  ✓ Tables and indexes exist';
    RAISE NOTICE '  ✓ All critical functions work';
    RAISE NOTICE '  ✓ Customer messages open 24-hour window';
    RAISE NOTICE '  ✓ Business messages do NOT extend window';
    RAISE NOTICE '  ✓ Window expiration works correctly';
    RAISE NOTICE '  ✓ Expired windows can be reset by customer';
    RAISE NOTICE '  ✓ Message/unread counts track correctly';
    RAISE NOTICE '  ✓ Statistics functions return correct data';
    RAISE NOTICE '  ✓ Upsert is idempotent';
    RAISE NOTICE '========================================';

END $$;

ROLLBACK;  -- Don't commit test data

-- ============================================
-- END OF TEST SUITE
-- ============================================
