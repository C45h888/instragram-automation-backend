// substrates/parsing/workers/messages-worker.js
// Messages parsing worker: build rows → CK(DB_WRITE_REQUESTED).
//
// Owns: transforming raw message batches into normalized rows,
//        emitting through CK for governed DB write.
// Does NOT own: Supabase, governance policy, fetch, orchestration.

async function execute(rawData, accountId, intentId, extra = {}, governance) {
  const igUserId = rawData.igUserId || extra.igUserId;
  const pageId = rawData.pageId || extra.pageId || null;
  const pageToken = rawData.pageToken || extra.pageToken || null;

  // Messages for a single conversation
  if (rawData.rawMessages && rawData.rawMessages.length > 0) {
    const rows = rawData.rawMessages
      .filter(m => m && m.id)
      .map(m => {
        const fromBusiness = m.from?.id === igUserId || (pageId && m.from?.id === pageId);
        const att = m.attachments?.data?.[0] || null;
        const imgData = att?.image_data || null;
        const mediaUrl = imgData?.url || imgData?.animated_gif_url || att?.file_url || m.story?.link || null;
        let messageType = 'text';
        if (imgData?.render_as_sticker) messageType = 'media';
        else if (att) messageType = 'media';
        else if (m.story) messageType = 'story_reply';
        else if (m.shares?.data?.length) messageType = 'post_share';

        return {
          instagram_message_id: m.id,
          message_text: m.message || null,
          message_type: messageType,
          media_url: mediaUrl,
          media_type: imgData ? 'image' : att?.file_url ? 'file' : null,
          conversation_id: rawData.conversationId || 'direct',
          business_account_id: accountId,
          is_from_business: fromBusiness,
          recipient_instagram_id: m.to?.data?.[0]?.id || '',
          sender_username: m.from?.username || null,
          sent_at: m.created_time,
          send_status: fromBusiness ? 'sent' : 'delivered',
        };
      });

    if (rows.length === 0) return { count: 0 };

    if (governance) {
      governance.dispatch({
        type: 'DB_WRITE_REQUESTED',
        domain: 'messages',
        accountId, intentId,
        table: 'instagram_dm_messages',
        operation: 'batch_upsert_messages',
        rows,
        extra: { igUserId, pageId, pageToken, conversationId: rawData.conversationId },
      });
    }

    return { count: 0 };
  }

  // Conversations list
  if (rawData.rawConversations && rawData.rawConversations.length > 0) {
    const rows = [];
    for (const conv of rawData.rawConversations) {
      const customerMsg = conv.messages?.data?.find(m => m.from?.id !== igUserId && m.from?.id !== pageId);
      const participants = conv.participants?.data || [];
      const customerParticipant = participants.find(p => p.id !== igUserId && p.id !== pageId) || participants[0];
      if (!customerParticipant?.id) continue;

      rows.push({
        instagram_thread_id: conv.id,
        customer_instagram_id: customerParticipant.id,
        customer_username: customerParticipant.username || null,
        business_account_id: accountId,
        last_message_at: conv.updated_time || null,
        last_user_message_at: customerMsg ? new Date(customerMsg.created_time).toISOString() : null,
        message_count: conv.message_count || 0,
        conversation_status: 'active',
      });
    }

    if (rows.length === 0) return { count: 0 };

    if (governance) {
      governance.dispatch({
        type: 'DB_WRITE_REQUESTED',
        domain: 'messages',
        accountId, intentId,
        table: 'instagram_dm_conversations',
        operation: 'batch_upsert_conversations',
        rows,
        extra: { igUserId, pageId },
      });
    }

    return { count: 0 };
  }

  return { count: 0 };
}

module.exports = { execute };
