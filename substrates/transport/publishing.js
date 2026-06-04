// substrates/transport/publishing.js — MIGRATED
// All content-publishing transport logic moved to:
//   substrates/publishing/content/transport.js   (publishPost, publishStory, repostUgc, createMediaContainer, pollAndPublish)
//   substrates/publishing/engagement/transport.js (replyComment, replyDm, sendDm)
//
// All substrate execute logic moved to:
//   substrates/publishing/content/index.js        (executePost, executeStory, executeRepostUgc)
//   substrates/publishing/engagement/index.js     (executeCommentReply, executeDmReply, executeDmSend)
//
// Orchestration wired through:
//   control-plane/orchestration/emission-orchestrator.js
//     → EXECUTE_CONTENT  → bounded content substrate → IG Graph API
//     → EXECUTE_ENGAGEMENT → bounded engagement substrate → IG Graph API
//
// FSM flow:
//   publishing-fsm (IDLE → FETCHING → EXECUTING → IDLE)
//   Retry sovereignty: engagement-fsm via CK membrane