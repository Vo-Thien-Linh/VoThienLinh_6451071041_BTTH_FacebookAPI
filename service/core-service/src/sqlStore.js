"use strict";

const { getPool, sql } = require("../db");

function statusFromDecision(decision) {
  if (!decision) return "received";
  if (decision.pendingReview) return "pending_review";
  if (decision.sendAutoReply) return "reply_enqueued";
  if (decision.hideComment) return "moderated";
  if (decision.enqueueManualReview) return "manual_review";
  return "processed";
}

async function upsertComment({ commentId, postId, senderId, message, intent, sentiment, status, replyText }) {
  if (!commentId || !postId) return;

  const pool = await getPool();
  await pool.request()
    .input("comment_id", sql.VarChar(100), commentId)
    .input("post_id", sql.VarChar(100), postId)
    .input("sender_id", sql.VarChar(100), senderId || null)
    .input("message", sql.NVarChar(sql.MAX), message || null)
    .input("intent", sql.VarChar(50), intent || null)
    .input("sentiment", sql.VarChar(20), sentiment || null)
    .input("status", sql.VarChar(20), status || "received")
    .input("reply_text", sql.NVarChar(sql.MAX), replyText || null)
    .query(`
      MERGE comments AS target
      USING (SELECT @comment_id AS comment_id) AS source
        ON target.comment_id = source.comment_id
      WHEN MATCHED THEN
        UPDATE SET
          post_id = COALESCE(@post_id, target.post_id),
          sender_id = COALESCE(@sender_id, target.sender_id),
          message = COALESCE(@message, target.message),
          intent = COALESCE(@intent, target.intent),
          sentiment = COALESCE(@sentiment, target.sentiment),
          status = @status,
          reply_text = COALESCE(@reply_text, target.reply_text)
      WHEN NOT MATCHED THEN
        INSERT (comment_id, post_id, sender_id, message, intent, sentiment, status, reply_text)
        VALUES (@comment_id, @post_id, @sender_id, @message, @intent, @sentiment, @status, @reply_text);
    `);
}

module.exports = {
  statusFromDecision,
  upsertComment,
};
