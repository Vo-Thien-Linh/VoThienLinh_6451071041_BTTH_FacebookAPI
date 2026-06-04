"use strict";

const { getPool, sql } = require("../db");

function stringifyError(error) {
  if (!error) return null;
  if (typeof error === "string") return error;
  try {
    return JSON.stringify(error).slice(0, 4000);
  } catch {
    return String(error).slice(0, 4000);
  }
}

function fbMessageIdFrom(result) {
  if (!result || typeof result !== "object") return null;
  return result.id || result.comment_id || result.post_id || null;
}

async function getIdempotency(commandId) {
  const pool = await getPool();
  const result = await pool.request()
    .input("command_id", sql.VarChar(100), commandId)
    .query(`
      SELECT command_id, status, retry_count, last_error, fb_message_id
      FROM idempotency_keys
      WHERE command_id = @command_id
    `);

  return result.recordset[0] || null;
}

async function upsertIdempotency({ commandId, status, retryCount = 0, lastError = null, fbResult = null }) {
  const pool = await getPool();
  await pool.request()
    .input("command_id", sql.VarChar(100), commandId)
    .input("status", sql.VarChar(20), status)
    .input("retry_count", sql.Int, Number(retryCount) || 0)
    .input("last_error", sql.NVarChar(sql.MAX), stringifyError(lastError))
    .input("fb_message_id", sql.VarChar(200), fbMessageIdFrom(fbResult))
    .query(`
      MERGE idempotency_keys AS target
      USING (SELECT @command_id AS command_id) AS source
        ON target.command_id = source.command_id
      WHEN MATCHED THEN
        UPDATE SET
          status = @status,
          retry_count = CASE
            WHEN @retry_count > target.retry_count THEN @retry_count
            WHEN @status = 'failed' THEN target.retry_count + 1
            ELSE target.retry_count
          END,
          last_error = @last_error,
          fb_message_id = COALESCE(@fb_message_id, target.fb_message_id),
          processed_at = GETDATE()
      WHEN NOT MATCHED THEN
        INSERT (command_id, status, retry_count, last_error, fb_message_id)
        VALUES (@command_id, @status, @retry_count, @last_error, @fb_message_id);
    `);
}

async function markCommentReply({ commentId, commandId, replyText }) {
  if (!commentId) return;
  const pool = await getPool();
  await pool.request()
    .input("comment_id", sql.VarChar(100), commentId)
    .input("command_id", sql.VarChar(100), commandId || null)
    .input("reply_text", sql.NVarChar(sql.MAX), replyText || null)
    .query(`
      UPDATE comments
      SET
        status = 'replied',
        reply_text = COALESCE(@reply_text, reply_text),
        command_id = COALESCE(@command_id, command_id),
        replied_at = GETDATE()
      WHERE comment_id = @comment_id;
    `);
}

module.exports = {
  getIdempotency,
  upsertIdempotency,
  markCommentReply,
};
