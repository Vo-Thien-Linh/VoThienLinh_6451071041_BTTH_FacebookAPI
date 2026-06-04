"use strict";

/**
 * api-service - Backend API worker (port 3000 in the architecture diagram)
 *
 * Responsibilities:
 *   - Consume reply_commands, moderation_commands, and send_retry.
 *   - Check/save idempotency keys before calling Facebook.
 *   - Send replies or moderation actions to Facebook Graph API.
 *   - Publish send_failed when a send fails. retry-service owns backoff/retry.
 */

const path = require("path");
const fs = require("fs");

const envCandidates = [
  path.join(__dirname, ".env"),
  path.join(__dirname, "..", ".env"),
  path.join(__dirname, "..", "webhook-service", ".env"),
];

const envPath = envCandidates.find((p) => {
  try {
    return fs.existsSync(p);
  } catch {
    return false;
  }
});

if (envPath) {
  require("dotenv").config({ path: envPath });
} else {
  require("dotenv").config();
}

const { Kafka, Partitioners, logLevel } = require("kafkajs");
const { FacebookClient } = require("./src/facebookClient");
const { patchKafkaJsNegativeTimeoutWarning } = require("./src/kafkajsPatch");
const { CircuitBreaker } = require("./src/circuitBreaker");
const sqlStore = require("./src/sqlStore");
const { closePool } = require("./db");

patchKafkaJsNegativeTimeoutWarning();

const SERVICE_NAME = process.env.API_SERVICE_NAME || "api-service";
const PORT = Number(process.env.API_PORT || 3000);

const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, "..", "data");
const IDEMPOTENCY_FILE = path.join(DATA_DIR, "api_idempotency.json");
const API_FILE_LOG = String(process.env.API_FILE_LOG || "").toLowerCase() === "true";
const SQL_LOG_ENABLED = String(process.env.SQL_LOG_ENABLED || "true").toLowerCase() !== "false";

const DEFAULT_KAFKA_BROKER = "localhost:19092";
const KAFKA_BROKER = (process.env.KAFKA_BROKER || DEFAULT_KAFKA_BROKER).trim();
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const KAFKA_TOPIC_REPLY_COMMANDS = process.env.KAFKA_TOPIC_REPLY_COMMANDS || "reply_commands";
const KAFKA_TOPIC_MODERATION_COMMANDS = process.env.KAFKA_TOPIC_MODERATION_COMMANDS || "moderation_commands";
const KAFKA_TOPIC_SEND_RETRY = process.env.KAFKA_TOPIC_SEND_RETRY || "send_retry";
const KAFKA_TOPIC_FAILED = process.env.KAFKA_TOPIC_FAILED || "send_failed";

const GROUP_ID = process.env.API_KAFKA_GROUP_ID || "api-service-v1";
const FROM_BEGINNING = String(process.env.KAFKA_FROM_BEGINNING || "").toLowerCase() === "true";
const PARTITIONS_CONCURRENTLY = Number(process.env.PARTITIONS_CONCURRENTLY || 1);
const MAX_MESSAGES_PER_BATCH = Number(process.env.MAX_MESSAGES_PER_BATCH || 50);
const FB_CIRCUIT_FAILURE_THRESHOLD = Number(process.env.FB_CIRCUIT_FAILURE_THRESHOLD || 5);
const FB_CIRCUIT_RESET_TIMEOUT_MS = Number(process.env.FB_CIRCUIT_RESET_TIMEOUT_MS || 30_000);

const fb = new FacebookClient({
  accessToken: process.env.ACCESS_TOKEN || "",
  graphVersion: process.env.FB_GRAPH_VERSION || "v23.0",
  timeoutMs: Number(process.env.FB_TIMEOUT_MS || 8000),
});

const fbCircuitBreaker = new CircuitBreaker({
  name: "facebook-api",
  failureThreshold: FB_CIRCUIT_FAILURE_THRESHOLD,
  resetTimeoutMs: FB_CIRCUIT_RESET_TIMEOUT_MS,
});

const kafka = new Kafka({
  clientId: SERVICE_NAME,
  brokers: KAFKA_BROKERS.length ? KAFKA_BROKERS : [KAFKA_BROKER],
  logLevel: logLevel.NOTHING,
});

const producer = kafka.producer({ createPartitioner: Partitioners.LegacyPartitioner });
const consumer = kafka.consumer({
  groupId: GROUP_ID,
  allowAutoTopicCreation: true,
  maxInFlightRequests: 1,
});

const idempotencyMap = new Map();
let idempotencyDirty = false;

function loadIdempotency() {
  try {
    if (!fs.existsSync(IDEMPOTENCY_FILE)) return;
    const raw = fs.readFileSync(IDEMPOTENCY_FILE, "utf8");
    const obj = JSON.parse(raw);
    for (const [k, v] of Object.entries(obj)) idempotencyMap.set(k, v);
    console.log(`[${SERVICE_NAME}] idempotency loaded: ${idempotencyMap.size} keys`);
  } catch {
    // Start fresh if the file is corrupt.
  }
}

function flushIdempotency() {
  if (!idempotencyDirty) return;
  try {
    const dir = path.dirname(IDEMPOTENCY_FILE);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(IDEMPOTENCY_FILE, JSON.stringify(Object.fromEntries(idempotencyMap), null, 2), "utf8");
    idempotencyDirty = false;
  } catch (e) {
    console.error(`[${SERVICE_NAME}] idempotency flush error`, e.message);
  }
}

function buildIdempotencyKey(cmd) {
  return `${cmd.type || ""}:${cmd.commentId || ""}:${cmd.eventId || ""}`;
}

function markProcessed(key, meta) {
  idempotencyMap.set(key, { ...meta, at: new Date().toISOString() });
  idempotencyDirty = true;
}

async function trySql(label, fn) {
  if (!SQL_LOG_ENABLED) return null;
  try {
    return await fn();
  } catch (err) {
    console.error(`[${SERVICE_NAME}] SQL ${label} error: ${err?.message || err}`);
    return null;
  }
}

setInterval(flushIdempotency, 10_000).unref();

function safeJsonParse(value) {
  if (!value) return null;
  const s = Buffer.isBuffer(value) ? value.toString("utf8") : String(value);
  try {
    return JSON.parse(s);
  } catch {
    const first = s.indexOf("{");
    const last = s.lastIndexOf("}");
    if (first >= 0 && last > first) {
      try {
        return JSON.parse(s.slice(first, last + 1));
      } catch {
        return null;
      }
    }
    return null;
  }
}

function appendJsonlSafe(filePath, obj) {
  try {
    const dir = path.dirname(filePath);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    fs.appendFileSync(filePath, `${JSON.stringify(obj)}\n`, "utf8");
  } catch {
    // ignore
  }
}

async function publish(topic, key, valueObj) {
  await producer.send({
    topic,
    messages: [{ key: key || "", value: JSON.stringify(valueObj) }],
  });
}

function errorInfoFrom(err) {
  return {
    message: err?.message ? String(err.message) : String(err),
    name: err?.name || null,
    status: typeof err?.status === "number" ? err.status : null,
    retryable: typeof err?.retryable === "boolean" ? err.retryable : isRetryableError(err),
    circuitOpen: !!err?.circuitOpen,
    data: err?.data ? JSON.stringify(err.data).slice(0, 2000) : null,
  };
}

function isRetryableError(err) {
  if (err?.circuitOpen) return true;
  const status = Number(err?.status);
  if (!Number.isFinite(status)) return true;
  return status === 408 || status === 429 || status >= 500;
}

async function callFacebook(fn) {
  return fbCircuitBreaker.execute(fn);
}

async function processCommand(cmd) {
  if (!cmd || typeof cmd !== "object") {
    return { skipped: true, reason: "invalid_command" };
  }

  const { type, commentId, message, eventId, actorId } = cmd;
  const nowIso = new Date().toISOString();

  if (!type) return { skipped: true, reason: "missing_type" };

  const ikey = buildIdempotencyKey(cmd);
  if (idempotencyMap.has(ikey)) {
    console.log(`[${SERVICE_NAME}] idempotency skip: ${ikey}`);
    return { skipped: true, reason: "duplicate_idempotency" };
  }

  const existingSqlKey = await trySql("idempotency check", () => sqlStore.getIdempotency(ikey));
  if (existingSqlKey && existingSqlKey.status === "processed") {
    markProcessed(ikey, { type, commentId, eventId, actorId, source: "sql" });
    console.log(`[${SERVICE_NAME}] SQL idempotency skip: ${ikey}`);
    return { skipped: true, reason: "duplicate_sql_idempotency" };
  }

  if (!fb.isConfigured()) {
    throw new Error("Facebook not configured: ACCESS_TOKEN is missing");
  }

  await trySql("idempotency processing", () =>
    sqlStore.upsertIdempotency({
      commandId: ikey,
      status: "processing",
      retryCount: Number(cmd.retryCount || 0),
      lastError: cmd.lastError || null,
    })
  );

  let fbResult = null;
  switch (type) {
    case "reply_comment":
      if (!commentId || !message) return { skipped: true, reason: "missing_commentId_or_message" };
      fbResult = await callFacebook(() => fb.replyToComment(commentId, message));
      break;
    case "hide_comment":
      if (!commentId) return { skipped: true, reason: "missing_commentId" };
      fbResult = await callFacebook(() => fb.hideComment(commentId));
      break;
    case "delete_comment":
      if (!commentId) return { skipped: true, reason: "missing_commentId" };
      fbResult = await callFacebook(() => fb.deleteComment(commentId));
      break;
    default:
      return { skipped: true, reason: `unknown_type:${type}` };
  }

  markProcessed(ikey, { type, commentId, eventId, actorId });

  await trySql("idempotency processed", () =>
    sqlStore.upsertIdempotency({
      commandId: ikey,
      status: "processed",
      retryCount: Number(cmd.retryCount || 0),
      fbResult,
    })
  );

  if (type === "reply_comment") {
    await trySql("comment reply update", () =>
      sqlStore.markCommentReply({
        commentId,
        commandId: ikey,
        replyText: message,
      })
    );
  }

  if (API_FILE_LOG) {
    appendJsonlSafe(path.join(DATA_DIR, "api_events.jsonl"), {
      at: nowIso,
      phase: "executed",
      type,
      commentId,
      eventId,
      actorId,
      fbResult,
    });
  }

  console.log(`[${SERVICE_NAME}] executed type=${type} commentId=${commentId || ""} eventId=${eventId || ""}`);
  return { ok: true, type, commentId, eventId, fbResult };
}

async function start() {
  loadIdempotency();

  console.log(`[${SERVICE_NAME}] starting on port ${PORT}...`);
  console.log(`[${SERVICE_NAME}] brokers=${(KAFKA_BROKERS.length ? KAFKA_BROKERS : [KAFKA_BROKER]).join(",")} groupId=${GROUP_ID}`);
  console.log(`[${SERVICE_NAME}] consuming: ${KAFKA_TOPIC_REPLY_COMMANDS}, ${KAFKA_TOPIC_MODERATION_COMMANDS}, ${KAFKA_TOPIC_SEND_RETRY}`);
  console.log(`[${SERVICE_NAME}] publishing failures to: ${KAFKA_TOPIC_FAILED}`);
  console.log(`[${SERVICE_NAME}] facebook circuit threshold=${FB_CIRCUIT_FAILURE_THRESHOLD} resetMs=${FB_CIRCUIT_RESET_TIMEOUT_MS}`);

  const fbDiag = fb.tokenDiagnostics();
  if (fb.isConfigured()) {
    console.log(`[${SERVICE_NAME}] FB token=${fbDiag.masked} len=${fbDiag.length}`);
    if (!fbDiag.looksLikeToken || fbDiag.looksConcatenated) {
      console.warn(`[${SERVICE_NAME}] ACCESS_TOKEN looks malformed. Re-copy a single Page Access Token.`);
    }
  } else {
    console.warn(`[${SERVICE_NAME}] ACCESS_TOKEN not set - FB actions will fail and go to send_failed.`);
  }

  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({
    topics: [KAFKA_TOPIC_REPLY_COMMANDS, KAFKA_TOPIC_MODERATION_COMMANDS, KAFKA_TOPIC_SEND_RETRY],
    fromBeginning: FROM_BEGINNING,
  });

  await consumer.run({
    partitionsConsumedConcurrently: PARTITIONS_CONCURRENTLY,
    eachBatchAutoResolve: false,
    eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary, isRunning, isStale }) => {
      for (const message of batch.messages.slice(0, MAX_MESSAGES_PER_BATCH)) {
        if (!isRunning() || isStale()) break;

        const cmd = safeJsonParse(message.value);
        const key = message.key ? message.key.toString("utf8") : "";

        try {
          await processCommand(cmd);
        } catch (err) {
          const nowIso = new Date().toISOString();
          const error = errorInfoFrom(err);
          const retryCount = Number(cmd?.retryCount ?? 0);
          const commandId = cmd && typeof cmd === "object" ? buildIdempotencyKey(cmd) : null;

          if (commandId) {
            await trySql("idempotency failed", () =>
              sqlStore.upsertIdempotency({
                commandId,
                status: "failed",
                retryCount,
                lastError: error,
              })
            );
          }

          if (API_FILE_LOG) {
            appendJsonlSafe(path.join(DATA_DIR, "api_events.jsonl"), {
              at: nowIso,
              phase: "failed",
              type: cmd?.type || null,
              commentId: cmd?.commentId || null,
              eventId: cmd?.eventId || null,
              retryCount,
              error,
            });
          }

          await publish(KAFKA_TOPIC_FAILED, key, {
            type: "send_failed",
            at: nowIso,
            eventId: cmd?.eventId || "unknown",
            retryCount,
            command: cmd,
            error,
          });

          console.error(`[${SERVICE_NAME}] send failed eventId=${cmd?.eventId || "unknown"} retry=${retryCount}: ${error.message}`);
        }

        resolveOffset(message.offset);
        await commitOffsetsIfNecessary();
        await heartbeat();
      }
    },
  });
}

function shutdown(signal) {
  console.log(`[${SERVICE_NAME}] shutting down (${signal})...`);
  flushIdempotency();
  Promise.resolve()
    .then(() => consumer.disconnect())
    .then(() => producer.disconnect())
    .then(() => closePool())
    .then(() => process.exit(0))
    .catch((e) => {
      console.error(`[${SERVICE_NAME}] shutdown error`, e);
      process.exit(1);
    });
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

start().catch((e) => {
  console.error(`[${SERVICE_NAME}] startup error`, e);
  process.exit(1);
});

const { getPool, sql } = require("./db");

// Check idempotency key
async function checkIdempotency(commandId) {
  const pool = await getPool();
  const result = await pool.request()
    .input("command_id", sql.VarChar(100), commandId)
    .query(`
      SELECT status, retry_count
      FROM idempotency_keys
      WHERE command_id = @command_id
    `);
  return result.recordset[0] || null;
}

// Lưu idempotency key
async function saveIdempotency(commandId, status) {
  const pool = await getPool();
  await pool.request()
    .input("command_id", sql.VarChar(100), commandId)
    .input("status",     sql.VarChar(20),  status)
    .query(`
      MERGE idempotency_keys AS target
      USING (SELECT @command_id AS command_id) AS source
        ON target.command_id = source.command_id
      WHEN MATCHED THEN
        UPDATE SET status = @status, retry_count = retry_count + 1
      WHEN NOT MATCHED THEN
        INSERT (command_id, status) VALUES (@command_id, @status);
    `);
}

// Lưu comment
async function saveComment({ commentId, postId, senderId, message, intent, sentiment }) {
  const pool = await getPool();
  await pool.request()
    .input("comment_id", sql.VarChar(100), commentId)
    .input("post_id",    sql.VarChar(100), postId)
    .input("sender_id",  sql.VarChar(100), senderId)
    .input("message",    sql.NVarChar(sql.MAX), message)
    .input("intent",     sql.VarChar(50),  intent)
    .input("sentiment",  sql.VarChar(20),  sentiment)
    .query(`
      IF NOT EXISTS (SELECT 1 FROM comments WHERE comment_id = @comment_id)
        INSERT INTO comments (comment_id, post_id, sender_id, message, intent, sentiment)
        VALUES (@comment_id, @post_id, @sender_id, @message, @intent, @sentiment)
    `);
}
