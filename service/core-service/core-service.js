"use strict";

const path = require("path");
const fs = require("fs");

const envFiles = [
  path.join(__dirname, ".env"),
  path.join(__dirname, "..", ".env"),
  path.join(__dirname, "..", "webhook-service", ".env"),
  path.join(__dirname, "..", "api-service", ".env"),
];

let loadedEnv = false;
for (const envFile of envFiles) {
  try {
    if (fs.existsSync(envFile)) {
      require("dotenv").config({ path: envFile });
      loadedEnv = true;
    }
  } catch {
    // ignore
  }
}

if (!loadedEnv) {
  require("dotenv").config();
}

const { Kafka, Partitioners, logLevel } = require("kafkajs");
const crypto = require("crypto");
const { patchKafkaJsNegativeTimeoutWarning } = require("./src/kafkajsPatch");
const { detectSpam } = require("./src/spamDetector");
const { AiClassifier } = require("./src/aiClassifier");
const { AiReplier } = require("./src/aiReplier");
const { UserStore } = require("./src/userStore");
const { StateStore } = require("./src/stateStore");
const sqlStore = require("./src/sqlStore");
const { closePool } = require("./db");

// NOTE: FacebookClient is intentionally NOT imported here.
// All Facebook API calls are delegated to api-service via Kafka topics:
//   reply_commands -> api-service handles replyToComment
//   moderation_commands -> api-service handles hide/deleteComment

patchKafkaJsNegativeTimeoutWarning();

const SERVICE_NAME = process.env.SERVICE_NAME || "core-service";

const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, "..", "data");
const CORE_FILE_LOG = String(process.env.CORE_FILE_LOG || "").toLowerCase() === "true";
const SQL_LOG_ENABLED = String(process.env.SQL_LOG_ENABLED || "true").toLowerCase() !== "false";

function appendJsonlSafe(filePath, obj) {
  try {
    const dir = path.dirname(filePath);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    fs.appendFileSync(filePath, `${JSON.stringify(obj)}\n`, "utf8");
  } catch {
    // ignore
  }
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

// ─── Kafka Configuration ──────────────────────────────────────────────────────
const DEFAULT_KAFKA_BROKER = "localhost:19092";
const KAFKA_BROKER = (process.env.KAFKA_BROKER || DEFAULT_KAFKA_BROKER).trim();
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const KAFKA_TOPIC_IN              = process.env.KAFKA_TOPIC_IN || process.env.KAFKA_TOPIC || "raw_events";
const KAFKA_TOPIC_RESULTS         = process.env.KAFKA_TOPIC_RESULTS || "moderation_results";
const KAFKA_TOPIC_MANUAL_REVIEW   = process.env.KAFKA_TOPIC_MANUAL_REVIEW || "manual_review";
const KAFKA_TOPIC_FAILED          = process.env.KAFKA_TOPIC_FAILED || "send_failed";

// Topics delegated to api-service.
const KAFKA_TOPIC_REPLY_COMMANDS      = process.env.KAFKA_TOPIC_REPLY_COMMANDS || "reply_commands";
const KAFKA_TOPIC_MODERATION_COMMANDS = process.env.KAFKA_TOPIC_MODERATION_COMMANDS || "moderation_commands";

const GROUP_ID             = process.env.KAFKA_GROUP_ID || "core-service-v1";
const FROM_BEGINNING       = String(process.env.KAFKA_FROM_BEGINNING || "").toLowerCase() === "true";

const PARTITIONS_CONCURRENTLY = Number(process.env.PARTITIONS_CONCURRENTLY || 1);
const MAX_MESSAGES_PER_BATCH  = Number(process.env.MAX_MESSAGES_PER_BATCH || 50);

// FB_SPAM_ACTION is still read here so core-service can tell api-service
// which moderation action to perform (hide vs delete). The actual API call is
// done by api-service.
const FB_SPAM_ACTION = String(process.env.FB_SPAM_ACTION || "hide").toLowerCase();
const RATE_LIMIT_ACTION = String(process.env.RATE_LIMIT_ACTION || "pending_review").toLowerCase();
const SPAM_BLACKLIST_REPEAT_THRESHOLD = Math.max(1, Number(process.env.SPAM_BLACKLIST_REPEAT_THRESHOLD || 3));

// ─── Kafka Client ─────────────────────────────────────────────────────────────
const kafka = new Kafka({
  clientId: SERVICE_NAME,
  brokers: KAFKA_BROKERS.length ? KAFKA_BROKERS : [KAFKA_BROKER],
  logLevel: logLevel.NOTHING,
});

const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner,
});
const consumer = kafka.consumer({
  groupId: GROUP_ID,
  allowAutoTopicCreation: true,
  maxInFlightRequests: 1,
});

// ─── Application Services ─────────────────────────────────────────────────────
const userStore = new UserStore({ dataDir: DATA_DIR });
const stateStore = new StateStore({ dataDir: DATA_DIR });

const ai = new AiClassifier({
  provider: process.env.AI_PROVIDER || "openai",
  openaiApiKey: process.env.OPENAI_API_KEY || "",
  geminiApiKey: process.env.GEMINI_API_KEY || "",
  claudeApiKey: process.env.CLAUDE_API_KEY || "",
  model: process.env.AI_MODEL || "gemini-2.5-flash",
  timeoutMs: Number(process.env.AI_TIMEOUT_MS || 8000),
});

const replyMode = String(process.env.AI_REPLY_MODE || "llm").toLowerCase();
const replier = new AiReplier({
  provider: process.env.AI_PROVIDER || "openai",
  openaiApiKey: process.env.OPENAI_API_KEY || "",
  geminiApiKey: process.env.GEMINI_API_KEY || "",
  claudeApiKey: process.env.CLAUDE_API_KEY || "",
  model: process.env.AI_REPLY_MODEL || process.env.AI_MODEL || "gemini-2.5-flash",
  timeoutMs: Number(process.env.AI_TIMEOUT_MS || 8000),
  shopName: process.env.SHOP_NAME || "Shop",
  maxReplyChars: Number(process.env.AI_REPLY_MAX_CHARS || 400),
  generalChat: String(process.env.AI_REPLY_GENERAL_CHAT || "").toLowerCase() === "true",
});

// ─── Helpers ──────────────────────────────────────────────────────────────────
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

// Legacy template replies — only used when AI_REPLY_MODE=template.
function buildAutoReply(aiResult) {
  switch (aiResult.intent) {
    case "price_inquiry":
      return "Dạ shop đã nhận được câu hỏi giá ạ. Bạn cho shop xin mẫu bạn muốn (màu/size) và số lượng; nếu có link/mã sản phẩm thì gửi giúp shop để shop báo giá chính xác nhé.";
    case "complaint_support":
      return "Dạ xin lỗi bạn vì trải nghiệm chưa tốt. Bạn inbox giúp shop mã đơn/sđt để shop kiểm tra và hỗ trợ ngay nhé.";
    case "praise_positive":
      return "Dạ cảm ơn bạn nhiều ạ!";
    case "product_inquiry":
      return "Dạ shop nhận được câu hỏi về sản phẩm ạ. Bạn cho shop xin size/màu bạn cần; nếu có link/mã sản phẩm thì gửi giúp shop để shop kiểm tra còn hàng và tư vấn nhanh nhất nhé.";
    case "shipping_inquiry":
      return "Dạ shop có hỗ trợ giao hàng ạ. Bạn cho shop xin khu vực (tỉnh/thành) để shop báo phí ship và thời gian dự kiến nhé.";
    default:
      return null;
  }
}

// ─── Decision Engine ──────────────────────────────────────────────────────────
function decide({ spam, aiResult, isBlacklisted, replyMode, commentText }) {
  const decision = {
    hideComment: false,
    enqueueManualReview: false,
    blacklistUser: false,
    sendAutoReply: false,
    autoReplyText: null,
    reason: [],
  };

  if (isBlacklisted) {
    decision.reason.push("blacklisted");
  }

  // Recidivism: already blacklisted + continues spamming => admin review.
  if (isBlacklisted && (spam.isSpamLight || spam.isMaliciousLink || spam.isBotLikely)) {
    decision.hideComment = true;
    decision.enqueueManualReview = true;
    decision.reason.push("blacklisted_reoffend");
    return decision;
  }

  if (spam.isMaliciousLink || spam.isBotLikely) {
    decision.hideComment = true;
    decision.enqueueManualReview = true;
    decision.reason.push("malicious_or_bot");
    return decision;
  }

  if (spam.isSpamLight) {
    decision.hideComment = true;
    decision.reason.push("spam_light");
  }

  const repeatForDecision = Number.isFinite(Number(spam.repeatSpamCount24hEffective))
    ? Number(spam.repeatSpamCount24hEffective)
    : Number(spam.repeatSpamCount24h) || 0;

  if (repeatForDecision >= SPAM_BLACKLIST_REPEAT_THRESHOLD) {
    decision.blacklistUser = true;
    decision.reason.push(`spam_repeat_${SPAM_BLACKLIST_REPEAT_THRESHOLD}_in_24h`);
  }

  if (!isBlacklisted && !decision.blacklistUser && !spam.isSpamLight && !spam.isMaliciousLink) {
    const t = String(commentText || "").trim();
    const tooShort = t.length <= 2;

    if (!tooShort) {
      if (replyMode === "llm") {
        decision.sendAutoReply = true;
      } else {
        decision.sendAutoReply = true;
        decision.autoReplyText = buildAutoReply(aiResult);
        if (!decision.autoReplyText) {
          decision.sendAutoReply = false;
        }
      }
    }
  }

  return decision;
}

// ─── Kafka Publish ────────────────────────────────────────────────────────────
async function publish(topic, key, valueObj) {
  await producer.send({
    topic,
    messages: [{ key: key || "", value: JSON.stringify(valueObj) }],
  });
}

// ─── Core Event Processor ─────────────────────────────────────────────────────
async function processEvent(ev) {
  const nowIso = new Date().toISOString();

  const initialEventId = ev && typeof ev === "object" ? ev.eventId : null;
  const safeEventId = initialEventId || `missing-${crypto.randomUUID ? crypto.randomUUID() : String(Date.now())}`;
  const safeEventType = ev && typeof ev === "object" ? ev.eventType : null;

  if (CORE_FILE_LOG) {
    appendJsonlSafe(path.join(DATA_DIR, "core_events.jsonl"), {
      at: nowIso,
      phase: "received",
      eventId: safeEventId,
      eventType: safeEventType,
      pageId: ev?.pageId || null,
      actorId: ev?.actorId || null,
      objectId: ev?.objectId || null,
      commentId: ev?.payload?.commentId || null,
    });
  }

  await stateStore.mark(safeEventId, "received", { receivedAt: nowIso, eventType: safeEventType });

  if (!ev || typeof ev !== "object") {
    await stateStore.mark(safeEventId, "failed", { at: nowIso, error: { message: "invalid_event" } });
    return { skipped: true, reason: "invalid_event" };
  }

  if (!ev.eventId) {
    ev.eventId = safeEventId;
  }

  if (stateStore.isFinal(ev.eventId)) {
    return { skipped: true, reason: "already_final" };
  }

  const isComment = ev.eventType === "comment.created" || ev.eventType === "comment.edited";
  if (!isComment) {
    await stateStore.mark(ev.eventId, "processed", { nonComment: true, at: nowIso });
    return { skipped: true, reason: "non_comment" };
  }

  const allowAutoReply = ev.eventType === "comment.created";

  const actorId = ev.actorId || "unknown";
  const text = (ev.payload && (ev.payload.message || ev.payload.text)) || "";
  const commentId = ev.payload?.commentId || null;
  const postId = ev.payload?.postId || ev.objectId || commentId;

  await trySql("comment received", () =>
    sqlStore.upsertComment({
      commentId,
      postId,
      senderId: actorId,
      message: text,
      status: "received",
    })
  );

  // Avoid reacting to the page's own comments (including our own auto-replies).
  const actorStr = String(actorId || "");
  const pageId = ev.pageId || null;
  const configuredPageId = process.env.PAGE_ID || null;
  const isFromPage =
    (pageId && actorStr && actorStr === String(pageId)) ||
    (configuredPageId && actorStr && actorStr === String(configuredPageId));

  if (isFromPage) {
    await stateStore.mark(ev.eventId, "processed", { at: nowIso, selfComment: true, pageId });

    if (CORE_FILE_LOG) {
      appendJsonlSafe(path.join(DATA_DIR, "core_events.jsonl"), {
        at: nowIso,
        phase: "skipped",
        reason: "self_comment",
        eventId: ev.eventId,
        pageId,
        actorId,
      });
    }

    return { skipped: true, reason: "self_comment" };
  }

  // ── Spam Detection ────────────────────────────────────────────────────────
  const isBlacklisted = userStore.isBlacklisted(actorId);

  const spam = detectSpam({
    text,
    actorId,
    now: Date.now(),
    userStore,
  });

  if (spam.isHighRate && RATE_LIMIT_ACTION !== "moderate") {
    const decision = {
      hideComment: false,
      enqueueManualReview: true,
      blacklistUser: false,
      sendAutoReply: false,
      autoReplyText: null,
      pendingReview: true,
      reason: ["rate_limit_pending_review", ...spam.reasons],
    };
    const aiResult = { intent: "pending_review", sentiment: "neutral", confidence: 0.0, provider: "skipped_rate_limit" };

    await trySql("comment pending_review", () =>
      sqlStore.upsertComment({
        commentId,
        postId,
        senderId: actorId,
        message: text,
        intent: aiResult.intent,
        sentiment: aiResult.sentiment,
        status: sqlStore.statusFromDecision(decision),
      })
    );

    await stateStore.mark(ev.eventId, "pending_review", {
      at: nowIso,
      actorId,
      pageId: ev.pageId || null,
      commentId,
      spam,
      ai: aiResult,
      decision,
    });

    await publish(KAFKA_TOPIC_MANUAL_REVIEW, actorId, {
      type: "manual_review",
      at: nowIso,
      reason: "rate_limit_pending_review",
      event: ev,
      spam,
      ai: aiResult,
      decision,
    });

    await publish(KAFKA_TOPIC_RESULTS, actorId, {
      type: "moderation_result",
      at: nowIso,
      eventId: ev.eventId,
      actorId,
      pageId: ev.pageId || null,
      objectId: ev.objectId || null,
      commentId,
      spam,
      ai: aiResult,
      decision,
      fbSpamAction: FB_SPAM_ACTION,
    });

    return { ok: true, spam, aiResult, decision };
  }

  const textForAi = spam?.normalized || text;

  // ── AI Classification ─────────────────────────────────────────────────────
  let aiResult = { intent: "other", sentiment: "neutral", confidence: 0.0, provider: "fallback" };
  if (!spam.isMaliciousLink && !spam.isBotLikely) {
    aiResult = await ai.classify({ text: textForAi });
  }

  // ── Decision Making ───────────────────────────────────────────────────────
  const decision = decide({ spam, aiResult, isBlacklisted, replyMode, commentText: textForAi });

  if (!allowAutoReply && decision.sendAutoReply) {
    decision.sendAutoReply = false;
    decision.autoReplyText = null;
    decision.reason.push("skip_auto_reply_on_edit");
  }

  if (decision.sendAutoReply && commentId && stateStore.hasRepliedComment(commentId)) {
    decision.sendAutoReply = false;
    decision.autoReplyText = null;
    decision.reason.push("dedupe_already_replied");
  }

  // ── LLM Reply Generation ──────────────────────────────────────────────────
  if (decision.sendAutoReply && replyMode === "llm") {
    const reply = await replier.generateReply({
      commentText: textForAi,
      intent: aiResult.intent,
      sentiment: aiResult.sentiment,
    });

    if (reply && reply.shouldReply && reply.reply) {
      decision.autoReplyText = reply.reply;
      decision.reason.push(`ai_reply_${reply.provider || "llm"}`);
    } else {
      decision.sendAutoReply = false;
      decision.autoReplyText = null;
      decision.reason.push("ai_reply_skip");
    }
  }

  await trySql("comment processed", () =>
    sqlStore.upsertComment({
      commentId,
      postId,
      senderId: actorId,
      message: text,
      intent: aiResult.intent,
      sentiment: aiResult.sentiment,
      status: sqlStore.statusFromDecision(decision),
      replyText: decision.autoReplyText,
    })
  );

  // ── Apply Decisions (via Kafka — no direct FB calls) ──────────────────────

  // 1. Blacklist user locally
  if (decision.blacklistUser) {
    const configuredPageIdForBlacklist = process.env.PAGE_ID || null;
    const actorStrForBlacklist = String(actorId || "");
    const isPageActor =
      (ev.pageId && actorStrForBlacklist === String(ev.pageId)) ||
      (configuredPageIdForBlacklist && actorStrForBlacklist === String(configuredPageIdForBlacklist));

    if (!actorStrForBlacklist || actorStrForBlacklist === "unknown") {
      decision.reason.push("skip_blacklist_unknown_actor");
    } else if (isPageActor) {
      decision.reason.push("skip_blacklist_page_actor");
    } else {
      userStore.blacklist(actorId, { at: nowIso, reason: decision.reason });
    }
  }

  // 2. Publish moderation_commands (hide/delete) -> api-service executes via FB API
  if (decision.hideComment && commentId) {
    const moderationType = FB_SPAM_ACTION === "delete" ? "delete_comment" : "hide_comment";
    await publish(KAFKA_TOPIC_MODERATION_COMMANDS, actorId, {
      type: moderationType,
      commentId,
      actorId,
      eventId: ev.eventId,
      pageId: ev.pageId || null,
      at: nowIso,
      reason: decision.reason,
      retryCount: 0,
    });
  }

  // 3. Publish to manual_review if needed
  if (decision.enqueueManualReview) {
    await publish(KAFKA_TOPIC_MANUAL_REVIEW, actorId, {
      type: "manual_review",
      at: nowIso,
      event: ev,
      spam,
      ai: aiResult,
      decision,
    });
  }

  // 4. Publish reply_commands -> api-service executes via FB API
  if (decision.sendAutoReply && commentId && decision.autoReplyText) {
    stateStore.markCommentReplied(commentId, { at: nowIso, eventId: ev.eventId });
    await stateStore.mark(ev.eventId, "reply_enqueued", { at: nowIso });

    await publish(KAFKA_TOPIC_REPLY_COMMANDS, actorId, {
      type: "reply_comment",
      commentId,
      message: decision.autoReplyText,
      actorId,
      eventId: ev.eventId,
      pageId: ev.pageId || null,
      at: nowIso,
      retryCount: 0,
    });
  }

  // ── State + Audit Log ─────────────────────────────────────────────────────
  await stateStore.mark(ev.eventId, "processed", {
    at: nowIso,
    actorId,
    pageId: ev.pageId || null,
    commentId,
    spam,
    ai: aiResult,
    decision,
  });

  if (CORE_FILE_LOG) {
    appendJsonlSafe(path.join(DATA_DIR, "core_events.jsonl"), {
      at: nowIso,
      phase: "processed",
      eventId: ev.eventId,
      eventType: ev.eventType,
      pageId: ev.pageId || null,
      actorId,
      commentId,
      decision: {
        hideComment: !!decision.hideComment,
        sendAutoReply: !!decision.sendAutoReply,
        blacklistUser: !!decision.blacklistUser,
        enqueueManualReview: !!decision.enqueueManualReview,
        reason: decision.reason,
      },
    });
  }

  // Publish moderation_results for monitoring/dashboards
  await publish(KAFKA_TOPIC_RESULTS, actorId, {
    type: "moderation_result",
    at: nowIso,
    eventId: ev.eventId,
    actorId,
    pageId: ev.pageId || null,
    objectId: ev.objectId || null,
    commentId,
    spam,
    ai: aiResult,
    decision,
    fbSpamAction: FB_SPAM_ACTION,
  });

  return { ok: true, spam, aiResult, decision };
}

// ─── Consumer Runner ──────────────────────────────────────────────────────────
async function start() {
  console.log(`[${SERVICE_NAME}] starting...`);
  console.log(
    `[${SERVICE_NAME}] brokers=${(KAFKA_BROKERS.length ? KAFKA_BROKERS : [KAFKA_BROKER]).join(",")} ` +
    `topicIn=${KAFKA_TOPIC_IN} groupId=${GROUP_ID}`
  );
  console.log(
    `[${SERVICE_NAME}] aiReplyMode=${replyMode} aiProvider=${ai.provider} aiModel=${ai.model} ` +
    `replyModel=${process.env.AI_REPLY_MODEL || "(inherit AI_MODEL)"} ` +
    `replierEnabled=${replier.isEnabled()} ` +
    `generalChat=${String(process.env.AI_REPLY_GENERAL_CHAT || "").toLowerCase() === "true"}`
  );
  console.log(
    `[${SERVICE_NAME}] publishing to: reply_commands=${KAFKA_TOPIC_REPLY_COMMANDS} ` +
    `moderation_commands=${KAFKA_TOPIC_MODERATION_COMMANDS} ` +
    `results=${KAFKA_TOPIC_RESULTS} failed=${KAFKA_TOPIC_FAILED}`
  );

  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: KAFKA_TOPIC_IN, fromBeginning: FROM_BEGINNING });

  await consumer.run({
    partitionsConsumedConcurrently: PARTITIONS_CONCURRENTLY,
    eachBatchAutoResolve: false,
    eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary, isRunning, isStale }) => {
      for (const message of batch.messages.slice(0, MAX_MESSAGES_PER_BATCH)) {
        if (!isRunning() || isStale()) break;

        const ev = safeJsonParse(message.value);
        const key = message.key ? message.key.toString("utf8") : "";

        try {
          const out = await processEvent(ev);
          resolveOffset(message.offset);
          await commitOffsetsIfNecessary();
          await heartbeat();

          if (out && out.ok) {
            if (String(process.env.LOG_LEVEL || "").toLowerCase() === "debug") {
              console.log(`[${SERVICE_NAME}] processed eventId=${ev?.eventId || ""} key=${key}`);
            }
          }
        } catch (err) {
          const nowIso = new Date().toISOString();
          const eventId = ev?.eventId || "unknown";
          const errStatus = typeof err?.status === "number" ? err.status : null;
          const errData = err?.data ?? null;

          const stringifyLimited = (obj, limit) => {
            try {
              let s = JSON.stringify(obj);
              s = s.replace(/Malformed access token\s+[^\"\s}]+/g, "Malformed access token <redacted>");
              if (typeof s === "string" && s.length > limit) return `${s.slice(0, limit)}...`;
              return s;
            } catch {
              return null;
            }
          };

          const errorInfo = {
            message: err?.message ? String(err.message) : String(err),
            name: err?.name || null,
            status: errStatus,
            data: errData ? stringifyLimited(errData, 4000) : null,
            stack: String(process.env.LOG_STACK || "").toLowerCase() === "true" ? err?.stack || null : null,
          };

          await stateStore.mark(eventId, "failed", { at: nowIso, error: errorInfo });
          await publish(KAFKA_TOPIC_FAILED, key, {
            type: "send_failed",
            at: nowIso,
            eventId,
            error: errorInfo,
            event: ev,
          });

          // Resolve + commit so we don't get stuck on a poison message.
          resolveOffset(message.offset);
          await commitOffsetsIfNecessary();
          await heartbeat();

          console.error(
            `[${SERVICE_NAME}] failed eventId=${eventId} offset=${message.offset}: ${errorInfo.message}`,
            errorInfo.status || errorInfo.data ? { status: errorInfo.status, data: errorInfo.data } : undefined
          );
        }
      }
    },
  });
}

// ─── Graceful Shutdown ────────────────────────────────────────────────────────
function shutdown(signal) {
  console.log(`[${SERVICE_NAME}] shutting down (${signal})...`);
  Promise.resolve()
    .then(() => userStore.flush())
    .then(() => stateStore.flush())
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
