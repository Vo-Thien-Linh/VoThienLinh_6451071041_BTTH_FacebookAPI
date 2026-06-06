"use strict";

/**
 * retry-service
 *
 * Responsibilities:
 *   - Consume send_failed messages from api-service/core-service.
 *   - If retryCount < MAX_RETRY_COUNT, wait with exponential backoff and publish send_retry.
 *   - If retryCount >= MAX_RETRY_COUNT, publish dead_letter for monitoring/alerting.
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
const { patchKafkaJsNegativeTimeoutWarning } = require("./src/kafkajsPatch");

patchKafkaJsNegativeTimeoutWarning();

const SERVICE_NAME = process.env.RETRY_SERVICE_NAME || "retry-service";
const PORT = Number(process.env.RETRY_PORT || 3003);

const DEFAULT_KAFKA_BROKER = "localhost:19092";
const KAFKA_BROKER = (process.env.KAFKA_BROKER || DEFAULT_KAFKA_BROKER).trim();
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const KAFKA_TOPIC_FAILED = process.env.KAFKA_TOPIC_FAILED || "send_failed";
const KAFKA_TOPIC_SEND_RETRY = process.env.KAFKA_TOPIC_SEND_RETRY || "send_retry";
const KAFKA_TOPIC_DEAD_LETTER = process.env.KAFKA_TOPIC_DEAD_LETTER || "dead_letter";

const GROUP_ID = process.env.RETRY_KAFKA_GROUP_ID || "retry-service-v1";
const FROM_BEGINNING = String(process.env.KAFKA_FROM_BEGINNING || "").toLowerCase() === "true";
const PARTITIONS_CONCURRENTLY = Number(process.env.PARTITIONS_CONCURRENTLY || 1);
const MAX_MESSAGES_PER_BATCH = Number(process.env.MAX_MESSAGES_PER_BATCH || 50);
const MAX_RETRY_COUNT = Number(process.env.MAX_RETRY_COUNT || 3);
const RETRY_NON_RETRYABLE_ERRORS = String(process.env.RETRY_NON_RETRYABLE_ERRORS || "").toLowerCase() === "true";

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

const pendingTimers = new Set();

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

async function publish(topic, key, valueObj) {
  await producer.send({
    topic,
    messages: [{ key: key || "", value: JSON.stringify(valueObj) }],
  });
}

function retryDelayMs(retryCount) {
  const base = Number(process.env.SEND_RETRY_BASE_DELAY_MS || 1000);
  const cappedBase = Number.isFinite(base) ? Math.max(50, base) : 1000;
  const exp = Math.max(0, Number(retryCount) || 0);
  const max = Number(process.env.SEND_RETRY_MAX_DELAY_MS || 30_000);
  const cappedMax = Number.isFinite(max) ? Math.max(250, max) : 30_000;
  return Math.min(cappedMax, cappedBase * Math.pow(2, exp));
}

function isRetryableFailure(failure) {
  if (RETRY_NON_RETRYABLE_ERRORS) return true;
  if (failure?.error?.retryable === true) return true;
  if (failure?.error?.retryable === false) return false;
  if (failure?.error?.circuitOpen) return true;

  const status = Number(failure?.error?.status);
  if (!Number.isFinite(status)) return true;
  return status === 408 || status === 429 || status >= 500;
}

function wait(ms) {
  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      pendingTimers.delete(timer);
      resolve();
    }, ms);
    pendingTimers.add(timer);
  });
}

function commandFromFailure(failure) {
  if (!failure || typeof failure !== "object") return null;
  const command = failure.command;
  if (!command || typeof command !== "object") return null;
  return command;
}

async function processFailure(failure, key) {
  if (!failure || typeof failure !== "object") {
    return { skipped: true, reason: "invalid_failure" };
  }

  const command = commandFromFailure(failure);
  if (!command) {
    await publish(KAFKA_TOPIC_DEAD_LETTER, key, {
      type: "dead_letter",
      at: new Date().toISOString(),
      reason: "missing_command",
      failure,
    });
    return { ok: true, deadLetter: true, reason: "missing_command" };
  }

  const retryCount = Number(failure.retryCount ?? command.retryCount ?? 0);

  if (!isRetryableFailure(failure)) {
    await publish(KAFKA_TOPIC_DEAD_LETTER, key, {
      type: "dead_letter",
      at: new Date().toISOString(),
      eventId: failure.eventId || command.eventId || "unknown",
      retryCount,
      reason: "non_retryable_error",
      command,
      failure,
    });
    console.error(`[${SERVICE_NAME}] dead_letter non_retryable eventId=${failure.eventId || command.eventId || "unknown"} status=${failure?.error?.status || ""}`);
    return { ok: true, deadLetter: true, reason: "non_retryable_error" };
  }

  if (retryCount >= MAX_RETRY_COUNT) {
    await publish(KAFKA_TOPIC_DEAD_LETTER, key, {
      type: "dead_letter",
      at: new Date().toISOString(),
      eventId: failure.eventId || command.eventId || "unknown",
      retryCount,
      command,
      failure,
    });
    console.error(`[${SERVICE_NAME}] dead_letter eventId=${failure.eventId || command.eventId || "unknown"} retry=${retryCount}`);
    return { ok: true, deadLetter: true };
  }

  const nextRetryCount = retryCount + 1;
  const delayMs = retryDelayMs(retryCount);
  await wait(delayMs);

  await publish(KAFKA_TOPIC_SEND_RETRY, key, {
    ...command,
    retryCount: nextRetryCount,
    lastError: failure.error || null,
    retryAt: new Date().toISOString(),
  });

  console.warn(
    `[${SERVICE_NAME}] send_retry eventId=${failure.eventId || command.eventId || "unknown"} retry=${nextRetryCount} delayMs=${delayMs}`
  );
  return { ok: true, retried: true };
}

async function start() {
  console.log(`[${SERVICE_NAME}] starting on port ${PORT}...`);
  console.log(`[${SERVICE_NAME}] brokers=${(KAFKA_BROKERS.length ? KAFKA_BROKERS : [KAFKA_BROKER]).join(",")} groupId=${GROUP_ID}`);
  console.log(`[${SERVICE_NAME}] consuming: ${KAFKA_TOPIC_FAILED}`);
  console.log(`[${SERVICE_NAME}] publishing: ${KAFKA_TOPIC_SEND_RETRY}, ${KAFKA_TOPIC_DEAD_LETTER}`);
  console.log(`[${SERVICE_NAME}] maxRetry=${MAX_RETRY_COUNT} retryNonRetryableErrors=${RETRY_NON_RETRYABLE_ERRORS}`);

  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: KAFKA_TOPIC_FAILED, fromBeginning: FROM_BEGINNING });

  await consumer.run({
    partitionsConsumedConcurrently: PARTITIONS_CONCURRENTLY,
    eachBatchAutoResolve: false,
    eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary, isRunning, isStale }) => {
      for (const message of batch.messages.slice(0, MAX_MESSAGES_PER_BATCH)) {
        if (!isRunning() || isStale()) break;

        const failure = safeJsonParse(message.value);
        const key = message.key ? message.key.toString("utf8") : "";

        try {
          await processFailure(failure, key);
        } catch (err) {
          console.error(`[${SERVICE_NAME}] failed to process send_failed: ${err?.message || err}`);
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
  for (const timer of pendingTimers) clearTimeout(timer);
  Promise.resolve()
    .then(() => consumer.disconnect())
    .then(() => producer.disconnect())
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
