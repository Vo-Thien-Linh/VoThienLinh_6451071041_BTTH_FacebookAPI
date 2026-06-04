const express = require("express");
const crypto = require("crypto");
const { Kafka, Partitioners } = require("kafkajs");
const fs = require("fs");
const path = require("path");
require("dotenv").config({ path: fs.existsSync(path.join(__dirname, ".env")) ? path.join(__dirname, ".env") : path.join(__dirname, "..", ".env") });
const { patchKafkaJsNegativeTimeoutWarning } = require("./src/kafkajsPatch");

patchKafkaJsNegativeTimeoutWarning();

const app = express();
const PORT = Number(process.env.PORT || 3001);
const VERIFY_TOKEN = process.env.VERIFY_TOKEN || "linhfacebookapi";
const APP_SECRET = (process.env.APP_SECRET || "").trim();
const KAFKA_TOPIC = process.env.KAFKA_TOPIC || "raw_events";

const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, "..", "data");
const WEBHOOK_FILE_LOG = String(process.env.WEBHOOK_FILE_LOG || "").toLowerCase() === "true";

function appendJsonlSafe(filePath, obj) {
  try {
    const dir = path.dirname(filePath);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    fs.appendFileSync(filePath, `${JSON.stringify(obj)}\n`, "utf8");
  } catch {
    // ignore
  }
}

// docker-compose.yml exposes Redpanda externally on localhost:19092.
// If running this service inside the compose network, set KAFKA_BROKER=kafka:9092.
const DEFAULT_KAFKA_BROKER = "localhost:19092";
const KAFKA_BROKER = (process.env.KAFKA_BROKER || DEFAULT_KAFKA_BROKER).trim();
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

if (!APP_SECRET) {
  console.error(
    "Missing APP_SECRET env var. Set APP_SECRET to your Facebook App Secret; otherwise x-hub-signature-256 verification will fail for real deliveries."
  );
  process.exit(1);
}

const kafka = new Kafka({
  clientId: "webhook-service",
  brokers: KAFKA_BROKERS.length ? KAFKA_BROKERS : [KAFKA_BROKER],
});
const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner,
});

let producerReady = false;
let producerConnectPromise = null;
let lastKafkaError = null;

// Parse JSON nhưng giữ lại raw body cho Facebook signature verification
app.use(
  express.json({
    verify: (req, _res, buf) => {
      // Use req.path to avoid missing rawBody due to querystring or trailing slash
      if (typeof req.path === "string" && req.path.startsWith("/webhook")) {
        req.rawBody = buf;
      }
    },
  })
);

// Log mọi request đi vào webhook để dễ debug (không log payload)
app.use("/webhook", (req, res, next) => {
  const startedAt = Date.now();
  res.on("finish", () => {
    const ms = Date.now() - startedAt;
    console.log(
      `[${new Date().toISOString()}] /webhook ${req.method} -> ${res.statusCode} (${ms}ms) sig256=${req.get("x-hub-signature-256") ? "yes" : "no"}`
    );
  });
  next();
});

function verifySignature(rawBody, headerSig) {
  if (!rawBody || !Buffer.isBuffer(rawBody)) return false;
  if (typeof headerSig !== "string") return false;

  const sig = headerSig.trim();
  if (!sig.toLowerCase().startsWith("sha256=")) return false;

  const sigHex = sig.slice("sha256=".length).trim();
  // Expect 32-byte HMAC => 64 hex chars
  if (!/^[0-9a-fA-F]{64}$/.test(sigHex)) return false;

  const expectedHex = crypto
    .createHmac("sha256", APP_SECRET)
    .update(rawBody)
    .digest("hex");

  const a = Buffer.from(expectedHex, "hex");
  const b = Buffer.from(sigHex, "hex");
  if (a.length !== b.length) return false;
  return crypto.timingSafeEqual(a, b);
}

function ensureProducerConnected() {
  if (producerReady) return Promise.resolve();
  if (producerConnectPromise) return producerConnectPromise;

  producerConnectPromise = producer
    .connect()
    .then(() => {
      producerReady = true;
      lastKafkaError = null;
      console.log(
        `Kafka producer connected brokers=${(KAFKA_BROKERS.length ? KAFKA_BROKERS : [KAFKA_BROKER]).join(",")}`
      );
    })
    .catch((e) => {
      lastKafkaError = e;
      // Allow future retries
      producerConnectPromise = null;
      producerReady = false;
      console.error("Kafka connect error (webhook will still run):", e);
      throw e;
    });

  return producerConnectPromise;
}

function normalizeFacebookEvent(rawPayload) {
  const now = new Date().toISOString();
  const out = [];

  function stableEventId(parts) {
    const s = parts.filter((p) => p !== null && p !== undefined).map(String).join("|");
    const hex = crypto.createHash("sha256").update(s).digest("hex").slice(0, 24);
    return `fb-${hex}`;
  }

  const entries = Array.isArray(rawPayload.entry) ? rawPayload.entry : [];
  for (const entry of entries) {
    const pageId = entry.id || null;
    const time = entry.time ? new Date(entry.time).toISOString() : now;

    if (Array.isArray(entry.changes)) {
      for (const c of entry.changes) {
        const item = c.value?.item;
        const verb = c.value?.verb || null;
        const isFeed = c.field === "feed" && !!c.value;

        const isComment = isFeed && item === "comment";
        if (isComment) {
          const eventType = verb === "remove" ? "comment.deleted" : verb === "edited" ? "comment.edited" : "comment.created";
          const commentId = c.value.comment_id || null;
          out.push({
            eventId: commentId ? stableEventId(["comment", pageId, eventType, commentId]) : crypto.randomUUID(),
            source: "facebook",
            eventType,
            pageId,
            actorId: c.value.from?.id || null,
            objectId: c.value.post_id || c.value.comment_id || null,
            occurredAt: time,
            receivedAt: now,
            payload: {
              message: c.value.message || null,
              postId: c.value.post_id || null,
              commentId
            },
            raw: c
          });
        }

        // Page reactions/likes thường đi qua feed change với item = "reaction"
        const isReaction = isFeed && (item === "reaction" || item === "like");
        if (isReaction) {
          const eventType = verb === "remove" ? "reaction.deleted" : "reaction.created";
          const objId = c.value.post_id || c.value.comment_id || null;
          out.push({
            eventId: objId ? stableEventId(["reaction", pageId, eventType, objId]) : crypto.randomUUID(),
            source: "facebook",
            eventType,
            pageId,
            actorId: c.value.from?.id || null,
            objectId: c.value.post_id || c.value.comment_id || null,
            occurredAt: time,
            receivedAt: now,
            payload: {
              reactionType: c.value.reaction_type || null,
              postId: c.value.post_id || null,
              commentId: c.value.comment_id || null
            },
            raw: c,
          });
        }
      }
    }

    if (Array.isArray(entry.messaging)) {
      for (const m of entry.messaging) {
        const mid = m.message?.mid || null;
        out.push({
          eventId: mid ? stableEventId(["message", pageId, mid]) : crypto.randomUUID(),
          source: "facebook",
          eventType: "message.created",
          pageId,
          actorId: m.sender?.id || null,
          objectId: m.recipient?.id || null,
          occurredAt: m.timestamp ? new Date(m.timestamp).toISOString() : time,
          receivedAt: now,
          payload: {
            text: m.message?.text || null,
            mid
          },
          raw: m
        });
      }
    }
  }

  return out;
}

// Facebook verify endpoint
app.get("/webhook", (req, res) => {
  const mode = req.query["hub.mode"];
  const token = req.query["hub.verify_token"];
  const challenge = req.query["hub.challenge"];

  if (mode === "subscribe" && token === VERIFY_TOKEN) {
    return res.status(200).send(challenge);
  }
  return res.sendStatus(403);
});

// Facebook event endpoint
app.post("/webhook", async (req, res) => {
  try {
    const rawBody = req.rawBody;
    const sig = req.get("x-hub-signature-256");

    const disableSigCheck = String(process.env.DISABLE_WEBHOOK_SIGNATURE || "").toLowerCase() === "true";
    const rejectInvalidSig = String(process.env.REJECT_INVALID_SIGNATURE || "").toLowerCase() === "true";
    const sigDebug = String(process.env.SIGNATURE_DEBUG || "").toLowerCase() === "true";

    if (!rawBody || !Buffer.isBuffer(rawBody)) {
      console.warn("Webhook received without raw body (check body parser config)");

      if (WEBHOOK_FILE_LOG) {
        appendJsonlSafe(path.join(DATA_DIR, "webhook_requests.jsonl"), {
          at: new Date().toISOString(),
          ok: false,
          reason: "missing_raw_body",
          method: req.method,
          path: req.path,
          status: 400,
          hasSig: !!sig,
          contentType: req.get("content-type") || null,
        });
      }

      return res.sendStatus(400);
    }

    let signatureOk = null;
    if (!disableSigCheck) {
      const ok = verifySignature(rawBody, sig);
      signatureOk = ok;
      if (!ok) {
        const details = {
          hasSig: !!sig,
          sigPrefix: typeof sig === "string" ? sig.slice(0, 20) : null,
          rawLen: rawBody.length,
          contentType: req.get("content-type") || null,
        };

        if (sigDebug && typeof sig === "string" && /^[sS][hH][aA]256=/.test(sig.trim())) {
          const sigHex = sig.trim().slice("sha256=".length).trim();
          const expectedHex = crypto
            .createHmac("sha256", APP_SECRET)
            .update(rawBody)
            .digest("hex");
          details.expectedPrefix = `sha256=${expectedHex.slice(0, 16)}`;
          details.receivedPrefix = `sha256=${sigHex.slice(0, 16)}`;
          details.appSecretLen = APP_SECRET.length;
        }

        console.warn("Invalid webhook signature", details);

        if (WEBHOOK_FILE_LOG) {
          appendJsonlSafe(path.join(DATA_DIR, "webhook_requests.jsonl"), {
            at: new Date().toISOString(),
            ok: false,
            reason: "invalid_signature",
            method: req.method,
            path: req.path,
            status: rejectInvalidSig ? 401 : 200,
            hasSig: !!sig,
            signatureOk: false,
            rawLen: rawBody.length,
            contentType: req.get("content-type") || null,
            details,
          });
        }

        if (rejectInvalidSig) {
          return res.sendStatus(401);
        }
      }
    }

    const payload = req.body;
    const normalized = normalizeFacebookEvent(payload);

    const now = new Date().toISOString();
    const eventsToSend = normalized.length
      ? normalized
      : [
          {
            eventId: crypto.randomUUID(),
            source: "facebook",
            eventType: "webhook.raw",
            pageId: payload?.entry?.[0]?.id || null,
            actorId: null,
            objectId: null,
            occurredAt: now,
            receivedAt: now,
            payload: {},
            raw: payload,
          },
        ];

    console.log(
      `Webhook OK: object=${payload?.object || "unknown"} entries=${Array.isArray(payload?.entry) ? payload.entry.length : 0} normalized=${normalized.length} sent=${eventsToSend.length}`
    );

    if (WEBHOOK_FILE_LOG) {
      appendJsonlSafe(path.join(DATA_DIR, "webhook_requests.jsonl"), {
        at: new Date().toISOString(),
        ok: true,
        method: req.method,
        path: req.path,
        status: 200,
        hasSig: !!sig,
        signatureOk,
        object: payload?.object || null,
        entries: Array.isArray(payload?.entry) ? payload.entry.length : 0,
        normalized: normalized.length,
        sent: eventsToSend.length,
      });

      for (const ev of eventsToSend) {
        appendJsonlSafe(path.join(DATA_DIR, "webhook_events.jsonl"), {
          at: new Date().toISOString(),
          eventId: ev.eventId || null,
          eventType: ev.eventType || null,
          pageId: ev.pageId || null,
          actorId: ev.actorId || null,
          objectId: ev.objectId || null,
          commentId: ev.payload?.commentId || null,
          textLen: typeof ev.payload?.message === "string" ? ev.payload.message.length : typeof ev.payload?.text === "string" ? ev.payload.text.length : null,
        });
      }
    }

    // Do not block webhook response on Kafka.
    // Attempt a (guarded) connect, then publish; on failures, log and move on.
    ensureProducerConnected().catch(() => {
      console.warn("Kafka not connected; publish will fail until broker is reachable", {
        topic: KAFKA_TOPIC,
        brokers: KAFKA_BROKERS.length ? KAFKA_BROKERS : [KAFKA_BROKER],
        lastError: lastKafkaError ? String(lastKafkaError.message || lastKafkaError) : null,
      });
    });

    for (const ev of eventsToSend) {
      ensureProducerConnected()
        .then(() =>
          producer.send({
            topic: KAFKA_TOPIC,
            messages: [
              { key: ev.pageId || ev.objectId || "unknown", value: JSON.stringify(ev) },
            ],
          })
        )
        .catch((e) => console.error("Kafka publish error", e));
    }

    return res.status(200).send("EVENT_RECEIVED");
  } catch (err) {
    console.error("Webhook error:", err);
    return res.sendStatus(500);
  }
});

async function start() {
  // Always start the HTTP server. Kafka connectivity should not prevent receiving webhooks.
  app.listen(PORT, () => {
    console.log(`webhook-service listening on port ${PORT}`);
  });

  if (!process.env.KAFKA_BROKER && !process.env.KAFKA_BROKERS) {
    console.warn(
      `KAFKA_BROKER not set; defaulting to ${DEFAULT_KAFKA_BROKER}. If Kafka is in Docker network, set KAFKA_BROKER=kafka:9092.`
    );
  }

  // Kick off connection attempt, but do not fail startup if Kafka is down.
  ensureProducerConnected().catch(() => {});
}

start().catch((e) => {
  console.error("Startup error:", e);
  process.exit(1);
});