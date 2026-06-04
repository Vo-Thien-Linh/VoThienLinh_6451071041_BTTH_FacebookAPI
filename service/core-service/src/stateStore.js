"use strict";

const fs = require("fs");
const path = require("path");

class StateStore {
  constructor({ dataDir }) {
    this.dataDir = dataDir;
    this.logPath = path.join(dataDir, "event_states.jsonl");
    this.indexPath = path.join(dataDir, "state_index.json");

    this.finalStates = new Set(["processed", "replied", "failed"]);

    this.index = this.#loadIndex();
    this._pendingIndexFlush = 0;
    this.flushEvery = Number(process.env.STATESTORE_FLUSH_EVERY || 100);

    // periodic flush for safety
    const intervalMsRaw = Number(process.env.STATESTORE_FLUSH_INTERVAL_MS || 5000);
    const intervalMs = Number.isFinite(intervalMsRaw) ? Math.max(250, intervalMsRaw) : 5000;

    this._timer = setInterval(() => {
      try {
        this.flush();
      } catch {
        // ignore
      }
    }, intervalMs);
    this._timer.unref?.();
  }

  #loadIndex() {
    try {
      const s = fs.readFileSync(this.indexPath, "utf8");
      const parsed = JSON.parse(s);
      if (parsed && typeof parsed === "object" && parsed.events) {
        if (!parsed.repliedComments || typeof parsed.repliedComments !== "object") {
          parsed.repliedComments = {};
        }
        return parsed;
      }
      return { events: {}, repliedComments: {} };
    } catch {
      return { events: {}, repliedComments: {} };
    }
  }

  hasRepliedComment(commentId) {
    const id = String(commentId || "").trim();
    if (!id) return false;
    return !!this.index.repliedComments?.[id];
  }

  markCommentReplied(commentId, meta) {
    const id = String(commentId || "").trim();
    if (!id) return;
    if (!this.index.repliedComments || typeof this.index.repliedComments !== "object") {
      this.index.repliedComments = {};
    }
    this.index.repliedComments[id] = {
      at: meta?.at || new Date().toISOString(),
      eventId: meta?.eventId || null,
    };
    this._pendingIndexFlush += 1;
    if (this._pendingIndexFlush >= this.flushEvery) {
      this.flush();
      this._pendingIndexFlush = 0;
    }
  }

  isFinal(eventId) {
    const id = String(eventId || "");
    const st = this.index.events[id]?.status;
    return this.finalStates.has(st);
  }

  async mark(eventId, status, extra) {
    const id = String(eventId || "unknown");
    const at = new Date().toISOString();

    const record = { eventId: id, status, at, extra: extra || null };

    // ensure dir
    if (!fs.existsSync(this.dataDir)) fs.mkdirSync(this.dataDir, { recursive: true });
    fs.appendFileSync(this.logPath, `${JSON.stringify(record)}\n`, "utf8");

    this.index.events[id] = { status, at };

    this._pendingIndexFlush += 1;
    if (this._pendingIndexFlush >= this.flushEvery) {
      this.flush();
      this._pendingIndexFlush = 0;
    }
  }

  flush() {
    if (!fs.existsSync(this.dataDir)) fs.mkdirSync(this.dataDir, { recursive: true });

    const tmp = `${this.indexPath}.tmp`;
    fs.writeFileSync(tmp, JSON.stringify(this.index, null, 2), "utf8");
    fs.renameSync(tmp, this.indexPath);
  }
}

module.exports = {
  StateStore,
};
