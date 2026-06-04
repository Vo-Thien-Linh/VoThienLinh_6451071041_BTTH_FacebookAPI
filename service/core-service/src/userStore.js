"use strict";

const fs = require("fs");
const path = require("path");

function readJson(filePath, fallback) {
  try {
    const s = fs.readFileSync(filePath, "utf8");
    return JSON.parse(s);
  } catch {
    return fallback;
  }
}

function atomicWriteJson(filePath, obj) {
  const dir = path.dirname(filePath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

  const tmp = `${filePath}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(obj, null, 2), "utf8");
  fs.renameSync(tmp, filePath);
}

class UserStore {
  constructor({ dataDir }) {
    this.dataDir = dataDir;
    this.blacklistPath = path.join(dataDir, "blacklist.json");
    this.statsPath = path.join(dataDir, "user_stats.json");

    this.autoReload = String(process.env.USERSTORE_AUTO_RELOAD || "true").toLowerCase() === "true";
    this._blacklistMtimeMs = 0;

    // Use *Data suffix to avoid shadowing prototype methods.
    this.blacklistData = readJson(this.blacklistPath, { users: {} });
    this.statsData = readJson(this.statsPath, { users: {} });

    this.flushEvery = Number(process.env.USERSTORE_FLUSH_EVERY || 50);
    this._dirty = 0;
  }

  #reloadBlacklistIfChanged(force) {
    if (!this.autoReload) return;
    try {
      const st = fs.statSync(this.blacklistPath);
      const m = Number(st.mtimeMs || 0);
      if (!force && m && this._blacklistMtimeMs && m === this._blacklistMtimeMs) return;
      this._blacklistMtimeMs = m;
      this.blacklistData = readJson(this.blacklistPath, { users: {} });
    } catch {
      // ignore
    }
  }

  isBlacklisted(actorId) {
    this.#reloadBlacklistIfChanged(false);
    if (!actorId) return false;
    return !!this.blacklistData.users[String(actorId)];
  }

  blacklist(actorId, meta) {
    if (!actorId) return;
    this.blacklistData.users[String(actorId)] = {
      at: meta?.at || new Date().toISOString(),
      reason: meta?.reason || [],
    };
    this.#dirty();
  }

  recordAndCountRepeat(actorId, textHash, nowMs) {
    const actorKey = String(actorId || "unknown");
    if (!this.statsData.users[actorKey]) {
      this.statsData.users[actorKey] = { hashes: [], comments: [] };
    }

    if (!Array.isArray(this.statsData.users[actorKey].comments)) {
      this.statsData.users[actorKey].comments = [];
    }

    const windowMs = 24 * 60 * 60 * 1000;
    const cutoff = nowMs - windowMs;

    const hashes = this.statsData.users[actorKey].hashes;
    // keep only last 24h
    const filtered = hashes.filter((h) => h && typeof h.t === "number" && h.t >= cutoff);
    filtered.push({ h: String(textHash), t: nowMs });
    this.statsData.users[actorKey].hashes = filtered;

    const count = filtered.filter((x) => x.h === String(textHash)).length;

    this.#dirty();
    return count;
  }

  recordAndCountCommentRate(actorId, nowMs, windowMs) {
    const actorKey = String(actorId || "unknown");
    if (!this.statsData.users[actorKey]) {
      this.statsData.users[actorKey] = { hashes: [], comments: [] };
    }

    const u = this.statsData.users[actorKey];
    if (!Array.isArray(u.comments)) u.comments = [];

    const w = Number(windowMs);
    const safeWindowMs = Number.isFinite(w) ? Math.max(1000, w) : 60_000;
    const cutoff = nowMs - safeWindowMs;

    const filtered = u.comments.filter((t) => typeof t === "number" && t >= cutoff);
    filtered.push(nowMs);
    u.comments = filtered;

    this.#dirty();
    return filtered.length;
  }

  #dirty() {
    this._dirty += 1;
    if (this._dirty >= this.flushEvery) {
      this.flush();
      this._dirty = 0;
    }
  }

  flush() {
    atomicWriteJson(this.blacklistPath, this.blacklistData);
    atomicWriteJson(this.statsPath, this.statsData);

    // refresh mtime cache so subsequent reads don't re-load immediately
    this.#reloadBlacklistIfChanged(true);
  }
}

module.exports = {
  UserStore,
};
