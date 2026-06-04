"use strict";

class FacebookClient {
  constructor({ accessToken, graphVersion, timeoutMs }) {
    this.accessToken = String(accessToken || "")
      .trim()
      .replace(/^"|"$/g, "")
      .replace(/[\u200B-\u200D\uFEFF]/g, "")
      .replace(/\s+/g, "")
      .replace(/[^\x21-\x7E]/g, "");
    this.graphVersion = String(graphVersion || "v23.0").trim();
    const raw = Number(timeoutMs);
    this.timeoutMs = Number.isFinite(raw) ? Math.max(250, raw) : 8000;
  }

  isConfigured() {
    return !!this.accessToken;
  }

  tokenDiagnostics() {
    const t = String(this.accessToken || "");
    const masked = t.length <= 8 ? "<short>" : `${t.slice(0, 4)}...${t.slice(-4)}`;
    const eaaCount = (t.match(/EAA/g) || []).length;
    const looksConcatenated = eaaCount >= 2;
    const looksLikeToken = /^[A-Za-z0-9._-]+$/.test(t);
    return { length: t.length, masked, eaaCount, looksConcatenated, looksLikeToken };
  }

  async #request(path, { method, query, bodyForm }) {
    const url = new URL(`https://graph.facebook.com/${this.graphVersion}/${path}`);
    if (query && typeof query === "object") {
      for (const [k, v] of Object.entries(query)) {
        if (v === undefined || v === null) continue;
        url.searchParams.set(k, String(v));
      }
    }

    const headers = { authorization: `Bearer ${this.accessToken}` };

    let body = undefined;
    if (bodyForm && typeof bodyForm === "object") {
      headers["content-type"] = "application/x-www-form-urlencoded";
      const sp = new URLSearchParams();
      for (const [k, v] of Object.entries(bodyForm)) {
        if (v === undefined || v === null) continue;
        sp.set(k, String(v));
      }
      body = sp.toString();
    }

    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), this.timeoutMs);

    try {
      const resp = await fetch(url.toString(), { method, headers, body, signal: controller.signal });
      const data = await resp.json().catch(() => null);
      if (!resp.ok) {
        if (data?.error?.message && typeof data.error.message === "string") {
          data.error.message = data.error.message.replace(/Malformed access token\s+\S+/g, "Malformed access token <redacted>");
        }
        const err = new Error(`Facebook API ${method} ${path} failed: ${resp.status}`);
        err.status = resp.status;
        err.data = data;
        throw err;
      }
      return data;
    } finally {
      clearTimeout(t);
    }
  }

  async hideComment(commentId) {
    if (!commentId) throw new Error("commentId is required");
    try {
      return await this.#request(String(commentId), { method: "POST", bodyForm: { is_hidden: "true" } });
    } catch (err) {
      const subcode = err?.data?.error?.error_subcode;
      if (err?.status === 400 && subcode === 1446036) {
        return { success: true, alreadyModerated: true, subcode };
      }
      throw err;
    }
  }

  async replyToComment(commentId, message) {
    if (!commentId) throw new Error("commentId is required");
    if (!message) throw new Error("message is required");
    return this.#request(`${commentId}/comments`, { method: "POST", bodyForm: { message } });
  }

  async deleteComment(commentId) {
    if (!commentId) throw new Error("commentId is required");
    return this.#request(String(commentId), { method: "DELETE" });
  }
}

module.exports = { FacebookClient };
