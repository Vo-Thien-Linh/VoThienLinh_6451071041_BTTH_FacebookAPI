"use strict";

class FacebookClient {
  constructor({ accessToken, graphVersion, timeoutMs }) {
    this.accessToken = String(accessToken || "")
      .trim()
      .replace(/^"|"$/g, "")
      .replace(/[\u200B-\u200D\uFEFF]/g, "")
      .replace(/\s+/g, "")
      // Remove any non-printable ASCII that can sneak in during copy/paste.
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
    return {
      length: t.length,
      masked,
      eaaCount,
      looksConcatenated,
      looksLikeToken,
    };
  }

  async #request(path, { method, query, bodyForm }) {
    const url = new URL(`https://graph.facebook.com/${this.graphVersion}/${path}`);
    if (query && typeof query === "object") {
      for (const [k, v] of Object.entries(query)) {
        if (v === undefined || v === null) continue;
        url.searchParams.set(k, String(v));
      }
    }

    const headers = {
      authorization: `Bearer ${this.accessToken}`,
    };

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

    const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
    const maxAttempts = Number(process.env.FB_RETRY_ATTEMPTS || 2);
    const baseDelayMs = Number(process.env.FB_RETRY_BASE_DELAY_MS || 250);

    let lastErr = null;
    for (let attempt = 1; attempt <= Math.max(1, maxAttempts); attempt += 1) {
      const controller = new AbortController();
      const t = setTimeout(() => controller.abort(), this.timeoutMs);
      try {
        const resp = await fetch(url.toString(), {
          method,
          headers,
          body,
          signal: controller.signal,
        });

        const data = await resp.json().catch(() => null);
        if (!resp.ok) {
          // Redact token if Facebook echoes it back (happens on malformed token errors).
          if (data?.error?.message && typeof data.error.message === "string") {
            data.error.message = data.error.message.replace(
              /Malformed access token\s+\S+/g,
              "Malformed access token <redacted>"
            );
          }
          const err = new Error(`Facebook API ${method} ${path} failed: ${resp.status}`);
          err.status = resp.status;
          err.data = data;
          throw err;
        }
        return data;
      } catch (err) {
        lastErr = err;
        const isAbort = err?.name === "AbortError" || /aborted/i.test(String(err?.message || ""));
        // Only retry transient network abort/timeouts.
        if (attempt < Math.max(1, maxAttempts) && isAbort) {
          const delay = Math.max(0, baseDelayMs) * Math.pow(2, attempt - 1);
          await sleep(delay);
          continue;
        }
        throw err;
      } finally {
        clearTimeout(t);
      }
    }

    throw lastErr;
  }

  async hideComment(commentId) {
    if (!commentId) throw new Error("commentId is required");
    // Hide comment: POST /{comment-id}?is_hidden=true
    try {
      return await this.#request(String(commentId), {
        method: "POST",
        bodyForm: { is_hidden: "true" },
      });
    } catch (err) {
      // Facebook sometimes returns a 400 with error_subcode=1446036 when the comment
      // was already moderated/flagged as spam. Treat as idempotent success.
      const subcode = err?.data?.error?.error_subcode;
      const userTitle = err?.data?.error?.error_user_title;
      if (err?.status === 400 && subcode === 1446036) {
        return { success: true, alreadyModerated: true, subcode, userTitle };
      }
      throw err;
    }
  }

  async replyToComment(commentId, message) {
    if (!commentId) throw new Error("commentId is required");
    if (!message) throw new Error("message is required");
    // Reply: POST /{comment-id}/comments with message
    return this.#request(`${commentId}/comments`, {
      method: "POST",
      bodyForm: { message },
    });
  }

  async deleteComment(commentId) {
    if (!commentId) throw new Error("commentId is required");
    // Delete comment: DELETE /{comment-id}
    return this.#request(String(commentId), {
      method: "DELETE",
    });
  }

  async deleteComment(commentId) {
  if (!commentId) throw new Error("commentId is required");
  // DELETE /{comment-id} — xóa hẳn, không ai thấy nữa
  return this.#request(String(commentId), { method: "DELETE" });
}

async reportComment(commentId) {
  if (!commentId) throw new Error("commentId is required");
  // Report spam — Facebook review
  return this.#request(String(commentId), {
    method: "POST",
    bodyForm: {
      object_type: "comment",
      reason: "SPAM",
    },
  });
}

async moderateComment(commentId, action = "delete") {
  // action: "delete" | "hide" | "report"
  if (action === "delete") return this.deleteComment(commentId);
  if (action === "hide") return this.hideComment(commentId);   // chỉ ẩn với admin
  if (action === "report") return this.reportComment(commentId);
  throw new Error(`Unknown action: ${action}`);
}
}

module.exports = {
  FacebookClient,
};
