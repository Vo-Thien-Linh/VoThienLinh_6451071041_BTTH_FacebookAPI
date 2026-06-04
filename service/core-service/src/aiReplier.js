"use strict";

function clamp01(n) {
  const x = Number(n);
  if (Number.isNaN(x)) return 0;
  return Math.max(0, Math.min(1, x));
}

function safeJsonFromText(s) {
  const str = String(s || "")
    .trim()
    .replace(/^```json\s*/i, "")
    .replace(/^```\s*/i, "")
    .replace(/\s*```$/i, "")
    .trim();

  try {
    return JSON.parse(str);
  } catch {
    // fallthrough
  }

  const first = str.indexOf("{");
  const last = str.lastIndexOf("}");
  if (first >= 0 && last > first) {
    try {
      return JSON.parse(str.slice(first, last + 1));
    } catch {
      // ignore
    }
  }

  return null;
}

function normalizeReplyText(s, maxChars) {
  const raw = String(s || "").trim();
  if (!raw) return "";

  // Remove excessive whitespace/newlines.
  const compact = raw.replace(/\s+/g, " ").trim();
  if (!compact) return "";

  const limit = Number.isFinite(Number(maxChars)) ? Math.max(40, Number(maxChars)) : 400;
  if (compact.length <= limit) return compact;
  return compact.slice(0, limit - 1).trimEnd() + "…";
}

function fallbackReply({ intent, commentText, generalChat }) {
  // Keep templates conservative. If the intent needs more context, ask to inbox.
  switch (intent) {
    case "price_inquiry":
      return {
        shouldReply: true,
        reply:
          "Dạ shop đã nhận được câu hỏi giá ạ. Bạn cho shop xin mẫu bạn muốn (màu/size) và số lượng; nếu có link/mã sản phẩm thì gửi giúp shop để shop báo giá chính xác nhé.",
        confidence: 0.35,
        reason: "template_price",
        provider: "fallback",
      };
    case "product_inquiry":
      return {
        shouldReply: true,
        reply:
          "Dạ shop nhận được câu hỏi về sản phẩm ạ. Bạn cho shop xin size/màu bạn cần; nếu có link/mã sản phẩm thì gửi giúp shop để shop kiểm tra còn hàng và tư vấn nhanh nhất nhé.",
        confidence: 0.35,
        reason: "template_product",
        provider: "fallback",
      };
    case "shipping_inquiry":
      return {
        shouldReply: true,
        reply: "Dạ shop có hỗ trợ giao hàng ạ. Bạn cho shop xin khu vực (tỉnh/thành) để shop báo phí ship và thời gian dự kiến nhé.",
        confidence: 0.35,
        reason: "template_shipping",
        provider: "fallback",
      };
    case "complaint_support":
      return {
        shouldReply: true,
        reply: "Dạ shop xin lỗi bạn vì trải nghiệm chưa tốt. Bạn inbox giúp shop mã đơn/số điện thoại để shop kiểm tra và hỗ trợ ngay nhé.",
        confidence: 0.35,
        reason: "template_support",
        provider: "fallback",
      };
    case "praise_positive": {
      // Rotate so repeated compliments don't get the identical reply
      const praiseReplies = [
        "Dạ cảm ơn bạn nhiều ạ! Shop rất vui khi bạn thích.",
        "Dạ cảm ơn bạn đã ủng hộ shop nha! Bạn cần tư vấn thêm gì cứ nhắn shop nhé.",
        "Cảm ơn bạn nhiều lắm ạ! Shop sẽ cố gắng hơn nữa để phục vụ bạn tốt nhé.",
        "Dạ shop cảm ơn bạn ạ! Nếu bạn muốn đặt hàng thì inbox shop nha.",
        "Cảm ơn bạn đã ghé shop và để lại nhận xét dễ thương vậy ạ!",
      ];
      const reply = praiseReplies[Math.floor(Math.random() * praiseReplies.length)];
      return {
        shouldReply: true,
        reply,
        confidence: 0.35,
        reason: "template_thanks",
        provider: "fallback",
      };
    }
    default: {
      const t = String(commentText || "").trim();
      if (t.length <= 2) {
        return { shouldReply: false, reply: "", confidence: 0.2, reason: "too_short", provider: "fallback" };
      }

      // Purchase intent without an explicit question.
      // Example: "tôi cần áo màu đỏ".
      const looksLikePurchaseIntent = /\b(c[aâ]n|mu[oố]n|mua|[đd][aă]t)\b/i.test(t);
      const mentionsProductOrAttr = /(a[oó]|qu[aâ]n|v[aả]i|size|m[aà]u|mau|c[oò]n\s*h[aà]ng|ch[aấ]t\s*li[eệ]u)/i.test(t);
      if (looksLikePurchaseIntent && mentionsProductOrAttr) {
        return {
          shouldReply: true,
          reply: "Dạ bạn cho shop xin thêm size và mẫu/link sản phẩm giúp shop ạ; bạn cần màu đỏ đúng không ạ để shop kiểm tra còn hàng và báo giá nhé.",
          confidence: 0.3,
          reason: "purchase_intent_other_fallback",
          provider: "fallback",
        };
      }

      // If it's clearly a question but intent is unknown, reply with a clarifying question.
      const looksLikeQuestion = /\?|\b(kh[oô]ng|k|k\?)\b/i.test(t);
      if (looksLikeQuestion) {
        return {
          shouldReply: true,
          reply: "Dạ bạn cho shop xin thêm thông tin giúp shop ạ (mẫu/link sản phẩm, size/màu) để shop báo giá và tư vấn chính xác nhé.",
          confidence: 0.3,
          reason: "question_other_fallback",
          provider: "fallback",
        };
      }

      if (generalChat) {
        return {
          shouldReply: true,
          reply: "Dạ shop đây ạ. Bạn cần shop hỗ trợ gì thêm (size/màu/giá/ship) hoặc bạn nói rõ giúp shop để shop hỗ trợ nhanh nhất nhé.",
          confidence: 0.3,
          reason: "general_chat_fallback",
          provider: "fallback",
        };
      }
      return { shouldReply: false, reply: "", confidence: 0.2, reason: "no_template", provider: "fallback" };
    }
  }
}

function buildSystemPrompt({ shopName, generalChat }) {
  const shop = String(shopName || "Shop").trim() || "Shop";

  return `Bạn là nhân viên CSKH của ${shop}.
Mục tiêu: soạn câu trả lời NGẮN (1-2 câu), lịch sự, thân thiện, bằng tiếng Việt.

Trả về DUY NHẤT JSON hợp lệ, không thêm chữ nào khác.
Schema:
{"shouldReply": boolean, "reply": string, "confidence": number, "reason": string}

Quy tắc:
- Nếu comment quá ngắn (ví dụ: "ok", "👍", "haha") hoặc không có nội dung cần phản hồi -> shouldReply=false.
- Ngoại lệ: nếu comment ngắn nhưng mang ý chê/không hài lòng (ví dụ: "xấu", "tệ", "không ổn") -> shouldReply=true và xin lỗi + hỏi thêm chi tiết.
- Nếu người dùng chỉ gọi/nhắc (ví dụ: "shop ơi", "ad ơi", "ib") và không có câu hỏi cụ thể:
  - ${generalChat ? "shouldReply=true và mời người dùng nói rõ nhu cầu" : "shouldReply=false"}.
- Nếu người dùng hỏi ngoài chủ đề mua hàng, hãy trả lời lịch sự và ngắn gọn; nếu thiếu thông tin thì hỏi lại 1 câu để làm rõ.
- Nếu comment hỏi giá -> xin link/mã sản phẩm hoặc mời inbox để báo giá chính xác.
- Nếu hỏi size/màu/còn hàng/chất liệu -> hỏi lại thông tin cần thiết hoặc xin link sản phẩm.
- Nếu hỏi ship/giao hàng -> xin khu vực để báo phí và thời gian.
- Nếu khiếu nại/đổi trả -> xin lỗi + mời inbox + xin mã đơn/sđt để kiểm tra.
- Nếu khen -> cảm ơn ngắn gọn.
- Không tranh cãi, không công kích, không tiết lộ thông tin nội bộ.
- Không dùng emoji.

Yêu cầu chất lượng:
- reply không được rỗng khi shouldReply=true
- reply tối đa 400 ký tự.`;
}

class AiReplier {
  constructor({
    provider,
    openaiApiKey,
    geminiApiKey,
    claudeApiKey,
    model,
    timeoutMs,
    shopName,
    maxReplyChars,
    generalChat,
  }) {
    this.provider = provider || "openai";
    this.openaiApiKey = openaiApiKey || "";
    this.geminiApiKey = geminiApiKey || "";
    this.claudeApiKey = claudeApiKey || "";
    this.model = model || "gpt-4o-mini";

    const raw = Number(timeoutMs);
    this.timeoutMs = Number.isFinite(raw) ? Math.max(250, raw) : 8000;

    this.shopName = shopName || "Shop";
    this.maxReplyChars = Number.isFinite(Number(maxReplyChars)) ? Number(maxReplyChars) : 400;

    this.generalChat = !!generalChat;

    this._systemPrompt = buildSystemPrompt({ shopName: this.shopName, generalChat: this.generalChat });
  }

  isEnabled() {
    if (String(process.env.AI_REPLY_DISABLED || "").toLowerCase() === "true") return false;
    if (this.provider === "openai") return !!this.openaiApiKey;
    if (this.provider === "gemini") return !!this.geminiApiKey;
    if (this.provider === "claude") return !!this.claudeApiKey;
    return false;
  }

  async generateReply({ commentText, intent, sentiment }) {
    const text = String(commentText || "").trim().slice(0, 800);
    if (!text || text.length <= 1) {
      return { shouldReply: false, reply: "", confidence: 0.2, reason: "empty", provider: "fallback" };
    }

    const normalizedIntent = String(intent || "other");

    // Short negative feedback should still be answered.
    const negativeFeedback = /(x[ấa]u|t[eệ]|d[oở]|d[ơo]m|k[eé]m|kh[oô]ng\s*[oô]n|kh[oô]ng\s*[đd][eẹ]p|th[âa]t\s*v[oọ]ng)/i;
    if (text.length <= 12 && negativeFeedback.test(text)) {
      return {
        shouldReply: true,
        reply: normalizeReplyText(
          "Dạ shop xin lỗi vì bạn chưa hài lòng ạ. Bạn cho shop xin thêm chi tiết (mẫu nào/size/màu) để shop kiểm tra và hỗ trợ mình nhé.",
          this.maxReplyChars
        ),
        confidence: 0.35,
        reason: "short_negative_feedback",
        provider: "fallback",
      };
    }

    // In general chat mode, reply to short greetings/attention calls.
    const attention = /^(shop|ad|admin|chị|anh|em)\s*ơi\b|\bib\b|\binbox\b|\bshop\s*ơi\b/i;
    if (this.generalChat && text.length <= 20 && attention.test(text)) {
      return {
        shouldReply: true,
        reply: normalizeReplyText(
          "Dạ shop đây ạ. Bạn cần hỏi thông tin gì (size/màu/giá/ship) hoặc gửi link/mã sản phẩm để shop hỗ trợ nhanh nhé.",
          this.maxReplyChars
        ),
        confidence: 0.3,
        reason: "attention_general_chat",
        provider: "fallback",
      };
    }

    if (!this.isEnabled()) {
      return fallbackReply({ intent: normalizedIntent, commentText: text, generalChat: this.generalChat });
    }

    try {
      let out;
      if (this.provider === "openai") out = await this.#replyOpenAI({ text, intent: normalizedIntent, sentiment });
      else if (this.provider === "gemini") out = await this.#replyGemini({ text, intent: normalizedIntent, sentiment });
      else if (this.provider === "claude") out = await this.#replyClaude({ text, intent: normalizedIntent, sentiment });
      else out = fallbackReply({ intent: normalizedIntent, commentText: text, generalChat: this.generalChat });

      // Safety net: do not skip actionable intents due to occasional model misfires.
      const actionableIntents = new Set([
        "price_inquiry",
        "product_inquiry",
        "shipping_inquiry",
        "complaint_support",
        "praise_positive",
      ]);

      if (
        out &&
        out.shouldReply === false &&
        actionableIntents.has(normalizedIntent) &&
        text.length > 2
      ) {
        const forced = fallbackReply({ intent: normalizedIntent, commentText: text, generalChat: this.generalChat });
        if (forced?.shouldReply) {
          forced.reason = `forced_${forced.reason || "template"}`;
          forced.provider = forced.provider || "fallback";
          return forced;
        }
      }

      return out;
    } catch (err) {
      console.warn(`[AiReplier] ${this.provider} failed, using fallback:`, err?.message);
      return fallbackReply({ intent: normalizedIntent, commentText: text, generalChat: this.generalChat });
    }
  }

  #normalizeResult(parsed, providerName) {
    if (!parsed || typeof parsed !== "object") return null;

    const shouldReply = !!parsed.shouldReply;
    const reply = normalizeReplyText(parsed.reply, this.maxReplyChars);
    const confidence = clamp01(parsed.confidence);
    const reason = typeof parsed.reason === "string" ? parsed.reason.slice(0, 200) : "";

    if (shouldReply && !reply) {
      return { shouldReply: false, reply: "", confidence: 0.2, reason: "empty_reply", provider: providerName };
    }

    return { shouldReply, reply, confidence, reason, provider: providerName };
  }

  async #replyOpenAI({ text, intent, sentiment }) {
    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), this.timeoutMs);

    try {
      const resp = await fetch("https://api.openai.com/v1/chat/completions", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${this.openaiApiKey}`,
        },
        body: JSON.stringify({
          model: this.model,
          temperature: 0.4,
          max_tokens: 220,
          response_format: { type: "json_object" },
          messages: [
            { role: "system", content: this._systemPrompt },
            {
              role: "user",
              content: `Comment: ${JSON.stringify(text)}\nIntent: ${String(intent || "unknown")}\nSentiment: ${String(sentiment || "unknown")}`,
            },
          ],
        }),
        signal: controller.signal,
      });

      const data = await resp.json().catch(() => null);
      const content = data?.choices?.[0]?.message?.content || "";
      const parsed = safeJsonFromText(content);
      const normalized = this.#normalizeResult(parsed, "openai");
      return normalized || fallbackReply({ intent: String(intent || "other"), commentText: text });
    } finally {
      clearTimeout(t);
    }
  }

  async #replyGemini({ text, intent, sentiment }) {
    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), this.timeoutMs);

    try {
      const rawModel = String(this.model || "").trim();
      const model = /^gemini-[\w.-]+$/i.test(rawModel) ? rawModel : "gemini-2.5-flash";
      const url = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${this.geminiApiKey}`;

      const prompt =
        this._systemPrompt +
        `\n\nDữ liệu đầu vào:\nComment: ${JSON.stringify(text)}\nIntent: ${String(intent || "unknown")}\nSentiment: ${String(sentiment || "unknown")}`;

      const resp = await fetch(url, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          contents: [{ parts: [{ text: prompt }] }],
          generationConfig: { temperature: 0.4, maxOutputTokens: 300 },
        }),
        signal: controller.signal,
      });

      const data = await resp.json().catch(() => null);
      const content = data?.candidates?.[0]?.content?.parts?.[0]?.text || "";
      const parsed = safeJsonFromText(content);
      const normalized = this.#normalizeResult(parsed, "gemini");
      return normalized || fallbackReply({ intent: String(intent || "other"), commentText: text });
    } finally {
      clearTimeout(t);
    }
  }

  async #replyClaude({ text, intent, sentiment }) {
    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), this.timeoutMs);

    try {
      const resp = await fetch("https://api.anthropic.com/v1/messages", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-api-key": this.claudeApiKey,
          "anthropic-version": "2023-06-01",
        },
        body: JSON.stringify({
          model: this.model.startsWith("claude") ? this.model : "claude-haiku-4-5-20251001",
          max_tokens: 220,
          system: this._systemPrompt,
          messages: [
            {
              role: "user",
              content: `Comment: ${JSON.stringify(text)}\nIntent: ${String(intent || "unknown")}\nSentiment: ${String(sentiment || "unknown")}`,
            },
          ],
        }),
        signal: controller.signal,
      });

      const data = await resp.json().catch(() => null);
      const content = data?.content?.[0]?.text || "";
      const parsed = safeJsonFromText(content);
      const normalized = this.#normalizeResult(parsed, "claude");
      return normalized || fallbackReply({ intent: String(intent || "other"), commentText: text });
    } finally {
      clearTimeout(t);
    }
  }
}

module.exports = {
  AiReplier,
};