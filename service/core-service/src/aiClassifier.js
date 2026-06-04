"use strict";

function clamp01(n) {
  const x = Number(n);
  if (Number.isNaN(x)) return 0;
  return Math.max(0, Math.min(1, x));
}

const FEW_SHOT_EXAMPLES = [
  { text: "shop ơi giá bao nhiêu vậy", intent: "price_inquiry", sentiment: "neutral" },
  { text: "cho hỏi cái này giá mấy tiền", intent: "price_inquiry", sentiment: "neutral" },
  { text: "bao nhiêu tiền 1 cái vậy shop", intent: "price_inquiry", sentiment: "neutral" },
  { text: "giá đôi giày này là bao nhiêu ạ", intent: "price_inquiry", sentiment: "neutral" },
  { text: "có bán combo không shop, giá thế nào", intent: "price_inquiry", sentiment: "neutral" },
  { text: "áo thun có đắt không?", intent: "price_inquiry", sentiment: "neutral" },
  { text: "mẫu này mắc không shop", intent: "price_inquiry", sentiment: "neutral" },
  { text: "mình đặt 3 ngày rồi mà chưa thấy hàng đâu", intent: "complaint_support", sentiment: "negative" },
  { text: "hàng giao bị lỗi, shop giải quyết thế nào", intent: "complaint_support", sentiment: "negative" },
  { text: "cho mình đổi size được không", intent: "complaint_support", sentiment: "neutral" },
  { text: "sao mình chưa nhận được hàng vậy shop ơi", intent: "complaint_support", sentiment: "negative" },
  { text: "mình muốn trả hàng hoàn tiền", intent: "complaint_support", sentiment: "negative" },
  { text: "shop bán đẹp quá, mình thích lắm", intent: "praise_positive", sentiment: "positive" },
  { text: "hàng quality lắm, mình mua lần 3 rồi", intent: "praise_positive", sentiment: "positive" },
  { text: "bài viết hay quá shop ơi", intent: "praise_positive", sentiment: "positive" },
  { text: "cảm ơn shop nhiều nha, giao nhanh lắm", intent: "praise_positive", sentiment: "positive" },
  { text: "áo màu tím quá đẹp", intent: "praise_positive", sentiment: "positive" },
  { text: "áo đẹp quá shop ơi", intent: "praise_positive", sentiment: "positive" },
  { text: "màu này đẹp ghê", intent: "praise_positive", sentiment: "positive" },
  { text: "mẫu này cute lắm", intent: "praise_positive", sentiment: "positive" },
  { text: "shop có size XL không", intent: "product_inquiry", sentiment: "neutral" },
  { text: "cái này có màu đen không shop", intent: "product_inquiry", sentiment: "neutral" },
  { text: "chất liệu vải gì vậy shop", intent: "product_inquiry", sentiment: "neutral" },
  { text: "còn hàng không shop ơi", intent: "product_inquiry", sentiment: "neutral" },
  { text: "shop có bán áo polo không", intent: "product_inquiry", sentiment: "neutral" },
  { text: "tôi cần áo màu đỏ", intent: "product_inquiry", sentiment: "neutral" },
  { text: "ship về tỉnh được không", intent: "shipping_inquiry", sentiment: "neutral" },
  { text: "freeship không shop", intent: "shipping_inquiry", sentiment: "neutral" },
  { text: "giao hàng mất mấy ngày vậy", intent: "shipping_inquiry", sentiment: "neutral" },
  { text: "ok", intent: "other", sentiment: "neutral" },
  { text: "👍", intent: "other", sentiment: "positive" },
  { text: "haha", intent: "other", sentiment: "neutral" },
];

function buildFewShotBlock() {
  return FEW_SHOT_EXAMPLES.map(
    (e) => `Comment: "${e.text}" → intent: ${e.intent}, sentiment: ${e.sentiment}`
  ).join("\n");
}

const VALID_INTENTS = [
  "price_inquiry",
  "product_inquiry",
  "shipping_inquiry",
  "complaint_support",
  "praise_positive",
  "other",
];

const VALID_SENTIMENTS = ["positive", "neutral", "negative"];

function foldForMatch(s) {
  let out = String(s || "").toLowerCase();
  try {
    out = out.normalize("NFD");
  } catch {
    // ignore
  }
  out = out.replace(/[\u0300-\u036f]/g, "");
  out = out.replace(/đ/g, "d");
  out = out.replace(/\s+/g, " ").trim();
  return out;
}

function productQuestionOverride(text) {
  const s = String(text || "").toLowerCase();
  const sf = foldForMatch(s);

  const asksAvailability =
    /\?/.test(s) ||
    /\b(kh[oô]ng|ko|k)\b/i.test(s) ||
    /\b(khong|ko|k)\b/i.test(sf);

  const hasProductWord =
    /\b(qu[aầ]n|[aá]o|v[aá]y|gi[aà]y|t[uú]i|d[eé]p|b[oò]|\bjean\b|\bquần bò\b|\bquần jean\b)\b/i.test(s) ||
    /\b(quan|ao|vay|giay|tui|dep|bo|jean|quan bo|quan jean)\b/i.test(sf);

  const asksAttribute =
    /\b(m[aà]u|size|c[oò]n\s*h[aà]ng|ch[aấ]t\s*li[eệ]u|v[aả]i|m[aã]\s*s[aả]n\s*ph[aẩ]m)\b/i.test(s) ||
    /\b(mau|size|con hang|chat lieu|vai|ma san pham)\b/i.test(sf);

  const asksShopHas =
    /\bc[oó]\b[\s\S]{0,80}\b(kh[oô]ng|ko|k)\b/i.test(s) ||
    /\bco\b[\s\S]{0,80}\b(khong|ko|k)\b/i.test(sf);

  if (asksAvailability && (hasProductWord || asksAttribute || asksShopHas)) {
    return {
      intent: "product_inquiry",
      sentiment: "neutral",
      confidence: 0.8,
      reason: "product_or_attribute_question",
      provider: "rule",
    };
  }

  return null;
}

function complaintSupportOverride(text) {
  const s = String(text || "").toLowerCase();
  const sf = foldForMatch(s);

  const hasComplaint =
    /(chưa\s*nhận|không\s*nhận|bị\s*lỗi|lỗi|hỏng|giao\s*(trễ|chậm)|đổi\s*trả|hoàn\s*tiền|khiếu\s*nại|không\s*hài\s*lòng)/i.test(s) ||
    /(chua nhan|khong nhan|bi loi|\bloi\b|hong|giao (tre|cham)|doi tra|hoan tien|khieu nai|khong hai long)/i.test(sf);

  if (hasComplaint) {
    return {
      intent: "complaint_support",
      sentiment: "negative",
      confidence: 0.85,
      reason: "clear_complaint_or_support_request",
      provider: "rule",
    };
  }

  return null;
}

function fallbackClassify(text) {
  const override = productQuestionOverride(text);
  if (override) return override;

  const complaintOverride = complaintSupportOverride(text);
  if (complaintOverride) return complaintOverride;

  const s = String(text || "").toLowerCase();
  const sf = foldForMatch(s);

  const negativeFeedback = /(x[ấa]u|t[eệ]|d[oở]|d[ơo]m|k[eé]m|kh[oô]ng\s*[oô]n|kh[oô]ng\s*[đd][eẹ]p|th[âa]t\s*v[oọ]ng|t[ií]m\s*t[iế]m|ch[aá]n|t[uứ]c)/i;

  // Praise pattern phải check TRƯỚC product_inquiry vì product_inquiry có "màu" có thể false-match
  // Thứ tự object này quan trọng — praise được check trước product
  const patterns = {
    // Price inquiries can be explicit ("giá bao nhiêu") or implicit ("đắt/mắc/rẻ không").
    // IMPORTANT: avoid matching "đặt" (order) as "đắt" when users type without accents.
    price_inquiry:     /(gi[aá]\s*(bao\s*nhi[eê]u|ti[eê]n|c[aả])?|bao\s*nhi[eê]u\s*ti[eê]n|m[aâá]y\s*ti[eê]n|price|bao\s*gi[aá]|gi[aá]\s*(sao|th[eế]|n[aà]o|ntn)|\b(?:đắt|dat)\b\s*(?:kh[oô]ng|ko|k\?)(?:\b|$)|\b(?:mắc|mac)\b\s*(?:kh[oô]ng|ko|k\?)(?:\b|$)|\b(?:rẻ|re)\b\s*(?:kh[oô]ng|ko|k\?)(?:\b|$))/i,
    
    shipping_inquiry:  /(ship|giao\s*h[àa]ng|v[aậ]n\s*chuy[eể]n|freeship|ph[íi]\s*ship|m[aâấ]y\s*ng[àa]y)/i,
    // praise_positive TRƯỚC product_inquiry để "màu ... đẹp" không bị nhầm thành product
    praise_positive:   /(hay\s*qu[aá]|tuy[eê]t\s*v[oờ]i|[đd][eẹ]p\s*qu[aá]|[đd][eẹ]p\s*gh[eê]|[đd][eẹ]p\s*l[aắ]m|[đd][eẹ]p\s*qu[aá]|c[aả]m\s*[oơ]n|ch[aấ]t\s*l[uư][oợ]ng|love|th[ií]ch|[uư]ng\s*h[oộ]|cute|xinh|ưng|thích\s*qu[aá])/i,
    product_inquiry:   /(c[oó]\s*b[aá]n|b[aá]n\s*.*kh[oô]ng|size|c[oò]n\s*h[àa]ng|ch[aấ]t\s*li[eệ]u|v[aả]i|lo[aạ]i|m[aã]|m[aẫ]u\s*n[àa]o|c[oó]\s*m[àa]u|m[àa]u\s*(g[ìi]|n[àa]o|kh[aá]c))/i,
  complaint_support: /(ch[uư]a\s*nh[aậ]n|kh[oô]ng\s*nh[aậ]n|tr[eễ]|l[oỗ]i|h[oỏ]ng|khi[eê]u\s*n[aạ]i|[dđ][oổ]i\s*tr[aả]|ho[aà]n\s*ti[eề]n|h[oỗ]\s*tr[oợ]|x[ấa]u|t[eệ]|d[oở]|d[ơo]m|k[eé]m|kh[oô]ng\s*[oô]n|kh[oô]ng\s*[đd][eẹ]p|th[âa]t\s*v[oọ]ng)/i,
  };

  const patternsFolded = {
    price_inquiry: /(gia\s*(bao nhieu|tien|ca)?|bao nhieu tien|may tien|price|bao gia|gia\s*(sao|the nao|nao|ntn)|\b(dat|mac|re)\b\s*(khong|ko|k\?)(?:\b|$))/i,
    complaint_support: /(chua nhan|khong nhan|tre|loi|hong|khieu nai|doi tra|hoan tien|ho tro|xau|te|do|dom|kem|khong on|khong dep|that vong)/i,
    shipping_inquiry: /(ship|giao hang|giao|van chuyen|freeship|phi ship|may ngay)/i,
    praise_positive: /(hay qua|tuyet voi|dep qua|dep ghe|dep lam|cam on|chat luong|love|thich|ung ho|cute|xinh)/i,
    product_inquiry: /(co ban|ban .*khong|size|con hang|chat lieu|vai|loai|ma|mau nao|co mau|mau (gi|nao|khac))/i,
  };

  let intent = "other";
  let sentiment = "neutral";

  for (const [key, re] of Object.entries(patterns)) {
    const rf = patternsFolded[key];
    if (re.test(s) || (rf && rf.test(sf))) {
      intent = key;
      break;
    }
  }

  // Disambiguation: shipping questions often contain "giao"/"mấy ngày" and can also include
  // words like "đặt" (order) which should not be treated as price.
  if (intent === "price_inquiry" && (patterns.shipping_inquiry.test(s) || patternsFolded.shipping_inquiry.test(sf))) {
    const explicitPrice = /(gi[aá]|bao\s*nhi[eê]u\s*ti[eê]n|m[aâá]y\s*ti[eê]n|price|bao\s*gi[aá])/i.test(s) || /(gia|bao nhieu tien|may tien|price|bao gia)/i.test(sf);
    const implicitPrice = /\b(?:đắt|dat|mắc|mac|rẻ|re)\b\s*(?:kh[oô]ng|ko|k\?)/i.test(s) || /\b(dat|mac|re)\b\s*(khong|ko|k\?)/i.test(sf);
    if (!explicitPrice && !implicitPrice) {
      intent = "shipping_inquiry";
    }
  }

  if (patterns.complaint_support.test(s) || patternsFolded.complaint_support.test(sf)) sentiment = "negative";
  else if (patterns.praise_positive.test(s) || patternsFolded.praise_positive.test(sf)) sentiment = "positive";
  else if (negativeFeedback.test(s)) sentiment = "negative";

  if (intent === "other" && negativeFeedback.test(s)) {
    intent = "complaint_support";
  }

  // Purchase-intent statements without explicit question marks.
  // Example: "tôi cần áo màu đỏ" => product_inquiry.
  if (intent === "other") {
    const wantsToBuy = /\b(c[aâ]n|mu[oố]n|mua|[đd][aă]t)\b/i.test(s) || /\b(can|muon|mua|dat)\b/i.test(sf);
    const hasAttribute = /(m[aà]u|size|c[oò]n\s*h[aà]ng|ch[aấ]t\s*li[eệ]u|v[aả]i)/i.test(s) || /(mau|size|con hang|chat lieu|vai)/i.test(sf);
    if (wantsToBuy && hasAttribute) {
      intent = "product_inquiry";
    }
  }

  return { intent, sentiment, confidence: 0.35, provider: "fallback" };
}

function safeJsonFromText(s) {
  const str = String(s || "").trim()
    .replace(/^```json\s*/i, "")
    .replace(/^```\s*/i, "")
    .replace(/\s*```$/i, "")
    .trim();

  try { return JSON.parse(str); } catch { /* */ }
  const first = str.indexOf("{");
  const last = str.lastIndexOf("}");
  if (first >= 0 && last > first) {
    try { return JSON.parse(str.slice(first, last + 1)); } catch { /* */ }
  }
  return null;
}

function buildSystemPrompt() {
  return `Bạn là bộ phân loại comment tiếng Việt cho shop bán hàng online.
Nhiệm vụ: phân tích comment và trả về JSON hợp lệ, KHÔNG thêm chữ nào khác ngoài JSON.

Schema bắt buộc:
{"intent": string, "sentiment": string, "confidence": number, "reason": string}

intent phải là một trong: ${VALID_INTENTS.join(" | ")}
sentiment phải là một trong: positive | neutral | negative
confidence: số từ 0.0 đến 1.0
reason: giải thích ngắn 1 câu bằng tiếng Việt tại sao chọn intent này

Quy tắc ưu tiên:
1. complaint_support: bất kỳ dấu hiệu chưa nhận hàng, lỗi, đổi trả, hoàn tiền, khiếu nại
2. price_inquiry: hỏi giá, bao nhiêu tiền, mấy tiền
3. shipping_inquiry: hỏi ship, giao hàng, freeship
4. praise_positive: khen ngợi, bày tỏ cảm xúc tích cực, nhận xét đẹp/thích/cute — KỂ CẢ khi có nhắc màu sắc ("màu tím đẹp", "màu này xinh")
5. product_inquiry: HỎI về size, màu, còn hàng, chất liệu — chỉ khi CÓ câu hỏi (có dấu "?", "không", "có ... không")
6. other: không rõ ý định

QUAN TRỌNG: "áo màu tím quá đẹp", "màu này đẹp ghê", "mẫu cute lắm" → praise_positive, KHÔNG phải product_inquiry.
product_inquiry chỉ dùng khi người dùng HỎI (ví dụ: "có màu đen không?", "còn size M không?").

Lưu ý: người Việt hay gõ thiếu dấu (telex), ví dụ "chua nhan hang" = "chưa nhận hàng".

Ví dụ tham khảo:
${buildFewShotBlock()}`;
}

class AiClassifier {
  constructor({ provider, openaiApiKey, geminiApiKey, claudeApiKey, model, timeoutMs }) {
    this.provider = provider || "openai";
    this.openaiApiKey = openaiApiKey || "";
    this.geminiApiKey = geminiApiKey || "";
    this.claudeApiKey = claudeApiKey || "";
    this.model = model || "gpt-4o-mini";
    const raw = Number(timeoutMs);
    this.timeoutMs = Number.isFinite(raw) ? Math.max(250, raw) : 8000;
    this._systemPrompt = buildSystemPrompt();
  }

  isEnabled() {
    if (String(process.env.AI_DISABLED || "").toLowerCase() === "true") return false;
    if (this.provider === "openai") return !!this.openaiApiKey;
    if (this.provider === "gemini") return !!this.geminiApiKey;
    if (this.provider === "claude") return !!this.claudeApiKey;
    return false;
  }

  async classify({ text }) {
    const override = productQuestionOverride(text);
    if (override) return override;

    const complaintOverride = complaintSupportOverride(text);
    if (complaintOverride) return complaintOverride;

    if (!this.isEnabled()) {
      console.debug("[AiClassifier] not configured, using fallback");
      return fallbackClassify(text);
    }

    try {
      let result;
      if (this.provider === "openai") result = await this.#classifyOpenAI(text);
      else if (this.provider === "gemini") result = await this.#classifyGemini(text);
      else if (this.provider === "claude") result = await this.#classifyClaude(text);
      else result = fallbackClassify(text);

      return result;
    } catch (err) {
      console.warn(`[AiClassifier] ${this.provider} failed, using fallback:`, err?.message);
      return fallbackClassify(text);
    }
  }

  _validateAndNormalize(parsed, providerName) {
    if (!parsed) return null;

    const intent = VALID_INTENTS.includes(parsed.intent) ? parsed.intent : "other";
    const sentiment = VALID_SENTIMENTS.includes(parsed.sentiment) ? parsed.sentiment : "neutral";
    const confidence = clamp01(parsed.confidence);
    const reason = typeof parsed.reason === "string" ? parsed.reason.slice(0, 200) : "";

    return { intent, sentiment, confidence, reason, provider: providerName };
  }

  async #classifyOpenAI(text) {
    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), this.timeoutMs);

    const MAX_RETRIES = 2;
    let lastErr;

    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      try {
        const resp = await fetch("https://api.openai.com/v1/chat/completions", {
          method: "POST",
          headers: {
            "content-type": "application/json",
            authorization: `Bearer ${this.openaiApiKey}`,
          },
          body: JSON.stringify({
            model: this.model,
            temperature: 0,
            max_tokens: 150,
            response_format: { type: "json_object" },
            messages: [
              { role: "system", content: this._systemPrompt },
              { role: "user", content: String(text || "").slice(0, 500) },
            ],
          }),
          signal: controller.signal,
        });

        if (resp.status === 429 || resp.status >= 500) {
          if (attempt < MAX_RETRIES) {
            await new Promise((r) => setTimeout(r, 800 * attempt));
            continue;
          }
          return fallbackClassify(text);
        }

        const data = await resp.json().catch(() => null);
        const content = data?.choices?.[0]?.message?.content || "";
        const parsed = safeJsonFromText(content);
        const result = this._validateAndNormalize(parsed, "openai");
        return result || fallbackClassify(text);
      } catch (err) {
        lastErr = err;
        if (attempt < MAX_RETRIES && err?.name !== "AbortError") {
          await new Promise((r) => setTimeout(r, 500 * attempt));
          continue;
        }
        throw err;
      } finally {
        if (attempt >= MAX_RETRIES) clearTimeout(t);
      }
    }

    clearTimeout(t);
    throw lastErr;
  }

  async #classifyGemini(text) {
    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), this.timeoutMs);

    try {
      const model = this.model.startsWith("gemini") ? this.model : "gemini-2.5-flash";
      const url = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${this.geminiApiKey}`;

      const resp = await fetch(url, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          contents: [
            {
              parts: [
                { text: this._systemPrompt + "\n\nComment cần phân tích: " + String(text || "").slice(0, 500) },
              ],
            },
          ],
          generationConfig: { temperature: 0, maxOutputTokens: 300 },
        }),
        signal: controller.signal,
      });

      const data = await resp.json().catch(() => null);
      const content = data?.candidates?.[0]?.content?.parts?.[0]?.text || "";
      const parsed = safeJsonFromText(content);
      return this._validateAndNormalize(parsed, "gemini") || fallbackClassify(text);
    } finally {
      clearTimeout(t);
    }
  }

  async #classifyClaude(text) {
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
          model: "claude-haiku-4-5-20251001",
          max_tokens: 150,
          system: this._systemPrompt,
          messages: [
            { role: "user", content: String(text || "").slice(0, 500) },
          ],
        }),
        signal: controller.signal,
      });

      const data = await resp.json().catch(() => null);
      const content = data?.content?.[0]?.text || "";
      const parsed = safeJsonFromText(content);
      return this._validateAndNormalize(parsed, "claude") || fallbackClassify(text);
    } finally {
      clearTimeout(t);
    }
  }
}

module.exports = { AiClassifier, fallbackClassify, VALID_INTENTS, FEW_SHOT_EXAMPLES };
