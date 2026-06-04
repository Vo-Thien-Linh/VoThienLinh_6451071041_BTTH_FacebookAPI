"use strict";

const crypto = require("crypto");

function foldForMatch(s) {
  let out = String(s || "").toLowerCase();
  try {
    // NFD splits base letters and diacritics so we can strip combining marks.
    out = out.normalize("NFD");
  } catch {
    // ignore
  }
  // Strip combining diacritics (covers most Vietnamese tone/mark combos)
  out = out.replace(/[\u0300-\u036f]/g, "");
  // Special-case Vietnamese đ
  out = out.replace(/đ/g, "d");
  out = out.replace(/\s+/g, " ").trim();
  return out;
}

function isLowValuePing(normalized) {
  const s = foldForMatch(normalized);
  if (!s) return false;
  if (s.length > 32) return false;

  // Common short pings that should not be treated as spam when repeated a few times.
  // High-rate protection still applies separately.
  const patterns = [
    /^shop\s*(ơi|oi)?\s*(ạ|a)?[.!?]*$/i,
    /^ad\s*(ơi|oi)?\s*(ạ|a)?[.!?]*$/i,
    /^admin\s*(ơi|oi)?\s*(ạ|a)?[.!?]*$/i,
    /^ch(ị|i)\s*(ơi|oi)?\s*(ạ|a)?[.!?]*$/i,
    /^anh\s*(ơi|oi)?\s*(ạ|a)?[.!?]*$/i,
    /^em\s*(ơi|oi)?\s*(ạ|a)?[.!?]*$/i,
  ];

  return patterns.some((re) => re.test(s));
}

function normalizeText(text) {
  return String(text || "")
    .toLowerCase()
    .normalize("NFC")
    .replace(/\s+/g, " ")
    .replace(/[\u200B-\u200D\uFEFF]/g, "")
    .trim();
}

function textHash(text) {
  return crypto.createHash("sha256").update(text, "utf8").digest("hex");
}

function extractLinks(text) {
  const s = String(text || "");
  const re = /(https?:\/\/\S+|www\.[^\s]+|\b\w+\.(com|net|org|xyz|top|shop|vn|io|co)\b[^\s]*)/gi;
  const matches = s.match(re);
  return matches ? matches.slice(0, 10) : [];
}

function tryGetHostname(urlLike) {
  const raw = String(urlLike || "").trim();
  if (!raw) return null;
  try {
    const withProto = raw.startsWith("http://") || raw.startsWith("https://") ? raw : `https://${raw}`;
    const u = new URL(withProto);
    return (u.hostname || "").toLowerCase();
  } catch {
    return null;
  }
}

function detectSpam({ text, actorId, now, userStore }) {
  const normalized = normalizeText(text);
  const links = extractLinks(normalized);

  const treatLinkAsSpamLight = String(process.env.SPAM_TREAT_LINK_AS_LIGHT || "true").toLowerCase() !== "false";

  const rateWindowMs = Number(process.env.COMMENT_RATE_WINDOW_MS || 60_000);
  const rateThreshold = Number(process.env.COMMENT_RATE_THRESHOLD || 3);

  const maliciousDomains = (process.env.MALICIOUS_DOMAINS || "")
    .split(",")
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean);

  const hostnames = links.map(tryGetHostname).filter(Boolean);
  const isMaliciousLink = hostnames.some((h) => maliciousDomains.includes(h) || maliciousDomains.some((d) => h === d || h.endsWith(`.${d}`)));

  // Basic bot-like heuristics (minimal): very long repeated characters or many links.
  const isBotLikely = links.length >= 3 || /(.)\1{12,}/.test(normalized);

  const hasLink = links.length > 0;

  const hash = textHash(normalized);
  const repeatSpamCount24h = userStore.recordAndCountRepeat(actorId, hash, now);

  const commentRateCount = userStore.recordAndCountCommentRate(actorId, now, rateWindowMs);
  const isHighRate = Number.isFinite(rateThreshold) ? commentRateCount >= rateThreshold : false;

  const ignoreRepeatAsSpam = isLowValuePing(normalized);
  const repeatSpamCount24hEffective = ignoreRepeatAsSpam ? 0 : repeatSpamCount24h;
  const isSpamLight = (treatLinkAsSpamLight && hasLink) || repeatSpamCount24hEffective >= 2 || isHighRate;

  return {
    normalized,
    textHash: hash,
    links,
    hostnames,
    hasLink,
    commentRateCount,
    rateWindowMs,
    isHighRate,
    repeatSpamCount24h,
    repeatSpamCount24hEffective,
    isMaliciousLink,
    isBotLikely,
    isSpamLight,
    reasons: [
      hasLink ? "contains_link" : null,
      isHighRate ? "high_rate" : null,
      repeatSpamCount24hEffective >= 2 ? "repeat_content" : null,
      isMaliciousLink ? "malicious_domain" : null,
      isBotLikely ? "bot_like" : null,
    ].filter(Boolean),
  };
}

module.exports = {
  detectSpam,
};
