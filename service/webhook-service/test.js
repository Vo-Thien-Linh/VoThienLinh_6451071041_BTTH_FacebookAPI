// check-env.js
require('dotenv').config();

const checks = {
  APP_SECRET:              process.env.APP_SECRET,
  ACCESS_TOKEN:            process.env.ACCESS_TOKEN,
  ENABLE_FACEBOOK_ACTIONS: process.env.ENABLE_FACEBOOK_ACTIONS || 'true (default)',
  AI_PROVIDER:             process.env.AI_PROVIDER || 'openai (default)',
  OPENAI_API_KEY:          process.env.OPENAI_API_KEY ? '✅ set' : '❌ missing',
  GEMINI_API_KEY:          process.env.GEMINI_API_KEY ? '✅ set' : '❌ missing',
  CLAUDE_API_KEY:          process.env.CLAUDE_API_KEY ? '✅ set' : '❌ missing',
  KAFKA_BROKER:            process.env.KAFKA_BROKER || 'localhost:19092 (default)',
  FB_GRAPH_VERSION:        process.env.FB_GRAPH_VERSION || 'v23.0 (default)',
};

for (const [k, v] of Object.entries(checks)) {
  const display = k.includes('TOKEN') || k.includes('SECRET')
    ? (v ? `✅ set (${String(v).length} chars)` : '❌ missing')
    : v;
  console.log(`${k.padEnd(30)} ${display}`);
}