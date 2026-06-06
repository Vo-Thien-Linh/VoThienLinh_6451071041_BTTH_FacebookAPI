"use strict";

const sql = require("mssql");

function isRunningInDocker() {
  try {
    return require("fs").existsSync("/.dockerenv");
  } catch {
    return false;
  }
}

function parseServer(value) {
  const raw = String(value || "").trim();
  if (!raw) return { server: null, instanceName: null };
  const parts = raw.split(/\\+/);
  if (parts.length < 2) return { server: raw, instanceName: null };
  return { server: parts[0], instanceName: parts.slice(1).join("\\") };
}

const parsedServer = parseServer(process.env.DB_HOST);
if (!isRunningInDocker() && parsedServer.server === "host.docker.internal") {
  parsedServer.server = "127.0.0.1";
}

const config = {
  server:   parsedServer.server,
  database: process.env.DB_NAME,
  user:     process.env.DB_USER || undefined,
  password: process.env.DB_PASSWORD || undefined,
  port:     process.env.DB_PORT ? Number(process.env.DB_PORT) : undefined,
  options: {
    trustedConnection:      !process.env.DB_USER,
    trustServerCertificate: true,
    encrypt:                false,
    instanceName:           parsedServer.instanceName || undefined,
  },
};

let pool = null;

async function getPool() {
  if (!config.server || !config.database) {
    throw new Error("SQL Server is not configured. Set DB_HOST and DB_NAME.");
  }

  if (!pool) {
    pool = await sql.connect(config);
    console.log("[DB] connected to SQL Server");
  }
  return pool;
}

async function closePool() {
  if (pool) {
    await pool.close();
    pool = null;
  }
}

module.exports = { getPool, closePool, sql };
