// Config loader: JSON file + env overrides. Env vars always win.
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';

const ROOT = path.resolve(import.meta.dirname, '..');
const CONFIG_PATH = process.env.FREEBUFF_CONFIG || path.join(ROOT, 'config.json');

function manicodeDir() {
  return process.env.MANICODE_CREDS_DIR || path.join(os.homedir(), '.config', 'manicode');
}
export { manicodeDir };

const defaults = {
  LISTEN_ADDR: '127.0.0.1:8090',
  UPSTREAM_BASE_URL: 'https://www.codebuff.com', // apex 307s to www and drops Authorization mid-hop
  AUTH_TOKENS: [],
  API_KEYS: [],
  PROXY_URL: '',
  REQUEST_TIMEOUT_MS: 900_000,
  ROTATION_INTERVAL_MIN: 360,
  DEBOUNCE_MS: 1100,
  // How long a request will wait in the free-tier waiting room (polling the
  // queue) before giving up. Must stay well under REQUEST_TIMEOUT_MS so a
  // genuinely stuck queue still fails fast instead of holding the socket.
  WAITING_ROOM_MAX_WAIT_MS: 120_000,
  // Earned-pool reroute targets (read-only legacy keys; see openai.js fallback).
  // DeepSeek V4 Flash moved onto the premium pool upstream 2026-08-18; the
  // always-available unlimited standby is now MiMo 2.5.
  POOL_FALLBACK_MODEL: 'mimo/mimo-v2.5',
  POOL_FALLBACK_ELIGIBLE: ['mimo/mimo-v2.5', 'deepseek/deepseek-v4-flash'],
};

function loadFile() {
  try {
    if (fs.existsSync(CONFIG_PATH)) {
      return JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
    }
  } catch (e) {
    console.error(`[config] failed to parse ${CONFIG_PATH}: ${e.message}`);
  }
  return {};
}

const file = loadFile();
const env = {};

if (process.env.LISTEN_ADDR) env.LISTEN_ADDR = process.env.LISTEN_ADDR;
if (process.env.UPSTREAM_BASE_URL) env.UPSTREAM_BASE_URL = process.env.UPSTREAM_BASE_URL;
if (process.env.AUTH_TOKENS) {
  env.AUTH_TOKENS = process.env.AUTH_TOKENS.includes(',')
    ? process.env.AUTH_TOKENS.split(',').map((s) => s.trim()).filter(Boolean)
    : [process.env.AUTH_TOKENS.trim()];
}
if (process.env.API_KEYS) {
  env.API_KEYS = process.env.API_KEYS.split(',').map((s) => s.trim()).filter(Boolean);
}
if (process.env.PROXY_URL !== undefined && process.env.PROXY_URL !== '') {
  env.PROXY_URL = process.env.PROXY_URL;
}
if (process.env.WAITING_ROOM_MAX_WAIT_MS !== undefined && process.env.WAITING_ROOM_MAX_WAIT_MS !== '') {
  const v = Number(process.env.WAITING_ROOM_MAX_WAIT_MS);
  if (Number.isFinite(v) && v > 0) env.WAITING_ROOM_MAX_WAIT_MS = v;
}
if (process.env.NO_PROXY_MODE === '1' || String(process.env.NO_PROXY_MODE).toLowerCase() === 'true') {
  env.PROXY_URL = '';
}

export const config = { ...defaults, ...file, ...env };

// Type-normalize list-valued keys: a plain string in config.json must become
// [string], never char-split. Warn loudly on nonsense shapes.
for (const key of ['AUTH_TOKENS', 'API_KEYS']) {
  const v = config[key];
  if (typeof v === 'string') {
    config[key] = v.split(',').map((s) => s.trim()).filter(Boolean);
    if (!env[key]) console.warn(`[config] ${key} was a string in config.json — coerced to an array`);
  } else if (v !== undefined && !Array.isArray(v)) {
    console.warn(`[config] ${key} has invalid type ${typeof v}; ignoring`);
    config[key] = [];
  } else if (Array.isArray(v)) {
    config[key] = v.filter((x) => typeof x === 'string' && x.trim()).map((x) => x.trim());
  }
}
if (typeof config.PROXY_URL !== 'string') {
  console.warn('[config] PROXY_URL has invalid type; ignoring');
  config.PROXY_URL = '';
}

// Auto-detect the locally installed Freebuff CLI's auth token when none provided.
let cachedCliToken; // undefined until first resolve — memoized, not re-read per call
export function detectCliToken() {
  if (cachedCliToken !== undefined) return cachedCliToken;
  const dir = manicodeDir();
  const candidates = [
    path.join(dir, 'credentials.json'),
    path.join(os.homedir(), '.config', 'codebuff', 'credentials.json'),
  ];
  for (const p of candidates) {
    try {
      const cred = JSON.parse(fs.readFileSync(p, 'utf8'));
      const tok = cred?.default?.authToken;
      if (typeof tok === 'string' && tok.length > 10) {
        cachedCliToken = tok;
        return tok;
      }
    } catch (e) {
      console.debug(`[config] no CLI credentials at ${p}: ${e.message}`);
    }
  }
  cachedCliToken = null;
  return null;
}

export function resolveTokens() {
  const tokens = [...config.AUTH_TOKENS];
  if (config.AUTH_TOKENS_OVERRIDE_CLI) return tokens; // dashboard removed the CLI token
  const cli = detectCliToken();
  if (cli && !tokens.includes(cli)) tokens.push(cli);
  return tokens;
}

/** Persist a partial patch into config.json (and the live config object). */
export function saveConfig(patch) {
  const file = loadFile();
  for (const [k, v] of Object.entries(patch)) {
    file[k] = v;
    config[k] = v;
  }
  fs.writeFileSync(CONFIG_PATH, `${JSON.stringify(file, null, 2)}\n`);
  return true;
}

export function configFileStatus() {
  try {
    const mtime = fs.statSync(CONFIG_PATH).mtimeMs;
    return { path: CONFIG_PATH, writable: true, mtime };
  } catch {
    return { path: CONFIG_PATH, writable: false };
  }
}

export const ROOT_DIR = ROOT;
