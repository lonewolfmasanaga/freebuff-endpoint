// Live model registry: syncs the agent<->model mapping from Freebuff's open
// source (FREE_MODE_AGENT_MODELS in free-agents.ts), resolving TS constants
// through their full definition chains (string literals, multi-line literals,
// object-member references), excluding withdrawn (paused) models.
// Sync traffic follows the configured PROXY_URL dispatcher (egress-gated
// networks need it), the last good map is persisted to disk across restarts,
// and FALLBACK_MODELS is only the final resort.
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { request as undiciRequest } from 'undici';
import { getDispatcher } from './http-client.js';

const SOURCES = {
  agents: [
    'https://raw.githubusercontent.com/CodebuffAI/freebuff/main/common/src/constants/free-agents.ts',
    'https://raw.githubusercontent.com/CodebuffAI/codebuff/main/common/src/constants/free-agents.ts',
  ],
  aux: [
    'https://raw.githubusercontent.com/CodebuffAI/freebuff/main/common/src/constants/freebuff-models.ts',
    'https://raw.githubusercontent.com/CodebuffAI/freebuff/main/common/src/constants/freebuff-model-ids.ts',
    'https://raw.githubusercontent.com/CodebuffAI/freebuff/main/common/src/constants/model-config.ts',
    'https://raw.githubusercontent.com/CodebuffAI/freebuff/main/common/src/constants/gemini.ts',
    'https://raw.githubusercontent.com/CodebuffAI/freebuff/main/common/src/constants/freebuff-gemini-thinker.ts',
  ],
};

// Fallback verified against the Aug 2026 snapshot of free-agents.ts.
const FALLBACK_MODELS = {
  'deepseek/deepseek-v4-flash': 'base2-free-deepseek-flash',
  'deepseek/deepseek-v4-pro': 'base2-free-deepseek',
  'minimax/minimax-m2.7': 'base2-free',
  'openai/gpt-5.6-luna': 'base2-free-luna',
  'mimo/mimo-v2.5': 'base2-free-mimo',
  'z-ai/glm-5.2': 'base2-free-glm',
  'crof/kimi-k3-eco': 'base2-free-kimi-k3-eco',
  'google/gemini-2.5-flash-lite': 'file-picker',
};

const CACHE_FILE = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'data', 'model-cache.json');

function loadDiskCache(log) {
  try {
    const parsed = JSON.parse(fs.readFileSync(CACHE_FILE, 'utf8'));
    if (parsed && typeof parsed === 'object' && Object.keys(parsed.modelToAgent || {}).length) {
      log.info(`registry seeded from disk cache (${Object.keys(parsed.modelToAgent).length} models, synced ${parsed.savedAt || 'unknown'})`);
      return parsed.modelToAgent;
    }
  } catch { /* first run or unreadable — fall through */ }
  return null;
}

function saveDiskCache(modelToAgent, log) {
  try {
    fs.mkdirSync(path.dirname(CACHE_FILE), { recursive: true });
    fs.writeFileSync(CACHE_FILE, JSON.stringify({ savedAt: new Date().toISOString(), modelToAgent }, null, 2));
  } catch (e) {
    log.warn(`registry disk cache write failed: ${e.message}`);
  }
}

export class ModelRegistry {
  constructor(logger) {
    this.log = logger;
    const cached = loadDiskCache(logger);
    this.modelToAgent = { ...FALLBACK_MODELS, ...(cached || {}) };
    this.fromDisk = !!cached;
    this.lastSync = cached ? null : null;
    this.lastError = null;
    this.syncFailures = 0;
    this.timer = null;
    this.refreshing = false;
  }

  start(intervalMin) {
    this.refresh();
    this.timer = setInterval(() => this.refresh(), intervalMin * 60_000);
    this.timer.unref?.();
  }

  stop() {
    if (this.timer) clearInterval(this.timer);
  }

  async refresh() {
    if (this.refreshing) return;
    this.refreshing = true;
    try {
      const [agentsSrc, auxParts] = await Promise.all([
        fetchText(SOURCES.agents),
        Promise.allSettled(SOURCES.aux.map((u) => fetchText([u]))).then((rs) =>
          rs.map((r) => (r.status === 'fulfilled' ? r.value : '')),
        ),
      ]);
      const auxSrc = auxParts.join('\n');
      // The paused (withdrawn) filter is applied when the list exists. Upstream
      // may move or retire the mechanism (withdrawal is enforced at admission
      // regardless); a missing list is only fatal when the fetch itself was
      // incomplete — otherwise we'd refuse every future sync forever.
      const failedAux = auxParts.filter((p) => p === '').length;
      const combined = `${agentsSrc}\n${auxSrc}`;
      const hasPausedList = /FREEBUFF_PAUSED_FREE_MODEL_IDS\s*(?::[^=]+)?=\s*\[/.test(stripComments(combined));
      if (!hasPausedList && failedAux > 0) {
        throw new Error(`paused-model list missing AND ${failedAux} aux source(s) failed — refusing possibly-unfiltered sync`);
      }
      const parsed = extractFreeModeAgentModels(agentsSrc, auxSrc);
      if (!Object.keys(parsed).length) throw new Error('no models parsed from source');
      this.modelToAgent = parsed;
      this.fromDisk = false;
      this.lastSync = new Date().toISOString();
      this.lastError = null;
      this.syncFailures = 0;
      saveDiskCache(parsed, this.log);
      this.log.info(`registry synced: ${Object.keys(parsed).length} models`);
    } catch (e) {
      this.syncFailures++;
      this.lastError = e.message;
      this.log.warn(`registry refresh failed (${e.message}); keeping current mapping`);
    } finally {
      this.refreshing = false;
    }
  }

  models() {
    return Object.keys(this.modelToAgent).sort();
  }

  has(model) {
    return !!this.modelToAgent[model];
  }

  agentForModel(model) {
    return this.modelToAgent[model] || null;
  }

  status() {
    return {
      models: this.models(),
      last_sync: this.lastSync,
      last_error: this.lastError,
      sync_failures: this.syncFailures,
      seeded_from_disk: this.fromDisk,
    };
  }
}

// ---------------------------------------------------------------------------
// Parsing

function stripComments(src) {
  return src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');
}

/**
 * Resolve every ALL_CAPS constant to its string value across all sources.
 * Handles single-, double-, and backtick literals plus tableName.member
 * references (resolved in a second pass so definition order never matters).
 */
export function buildConstTable(sources) {
  const consts = {};
  const objTables = {};
  const memberRefs = [];

  for (const raw of sources) {
    const src = stripComments(raw || '');

    // Object tables: export const name = { KEY: 'value', ... } as const
    const objRe = /export\s+const\s+([a-zA-Z0-9_]+)\s*(?::[^=]+)?=\s*\{([^{}]*)\}/g;
    for (const m of src.matchAll(objRe)) {
      const table = {};
      for (const kv of m[2].matchAll(/'?([A-Za-z0-9_]+)'?\s*:\s*(?:'((?:[^'\\]|\\.)*)'|"((?:[^"\\]|\\.)*)")/g)) {
        table[kv[1]] = kv[2] ?? kv[3] ?? ''; // inner captures already exclude the quotes
      }
      if (Object.keys(table).length) objTables[m[1]] = { ...(objTables[m[1]] || {}), ...table };
    }

    // Plain string constants in all quote styles (handles multi-line `=\n  'v'`)
    const literalRes = [
      /export\s+const\s+([A-Z0-9_]+)\s*(?::[^=]+)?=\s*'((?:[^'\\]|\\.)*)'/g,
      /export\s+const\s+([A-Z0-9_]+)\s*(?::[^=]+)?=\s*"((?:[^"\\]|\\.)*)"/g,
      /export\s+const\s+([A-Z0-9_]+)\s*(?::[^=]+)?=\s*`((?:[^`\\]|\\.)*)`/g,
    ];
    for (const re of literalRes) {
      re.lastIndex = 0;
      for (const m of src.matchAll(re)) consts[m[1]] = m[2];
    }

    // Member references: export const NAME = tableName.memberKey (resolve later)
    const memRe = /export\s+const\s+([A-Z0-9_]+)\s*(?::[^=]+)?=\s*([a-zA-Z][a-zA-Z0-9_]*)\.([A-Za-z0-9_]+)/g;
    for (const m of src.matchAll(memRe)) memberRefs.push(m);
  }

  // Second pass: every table from every source is visible by now.
  for (const [, name, table, key] of memberRefs) {
    const v = objTables[table]?.[key];
    if (typeof v === 'string') consts[name] = v;
  }
  return consts;
}

/**
 * Extract FREE_MODE_AGENT_MODELS into { modelId: agentId }, resolving constants
 * and inline helper sets, dropping paused models and non-root agents.
 * Entry values are segmented with a depth-aware splitter (never a lazy
 * lookahead regex), so inline Sets with many elements survive intact.
 */
export function extractFreeModeAgentModels(agentsSource, auxSources) {
  const src = stripComments(agentsSource);
  const consts = buildConstTable([auxSources || '', agentsSource]);

  const objStart = src.search(/FREE_MODE_AGENT_MODELS\s*(?::[^=]+)?=\s*\{/);
  if (objStart === -1) throw new Error('FREE_MODE_AGENT_MODELS not found');
  const body = matchBraces(src, src.indexOf('{', objStart));
  if (!body) throw new Error('unbalanced braces in FREE_MODE_AGENT_MODELS');

  // Paused (withdrawn) models — searched across aux AND agents sources; list
  // entries may themselves be constants.
  let paused = new Set();
  const pauseMatch = stripComments(`${auxSources || ''}\n${agentsSource}`).match(
    /FREEBUFF_PAUSED_FREE_MODEL_IDS\s*(?::[^=]+)?=\s*\[([^\]]*)\]/,
  );
  if (pauseMatch) {
    for (const item of pauseMatch[1].matchAll(/'([^']+)'|([A-Z0-9_]{4,})/g)) {
      const val = item[1] || consts[item[2]];
      if (val) paused.add(val);
    }
  }

  // Named set references defined anywhere (GEMINI_HELPER_MODELS…)
  const namedSets = {};
  const setRe = /([A-Z0-9_]+)\s*(?::[^=]+)?=\s*new\s+Set\s*\(\s*\[/g;
  for (const m of src.matchAll(setRe)) namedSets[m[1]] = balancedBracketBody(src, src.indexOf('[', m.index));
  for (const m of stripComments(auxSources || '').matchAll(setRe)) {
    namedSets[m[1]] ||= balancedBracketBody(stripComments(auxSources || ''), stripComments(auxSources || '').indexOf('[', m.index));
  }

  const result = {};
  for (const entry of splitTopLevel(body)) {
    const keyMatch = entry.match(/^\s*(?:'([^']+)'|\[([A-Z0-9_]+)\])\s*:\s*([\s\S]*)$/);
    if (!keyMatch) continue;
    const agentId = keyMatch[1] || consts[keyMatch[2]];
    if (!agentId) continue;
    if (/freebuff-desktop/i.test(agentId)) continue; // desktop roots need their harness marker
    const valueSrc = keyMatch[3].trim();
    if (valueSrc.startsWith('...')) continue; // derived spreads (same models under other roots)
    let inner = null;
    const inlineSet = valueSrc.match(/new\s+Set\s*\(\s*\[/);
    if (inlineSet) inner = balancedBracketBody(valueSrc, valueSrc.indexOf('[', inlineSet.index));
    else {
      const ident = valueSrc.match(/^([A-Z0-9_]+)$/);
      if (ident && namedSets[ident[1]] !== undefined) inner = namedSets[ident[1]];
    }
    if (inner == null) continue;

    for (const item of inner.matchAll(/'([^']+)'|([A-Z0-9_]{4,})/g)) {
      const model = item[1] || consts[item[2]];
      if (!model || !model.includes('/')) continue; // unresolved or non-model entry
      if (paused.has(model)) continue; // withdrawn from free mode
      if (!(model in result) || preferAgent(agentId, result[model])) result[model] = agentId;
    }
  }
  return result;
}

function preferAgent(candidate, incumbent) {
  // Dedicated per-model CLI roots win; the generic multi-model 'base2-free'
  // root is a fallback; helper/subagent roots lose outright.
  const HELPER = /^(file-|researcher|basher$|browser-use|tmux-cli|code-reviewer|cloud-planner)/;
  const score = (id) =>
    (id.startsWith('base') ? 2 : 0) +
    (id.startsWith('base2-free-') && id !== 'base2-free' ? 3 : 0) +
    (id === 'base2-free' ? 1 : 0) -
    (HELPER.test(id) ? 20 : 0) -
    (id.includes('-max') || id.includes('-es') ? 1 : 0);
  return score(candidate) > score(incumbent);
}

/** Return the substring between the braces that open at `openIdx`, balanced. */
function matchBraces(src, openIdx) {
  let depth = 0;
  for (let i = openIdx; i < src.length; i++) {
    const ch = src[i];
    if (ch === "'" || ch === '"' || ch === '`') i = skipString(src, i);
    else if (ch === '{') depth++;
    else if (ch === '}') {
      depth--;
      if (depth === 0) return src.slice(openIdx + 1, i);
    }
  }
  return null;
}

/** Body between the '[' at openIdx and its matching ']' (nesting- and quote-aware). */
function balancedBracketBody(src, openIdx) {
  let depth = 0;
  for (let i = openIdx; i < src.length; i++) {
    const ch = src[i];
    if (ch === "'" || ch === '"' || ch === '`') i = skipString(src, i);
    else if (ch === '[') depth++;
    else if (ch === ']') {
      depth--;
      if (depth === 0) return src.slice(openIdx + 1, i);
    }
  }
  return null;
}

/** Index of the closing quote for the string opening at `openIdx`. */
function skipString(src, openIdx) {
  const q = src[openIdx];
  for (let i = openIdx + 1; i < src.length; i++) {
    if (src[i] === '\\') i++;
    else if (src[i] === q) return i;
  }
  return src.length - 1;
}

/** Split on commas that sit outside any (), [], {}, or string literal. */
function splitTopLevel(body) {
  const parts = [];
  let depth = 0;
  let cur = '';
  for (let i = 0; i < body.length; i++) {
    const ch = body[i];
    if (ch === "'" || ch === '"' || ch === '`') {
      const end = skipString(body, i);
      cur += body.slice(i, end + 1);
      i = end;
      continue;
    }
    if (ch === '(' || ch === '[' || ch === '{') depth++;
    else if (ch === ')' || ch === ']' || ch === '}') depth--;
    if (ch === ',' && depth === 0) {
      parts.push(cur);
      cur = '';
      continue;
    }
    cur += ch;
  }
  if (cur.trim()) parts.push(cur);
  return parts;
}

/** Fetch each URL in order through the configured dispatcher (proxy-aware),
 *  with one retry per URL to ride out GitHub CDN blips (503 max_conn). */
async function fetchText(urls) {
  let lastErr;
  for (const url of urls) {
    for (let attempt = 0; attempt < 2; attempt++) {
      try {
        const res = await undiciRequest(url, {
          dispatcher: getDispatcher(),
          headersTimeout: 20_000,
          bodyTimeout: 20_000,
        });
        if (res.statusCode >= 400) {
          res.body.destroy()?.catch?.(() => {});
          throw new Error(`HTTP ${res.statusCode}`);
        }
        const body = await res.body.text();
        // GitHub serves error pages with 200 sometimes — sanity-check content.
        if (/^\s*<\?xml|Backend\.max_conn/i.test(body.slice(0, 200))) {
          throw new Error('CDN error page instead of source');
        }
        return body;
      } catch (e) {
        lastErr = e;
        if (attempt === 0) await new Promise((r) => setTimeout(r, 1_500));
      }
    }
  }
  throw lastErr;
}
