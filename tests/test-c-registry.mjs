// Registry regression tests: depth-aware parsing vs synthetic fixtures AND the
// real upstream research snapshots. Pure logic — no network.
import assert from 'node:assert';
import { extractFreeModeAgentModels, buildConstTable } from '../src/registry.js';
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';

const R = fileURLToPath(new URL('../research/', import.meta.url));
let passed = 0;
const failures = [];
async function t(name, fn) {
  try { await fn(); console.log('PASS', name); passed++; }
  catch (e) { console.log('FAIL', name, '::', e.message); failures.push(name); }
}

// ---- 1. Two-element inline Set (the bug that silently dropped models) --------
await t('two-element inline Set fully extracted', () => {
  const agents = `
export const FREE_MODE_AGENT_MODELS = {
  'base2-free-x': new Set(['test/model-a', 'test/model-b']),
  'file-picker': new Set(['google/gemini-2.5-flash-lite']),
};
`;
  const aux = `export const FREEBUFF_PAUSED_FREE_MODEL_IDS = ['test/gone'];`;
  const out = extractFreeModeAgentModels(agents, aux);
  assert.equal(out['test/model-a'], 'base2-free-x', `model-a missing: ${JSON.stringify(out)}`);
  assert.equal(out['test/model-b'], 'base2-free-x', `model-b missing: ${JSON.stringify(out)}`);
  assert.equal(out['google/gemini-2.5-flash-lite'], 'file-picker');
});

// ---- 2. Paused list as constants still filters -------------------------------
await t('paused entries resolved through constants', () => {
  const agents = `
export const FREE_MODE_AGENT_MODELS = {
  'base2-free': new Set(['x/keep-me', 'x/drop-me']),
};
`;
  const aux = `
export const DROP_ME_MODEL_ID = 'x/drop-me';
export const FREEBUFF_PAUSED_FREE_MODEL_IDS = [DROP_ME_MODEL_ID];
`;
  const out = extractFreeModeAgentModels(agents, aux);
  assert.ok(out['x/keep-me'], 'kept model present');
  assert.ok(!out['x/drop-me'], 'paused model excluded');
});

// ---- 3. Double-quoted + backtick literals resolve ----------------------------
await t('double-quoted and backtick literals resolve in const table', () => {
  const src = `
export const A_ID = "x/alpha";
export const B_ID = \`x/bravo\`;
`;
  const t1 = buildConstTable([src]);
  assert.equal(t1.A_ID, 'x/alpha');
  assert.equal(t1.B_ID, 'x/bravo');
});

// ---- 4. Member references resolve regardless of definition order -------------
await t('member reference resolves when table defined later', () => {
  const src = `
export const MIMO_ID = mimoModels.mimoV25;
export const mimoModels = { mimoV25: 'mimo/mimo-v2.5' };
`;
  const t1 = buildConstTable([src]);
  assert.equal(t1.MIMO_ID, 'mimo/mimo-v2.5');
});

// ---- 5. THE REAL SNAPSHOT: extraction matches the verified Aug 2026 catalog --
await t('real research snapshot extracts full catalog', () => {
  const agents = fs.readFileSync(`${R}/free-agents.ts`, 'utf8');
  const auxFiles = ['freebuff-models.ts', 'freebuff-model-ids.ts', 'model-config.ts'].map((f) => {
    try { return fs.readFileSync(`${R}/${f}`, 'utf8'); } catch { return ''; }
  });
  const aux = auxFiles.join('\n');
  const out = extractFreeModeAgentModels(agents, aux);
  const models = Object.keys(out);
  console.log(`      extracted ${models.length} models`);
  for (const m of [
    'deepseek/deepseek-v4-flash',
    'deepseek/deepseek-v4-pro',
    'mimo/mimo-v2.5',
    'openai/gpt-5.6-luna',
    'z-ai/glm-5.2',
  ]) assert.ok(models.includes(m), `missing ${m}`);
  // M3 was withdrawn upstream on 2026-08-20 — must stay excluded.
  const m3 = models.filter((m) => /minimax.*m3| minimax-minimax-m3/i.test(m) || m === 'minimax/minimax-m3');
  assert.equal(m3.length, 0, `withdrawn M3 leaked back in: ${m3}`);
  assert.ok(models.length >= 10, `catalog suspiciously small (${models.length})`);
});

console.log(`\n${passed} passed, ${failures.length} failed${failures.length ? ': ' + failures.join(', ') : ''}`);
process.exit(failures.length ? 1 : 0);
