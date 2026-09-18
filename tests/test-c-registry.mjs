// Registry regression tests against the CURRENT catalog/registry API.
// Pure logic — no network required (falls back to cache/seed offline).
import assert from 'node:assert';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { ModelRegistry } from '../src/registry.js';
import { parseCatalogFromBinary, readCliCatalog } from '../src/catalog.js';

const R = fileURLToPath(new URL('../research/', import.meta.url));
let passed = 0;
const failures = [];
async function t(name, fn) {
  try { await fn(); console.log('PASS', name); passed++; }
  catch (e) { console.log('FAIL', name, '::', e.message); failures.push(name); }
}

// ---- 1. ModelRegistry loads a map and answers lookups consistently ----------
await t('registry loads a model->agent map and answers lookups', async () => {
  const log = { info() {}, warn() {}, error() {} };
  // remoteUrl '' keeps this fully offline: seed/catalog-cache path only.
  const reg = new ModelRegistry(log, { tier: 'base3', remoteUrl: '' });
  await reg.refresh(); // async: must await, never treat the Promise as a catalog
  const models = reg.models();
  assert.ok(Array.isArray(models) && models.length > 0, `non-empty model list, got ${models.length}`);
  for (const m of models) {
    assert.ok(reg.has(m), `has('${m}') true for every listed model`);
    assert.ok(reg.agentForModel(m), `agentForModel('${m}') non-null`);
  }
  assert.ok(!reg.has('no/such-model'), 'unknown model not advertised');
  assert.equal(reg.agentForModel('no/such-model'), null, 'unknown model maps to null agent');
  const status = reg.status();
  assert.ok(Array.isArray(status.models) && typeof status.source === 'string', 'status() well-formed');
});

// ---- 2. Self-describing agent records parse into a tier map ------------------
await t('parseCatalogFromBinary extracts base3 records; root agent wins over -evals', () => {
  const fake = [
    'junk before',
    '"base3-free-mimo":{publisher:"codebuff",model:"mimo/mimo-v2.5",x:1}',
    '"base3-free-mimo-evals":{publisher:"codebuff",model:"mimo/mimo-v2.5",x:1}',
    '"base2-free-glm":{publisher:"codebuff",model:"z-ai/glm-5.2",x:1}',
    'junk after',
  ].join('\n');
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'fbcat-'));
  const p = path.join(dir, 'fake.exe');
  fs.writeFileSync(p, fake, 'latin1');
  const out = parseCatalogFromBinary(p, 'base3');
  assert.equal(out['mimo/mimo-v2.5'], 'base3-free-mimo', 'root agent preferred over -evals roll');
  assert.equal(out['z-ai/glm-5.2'], undefined, 'base2 record excluded on tier base3');
  fs.rmSync(dir, { recursive: true, force: true });
});

await t('parseCatalogFromBinary returns null for unusable input', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'fbcat-'));
  const p = path.join(dir, 'empty.exe');
  fs.writeFileSync(p, 'no agent records here', 'latin1');
  assert.equal(parseCatalogFromBinary(p, 'base3'), null, 'no records -> null');
  assert.equal(parseCatalogFromBinary(path.join(dir, 'missing.exe'), 'base3'), null, 'unreadable file -> null');
  fs.rmSync(dir, { recursive: true, force: true });
});

// ---- 3. Research snapshot stays readable; readCliCatalog degrades gracefully -
await t('research snapshot present; readCliCatalog returns map or null (no throw)', () => {
  const agents = fs.readFileSync(`${R}/free-agents.ts`, 'utf8');
  assert.ok(agents.includes('AGENT_ID_BY_MODEL') || agents.length > 1000, 'snapshot present');
  // No real CLI binary on this machine -> null, never a throw.
  const r = readCliCatalog('base3');
  assert.ok(r === null || (r.map && Object.keys(r.map).length > 0));
});

console.log(`\n${passed} passed, ${failures.length ? failures.length + ' failed: ' + failures.join(', ') : 'all passed'}`);
process.exit(failures.length ? 1 : 0);
