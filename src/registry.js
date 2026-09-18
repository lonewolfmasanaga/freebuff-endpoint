// Live model registry backed by the installed Freebuff/Codebuff CLI catalog.
//
// The former implementation was a hand-maintained static VERIFIED map — which
// went stale the moment Freebuff retired an agent (base2-free-luna) or added
// new tiers. This version derives the model -> agent map from the CLI binary
// at load time and refreshes it periodically, so upgrades to the CLI are
// picked up automatically and the gateway never advertises a model it can't
// serve.
//
// Keeps the same public surface (start, stop, models, has, agentForModel,
// status) so the protocol layers are untouched.
import { loadCatalog, readCliCatalog, catalogSourceLabel } from './catalog.js';
import { currentCliVersion } from './http-client.js';

const DEFAULT_REFRESH_MS = 6 * 3600 * 1000; // must equal catalog cache TTL default

export class ModelRegistry {
  constructor(logger, options = {}) {
    this.log = logger;
    this.tier = options.tier || 'base3';
    this.refreshMs = options.refreshMs || DEFAULT_REFRESH_MS;
    this.remoteUrl = options.remoteUrl || ''; // '' = remote disabled
    this.modelToAgent = {}; // model id -> agent
    this.source = 'seed';
    this.updatedAt = Date.now();
    this._timer = null;
  }

  async _loadAsync() {
    const c = await loadCatalog({ tier: this.tier, remoteUrl: this.remoteUrl });
    this.modelToAgent = c.models || {};
    this.source = c.source;
    this.updatedAt = c.updatedAt;
  }

  async start() {
    await this._loadAsync();
    this.log.info(
      `catalog: ${Object.keys(this.modelToAgent).length} model(s) from ${catalogSourceLabel({ source: this.source })} (tier ${this.tier})`,
    );
    // Re-check periodically so CLI upgrades / repo changes propagate while the
    // gateway runs.
    this._timer = setInterval(() => {
      this._loadAsync().catch((e) => this.log.warn(`catalog refresh failed: ${e.message}`));
    }, this.refreshMs);
    this._timer.unref?.();
  }

  stop() {
    if (this._timer) clearInterval(this._timer);
    this._timer = null;
  }

  /** Re-read the catalog (used by the admin refresh endpoint). */
  async refresh() {
    // Await the load: loadCatalog is async (remote fetch), and treating the
    // returned Promise as a catalog used to WIPE modelToAgent to {} — a manual
    // refresh bricked the gateway with 0 models until the next restart.
    await this._loadAsync();
    this.log.info(`catalog manually refreshed: ${Object.keys(this.modelToAgent).length} model(s)`);
    return { source: this.source, models: this.models() };
  }

  models() {
    return Object.keys(this.modelToAgent).sort();
  }

  has(model) {
    return Object.prototype.hasOwnProperty.call(this.modelToAgent, model);
  }

  agentForModel(model) {
    return this.modelToAgent[model] || null;
  }

  status() {
    return {
      models: this.models(),
      source: this.source,
      tier: this.tier,
      updatedAt: this.updatedAt,
      cli: currentCliVersion(),
    };
  }
}