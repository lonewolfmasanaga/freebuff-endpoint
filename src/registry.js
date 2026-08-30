// Static model registry: a verified model -> agent map, no network sync.
// The FALLBACK_MODELS list is verified against the Aug 2026 snapshot of
// free-agents.ts. If upstream adds or withdraws models, update this table.

const MODELS = {
  'deepseek/deepseek-v4-flash': 'base2-free-deepseek-flash',
  'deepseek/deepseek-v4-pro': 'base2-free-deepseek',
  'minimax/minimax-m2.7': 'base2-free',
  'openai/gpt-5.6-luna': 'base2-free-luna',
  'mimo/mimo-v2.5': 'base2-free-mimo',
  'z-ai/glm-5.2': 'base2-free-glm',
  'crof/kimi-k3-eco': 'base2-free-kimi-k3-eco',
  'google/gemini-2.5-flash-lite': 'file-picker',
};

export class ModelRegistry {
  constructor(logger) {
    this.log = logger;
    this.modelToAgent = { ...MODELS };
  }

  start() {}

  stop() {}

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
      source: 'static',
    };
  }
}
