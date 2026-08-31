// Static model registry: a verified model -> agent map, no network sync.
// Verified against upstream CodebuffAI/freebuff (common/src/constants/
// free-agents.ts, freebuff-models.ts, freebuff-model-ids.ts), snapshot Aug 2026.
// This mirrors FREEBUFF_ROOT_AGENT_ID_BY_MODEL for the current catalog. If
// upstream adds or withdraws models, update this table.
//
// Notable since the last revision:
//   - GLM 5.3 Flash replaced DeepSeek V4 Pro as the deep row (dropped here
//     because app pickers no longer offer V4 Pro).
//   - Solar Pro 4 is a new premium row.
//   - MiniMax moved to M3 (minimax/minimax-m3). V4 Flash & MiMo are unlimited.
//   - 'google/gemini-2.5-flash-lite' never was a free-buff root (it pointed at
//     the file-picker subagent) and was removed.
//   - Ox Alpha was withdrawn upstream 2026-08-27 (not added).

const MODELS = {
  'deepseek/deepseek-v4-flash': 'base2-free-deepseek-flash',
  'mimo/mimo-v2.5': 'base2-free-mimo',
  'minimax/minimax-m3': 'base2-free-minimax-m3',
  'openai/gpt-5.6-luna': 'base2-free-luna',
  'upstage/solar-pro4': 'base2-free-solar-pro4',
  'z-ai/glm-5.2': 'base2-free-glm', // referral-gated premium tier, still live
  'z-ai/glm-5.3-flash': 'base2-free-glm-5-3-flash',
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