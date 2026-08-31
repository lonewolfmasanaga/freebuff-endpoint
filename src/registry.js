// Static model registry: a verified model -> agent map, no network sync.
// Verified against upstream CodebuffAI/freebuff (common/src/constants/
// free-agents.ts, freebuff-models.ts, freebuff-model-ids.ts), snapshot Aug 2026.
// This mirrors FREEBUFF_ROOT_AGENT_ID_BY_MODEL for the current catalog. If
// upstream adds or withdraws models, update this table.
//
// Notable since the last revision:
//   - GLM 5.3 Flash replaced DeepSeek V4 Pro as the deep row (dropped here
//     because app pickers no longer offer V4 Pro).
//   - Solar Pro 4 was a new premium row; withdrawn upstream (absent from the
//     0.0.156 CLI catalog) and removed from this map.
//   - MiniMax moved to M3 (minimax/minimax-m3). V4 Flash & MiMo are unlimited.
//   - 'google/gemini-2.5-flash-lite' never was a free-buff root (it pointed at
//     the file-picker subagent) and was removed.
//   - Ox Alpha was withdrawn upstream 2026-08-27 (not added).
//   - Luna moved from base2-free-luna -> base3-free-luna (base2 retired
//     upstream; free_mode_legacy_luna_agent). Kept:
//       'openai/gpt-5.6-luna'     -> base3-free-luna   (DEFAULT Luna)
//       'openai/gpt-5.6-luna-es'  -> base3-free-luna-es  (not yet mapped)
//       'openai/gpt-5.6-luna-max' -> base3-free-luna-max (not yet mapped)
//   - GLM 5.3 Flash and Solar Pro 4 are gone from the current (0.0.156) CLI
//     catalog — both removed; only glm-5.2 remains in the z-ai family.

const MODELS = {
  'deepseek/deepseek-v4-flash': 'base2-free-deepseek-flash',
  'mimo/mimo-v2.5': 'base2-free-mimo',
  'minimax/minimax-m3': 'base2-free-minimax-m3',
  'openai/gpt-5.6-luna': 'base3-free-luna',
  'z-ai/glm-5.2': 'base2-free-glm', // referral-gated premium tier, still live
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
