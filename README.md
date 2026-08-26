# Freebuff Endpoint

Self-hosted gateway that exposes **Freebuff's free models as standard OpenAI and Anthropic APIs** — use them from any harness (Hermes, Claude Code, opencode, Cline, LangChain, raw SDKs) instead of the proprietary CLI. Includes a web dashboard for token management/testing and a hidden Windows auto-start service.

## One-click install (Windows)

Open PowerShell and run:

```powershell
irm https://raw.githubusercontent.com/lonewolfmasanaga/freebuff-endpoint/main/setup.ps1 -OutFile setup.ps1; powershell -ExecutionPolicy Bypass -File setup.ps1; Remove-Item setup.ps1
```

That single command: checks Node.js → clones this repo to `%USERPROFILE%\freebuff-endpoint` → installs dependencies → registers a hidden auto-start service → starts the gateway.

Then open **http://127.0.0.1:8090/** — paste your Freebuff auth token in the dashboard and you're live.

> Need a token? Log in once with the [Freebuff CLI](https://github.com/CodebuffAI/freebuff) (`npm i -g freebuff` / `freebuff`), or take it from your account settings. The CLI-detected token is picked up automatically on local machines.

## Manual install

```powershell
git clone https://github.com/lonewolfmasanaga/freebuff-endpoint.git
cd freebuff-endpoint
npm install
copy config.example.json config.json   # optional; defaults apply without it
npm start
```

Requires **Node.js ≥ 20.11** ([nodejs.org](https://nodejs.org)).

## The dashboard (`http://127.0.0.1:8090/`)

| Panel | What it does |
|---|---|
| **Status** | Live health, active tokens, model count, egress mode |
| **Auth tokens** | Paste a new token → saved + applied instantly, no restart. One-click removal. |
| **Test chat** | Pick any model, send a prompt, read the reply |

## Point your tools at it

**OpenAI-compatible (Hermes, LangChain, raw SDKs):**
```python
from openai import OpenAI
client = OpenAI(base_url="http://127.0.0.1:8090/v1", api_key="none")
client.chat.completions.create(model="mimo/mimo-v2.5", messages=[...])
```

**Claude Code / Anthropic-shaped tools:**
```bash
export ANTHROPIC_BASE_URL=http://127.0.0.1:8090
export ANTHROPIC_MODEL=mimo/mimo-v2.5
```

Model list: `GET http://127.0.0.1:8090/v1/models` · health: `/health`, `/status`

## Configuration (`config.json`)

Works with defaults. Common tweaks:

| Key | Default | Meaning |
|---|---|---|
| `AUTH_TOKENS` | auto-detect | Freebuff tokens (or manage via dashboard) |
| `PROXY_URL` | *(empty)* | Upstream SOCKS4/5 or HTTP(S) proxy — full-region exits unlock the whole catalog |
| `API_KEYS` | `[]` | Require client keys if exposing beyond localhost |
| `POOL_FALLBACK_MODEL` | `deepseek/deepseek-v4-flash` | Transparent fallback when earned-pool models are out of quota |
| `DEBOUNCE_MS` | `1100` | Min gap between upstream calls |

Full defaults in `src/config.js`.

## Service management

```powershell
Stop-Process -Name node -Force                                   # stop
& "$env:USERPROFILE\freebuff-endpoint\install-windows.ps1"       # reinstall/restart
powershell -ExecutionPolicy Bypass -File "$env:USERPROFILE\freebuff-endpoint\uninstall.ps1" -Wipe   # full uninstall
```

The service runs hidden at every login and restarts itself within ~5s of a crash. Logs: `logs\gateway.log` (auto-rotates).

## How it works

1. Admits a free session per token (waiting room handled transparently), declaring your model via `x-freebuff-model`
2. Starts long-lived agent-runs per model-agent with rotation
3. Wraps requests in CLI-conformant shape (canonical system marker + signature toolset — upstream enforces both for free mode) and strips conformance artifacts from responses
4. Syncs the model catalog live from Freebuff's open source (withdrawn models filtered, catalog persisted across restarts)

## Notes & honest risks

- This automates your own account outside the official client — accounts *can* be flagged/banned. Don't run heavy load.
- Some models train on submissions (DeepSeek/MiniMax variants) — don't send secrets.
- `config.json` holds your tokens/proxy credentials — it's gitignored and never leaves your machine.

MIT license. Not affiliated with CodebuffAI.
