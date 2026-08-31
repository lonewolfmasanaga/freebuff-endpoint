# Freebuff Endpoint

Self-hosted gateway that exposes **Freebuff's free models as standard OpenAI and Anthropic APIs** — use them from any harness (Hermes, Claude Code, opencode, Cline, LangChain, raw SDKs) instead of the proprietary CLI. Minimal, on-demand, foreground process: run it when you need it, close the terminal when you're done.

## Quick start

```bash
git clone https://github.com/lonewolfmasanaga/freebuff-endpoint.git
cd freebuff-endpoint
npm install
AUTH_TOKENS=your-freebuff-token npm start
```

Requires **Node.js ≥ 20.11** ([nodejs.org](https://nodejs.org)). Only two dependencies (`undici`, `socks`).

> No token handy? Log in once with the [Freebuff CLI](https://github.com/CodebuffAI/freebuff) (`npm i -g freebuff` / `freebuff`) — the gateway auto-detects `~/.config/manicode/credentials.json` on boot. Or paste a token in the dashboard.

Then open **http://127.0.0.1:8090/** to confirm it's live and tune the upstream proxy if needed.

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

## The dashboard (`http://127.0.0.1:8090/`)

A single page dedicated to the one thing you'll tune in day-to-day: **live egress proxy control.** It shows the current egress mode and lets you paste a `socks5://` or `http(s)://` URL and apply it instantly — no restart. Tokens are NOT managed here; set `AUTH_TOKENS` or rely on CLI auto-detect.

### Upstream proxy

Set `PROXY_URL` in the environment (or `config.json`) before boot, or apply it live from the dashboard:

```bash
PROXY_URL=socks5://user:pass@host:1080 npm start
```

Supported schemes: `socks5://`, `socks4://`, `http://`, `https://`. The proxy applies **only** to upstream codebuff.com traffic — the local listener is untouched. Changes from the dashboard take effect on the next request (in-flight requests finish on the old connection).

## Configuration

Works with defaults. Env vars beat `config.json` beats built-in defaults:

| Key | Default | Meaning |
|---|---|---|
| `AUTH_TOKENS` | auto-detect | Freebuff token(s), comma-separated |
| `PROXY_URL` | *(empty)* | Upstream SOCKS4/5 or HTTP(S) proxy — also settable live from the dashboard |
| `LISTEN_ADDR` | `127.0.0.1:8090` | Local listener |
| `API_KEYS` | `[]` | Require client keys if exposing beyond localhost |
| `UPSTREAM_BASE_URL` | codebuff.com | Upstream API base |
| `REQUEST_TIMEOUT_MS` | `900000` | Max request lifetime |
| `ROTATION_INTERVAL_MIN` | `360` | Restart an agent-run after this many minutes |
| `DEBOUNCE_MS` | `1100` | Min gap between upstream calls |
| `WAITING_ROOM_MAX_WAIT_MS` | `120000` | How long a request waits in the free-tier queue before timing out |
| `POOL_FALLBACK_MODEL` | `mimo/mimo-v2.5` | When a premium model's pool is exhausted *or* the free tier queues it, re-route to this unlimited model |
| `POOL_FALLBACK_ELIGIBLE` | `["mimo/mimo-v2.5", ...]` | Which models may be used as the fallback target |

`config.example.json` is a safe template — copy it to `config.json` and fill in your own `AUTH_TOKENS` / `PROXY_URL`. Real tokens and proxy credentials never belong in a committed file (see `.gitignore`).

## How it works

1. Admits a free session per token (waiting room handled transparently), declaring your model via `x-freebuff-model`
2. Keeps one long-lived agent-run per model-agent, restarted when stale
3. Wraps requests in CLI-conformant shape (canonical system marker + signature toolset — upstream enforces both for free mode) and strips conformance artifacts from responses
4. Model catalog is a static verified map (`src/registry.js`) — update the table when upstream changes

## Error handling

Every failure carries an explanation you can act on, on both protocol surfaces:

- OpenAI errors include `code` (e.g. `insufficient_quota`, `rate_limit_exceeded`, `region_or_account_blocked`, `waiting_room_queued`) and a plain-English `hint` telling you what to do next.
- Anthropic errors fold the same hint into `message` (most clients render only that) and keep structured `code` / `hint` fields for tools that inspect JSON.
- The free-tier waiting room is handled transparently: a queued request waits (up to `WAITING_ROOM_MAX_WAIT_MS`) instead of failing on first poll, and if a `POOL_FALLBACK_MODEL` is configured the request re-routes to that unlimited standalone instead of erroring.
- Quota, rate-limit, and waiting-room responses set a `retry-after` header so clients can back off instead of hammering.

## Notes & honest risks

- This automates your own account outside the official client — accounts *can* be flagged/banned. Don't run heavy load.
- Some models train on submissions (DeepSeek/MiniMax variants) — don't send secrets.
- `config.json` holds your tokens/proxy credentials — it's gitignored and never leaves your machine.

MIT license. Not affiliated with CodebuffAI.
