# Publishing aito-mcp: release, registry, marketplace (drafts)

Prepared 5.10.2026. Nothing here is published, and each step below needs Antti's yes.

## What gets published

| What | Where | Draft in this repo |
|---|---|---|
| aitoai 1.2.0 (the SDK, the `[mcp]` extra, the engine pin v2.11.2) | PyPI | branch `release/1.2.0` |
| aito-mcp 1.2.0 (a thin package, so a client can run `uvx aito-mcp`) | PyPI | `packaging/aito-mcp/` |
| the server entry `io.github.AitoDotAI/aito` | the official MCP registry | `registry/server.json` |
| the plugin `aito` | a Claude Code marketplace: this repo | `.claude-plugin/marketplace.json` |

Why a second package: registry clients run `uvx <identifier>`. `uvx aitoai` fails, because
aitoai's commands are `aito` and `aito-mcp` ("Use `uvx --from aitoai <EXECUTABLE-NAME>`
instead"). `aito-mcp` depends on `aitoai[mcp]` at the same version and exposes the command.

## Order

1. **Merge the release.** Push `release/1.2.0` and open a PR. Dispatch the "aito start
   across platforms" workflow with `image=ghcr.io/aitohq/aito:v2.11.2`, then merge once green.
2. **aitoai to PyPI**, from master: `./do release`. It prompts once and reads `TWINE_*`
   from `.env`.
3. **aito-mcp to PyPI**, after aitoai 1.2.0 is live (its dependency must resolve). It is a new
   project: a token scoped to the `aitoai` project cannot create it, so the first upload needs
   an account-wide token (then scope a new token to `aito-mcp`):

       pip install build twine && cd packaging/aito-mcp && python3 -m build && twine check dist/* && twine upload dist/*

4. **Registry.** One-time install of `mcp-publisher`, from the modelcontextprotocol/registry
   releases. Then:

       mcp-publisher login github      # as a member of the AitoDotAI GitHub org
       mcp-publisher publish registry/server.json

   The registry checks that the aito-mcp PyPI description contains
   `mcp-name: io.github.AitoDotAI/aito`; `packaging/aito-mcp/README.md` has it.
5. **Marketplace.** Nothing to submit. Once on master, users run:

       /plugin marketplace add AitoDotAI/aito-python-tools
       /plugin install aito@aito

   Listing in Anthropic's plugin directory would be a separate submission form.
6. **Docs link** on aito.ai, owned by the website lane.

## Decisions (Antti)

- The second PyPI package, `aito-mcp`. The alternative is no registry entry and only the
  plugin's `uvx --from 'aitoai[mcp]' aito-mcp`.
- The registry name: `io.github.AitoDotAI/aito` (GitHub login, works today), or
  `ai.aito/aito` (needs a DNS TXT record on aito.ai).
- The engine pin for `aito start`: v2.11.2 now (the latest multi-arch image on ghcr), or
  wait for v2.11.3/v2.11.4 images (v2.11.4 fixes the nested-from link-path 400).
- After step 3, whether the plugin's `.mcp.json` moves to `uvx aito-mcp` (shorter, pinned
  by the package) from `uvx --from 'aitoai[mcp]' aito-mcp` (works today).
