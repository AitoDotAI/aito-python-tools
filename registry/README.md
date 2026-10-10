# Releasing aitoai, aito-mcp and the MCP registry entry

One version tag publishes all three, from GitHub Actions (`.github/workflows/release.yml`), with
no stored secrets:

| What | Where | How the workflow authenticates |
|---|---|---|
| aitoai (the SDK, with the `[mcp]` extra) | PyPI | PyPI Trusted Publishing (OIDC) |
| aito-mcp (a thin package, so a client can run `uvx aito-mcp`) | PyPI | PyPI Trusted Publishing (OIDC) |
| the server entry `io.github.AitoDotAI/aito` | the official MCP registry | `mcp-publisher login github-oidc` |

Why a second package: registry clients run `uvx <identifier>`. `uvx aitoai` fails, because aitoai's
commands are `aito` and `aito-mcp` ("Use `uvx --from aitoai <EXECUTABLE-NAME>` instead").
`aito-mcp` depends on `aitoai[mcp]` at the same version and exposes the command.

## Cutting a release (an agent can do all of it; the tag push needs Antti's yes)

1. `python scripts/versions.py bump X.Y.Z`. It writes the version into `aito/__init__.py`,
   `packaging/aito-mcp/pyproject.toml` (the version and the `aitoai[mcp]==` pin) and
   `registry/server.json`, and renames the changelog's "Unreleased" section to `X.Y.Z`.
2. PR, CI green, merge. `tests/sdk/test_release_versions.py` fails CI if any version disagrees.
3. On master: `git tag X.Y.Z && git push origin X.Y.Z`.
4. The workflow runs:
   - it checks the tag equals the version in every file;
   - it builds and `twine check`s both packages;
   - it publishes aitoai;
   - it waits until aitoai is installable, then publishes aito-mcp;
   - it waits until aito-mcp is on PyPI, then publishes the registry entry.
5. Verify:
   - `uvx aito-mcp` runs;
   - `https://registry.modelcontextprotocol.io/v0/servers?search=io.github.AitoDotAI` shows `X.Y.Z`.

The SDK docs deploy from master by themselves (`docs.yml`).

## One-time setup (Antti, a few minutes; then no tokens are needed again)

1. **PyPI Trusted Publishing, on both projects.** On pypi.org, open `aitoai` → Settings → Publishing →
   Add a new publisher → GitHub. Enter:
   - Owner `AitoDotAI`
   - Repository `aito-python-tools`
   - Workflow `release.yml`
   - Environment `pypi`

   Then the same for `aito-mcp`.
2. **The `pypi` environment on GitHub.** In AitoDotAI/aito-python-tools → Settings → Environments →
   New environment `pypi`:
   - **Deployment branches and tags:** "Selected branches and tags", add a tag rule `*.*.*`.
   - **Optional:** "Required reviewers: Antti". Every release then waits for one click in GitHub.
     That makes Antti's yes an explicit gate even when an agent pushed the tag.
3. **Nothing for the registry.** The OIDC login grants the repository owner's namespace
   (`io.github.AitoDotAI/*`) to workflows in AitoDotAI repositories. The environment rule in 2 is
   what keeps that to release tags.

## Manual fallback

- **aitoai:** `./do release` (reads `TWINE_*` from `.env`).
- **aito-mcp:**
  `twine upload --config-file /dev/null --repository-url https://upload.pypi.org/legacy/ -u __token__ packaging/aito-mcp/dist/*`
  with a token scoped to aito-mcp, built first with `python -m build packaging/aito-mcp`.
- **Registry:** an org **Owner** logs in with a classic personal access token whose only scope is
  `read:org`: `mcp-publisher login github --token <PAT>`, then `mcp-publisher publish registry/server.json`.
  The browser login (`login github`) is a GitHub App that sees only orgs where it is installed, so it
  grants just the personal namespace.
