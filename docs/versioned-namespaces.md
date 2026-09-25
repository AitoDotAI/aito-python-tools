# Versioned namespaces: `aito.v1`, `aito.v2`, and a default `aito.Client`

Status: **agreed 2026-09-25** (Antti), not yet implemented. Supersedes option (a)
of td-20260829212355565150 ("move the v2 client to a top-level `aito.v2`") — this
is the same move, generalised to every API version.

## The problem

Today the import path is the only version switch, and it is lopsided:

```python
from aito.client import AitoClient          # v1 — the obvious import
from aito.client.v2 import AitoClientV2     # v2 — an opt-in second path
```

The platform has already made v2 the default (the docs at `/docs/api/`, aito-demo on
2026-09-14). The SDK is the one surface where v1 is still the front door: the obvious
import, every `aito.api` helper, and every CLI subcommand talk to `/api/v1`.

## The decisions

1. **Each API version gets an explicit package**, whose meaning never changes:
   `aito.v1`, `aito.v2`, later `aito.v3`.
2. **The default is `aito.Client`**, which points at the current recommended version.
3. **The CLI follows the default.** When `aito.Client` moves to v2, so does `aito`.
4. **Moving the default is a major version bump.** The restructure ships as `0.7.0` with
   the default still on v1; flipping the default to v2 is `1.0.0`.

## Layout

```
aito/
  __init__.py     # `Client` -> the current default version (lazy, see below)
  v1/             # the v1 client, requests, responses, and the aito.api helpers
  v2/             # the v2 client (today's aito/client/v2)
  schema.py       # shared, version-agnostic
  cli/, utils/    # shared
  client/         # FROZEN v1 compatibility shim, deprecated — see "The trap"
```

Public names drop the redundant prefixes; the old names stay as aliases:

| Canonical | Kept alias |
|---|---|
| `aito.v1.Client` | `aito.client.AitoClient` |
| `aito.v2.Client` | `aito.client.v2.AitoClientV2`, `aito.v2.AitoClientV2` |
| `aito.v2.Error` | `AitoV2Error` |
| `aito.Client` | — (new) |

## The rule that makes "default follows latest" safe

`from aito import Client` **changes meaning** when the default moves. That is its
purpose, and also its risk: `pip install -U` could silently hand a script a
different API. So the default moves **only on a major bump**, and the package major
therefore says which API the default is:

```
aitoai 0.7.x  ->  aito.Client is v1
aitoai 1.x    ->  aito.Client is v2
aitoai 2.x    ->  aito.Client is v3
pip install "aitoai<2"   # the default stays v2
```

Documentation says so plainly: quickstarts use `aito.Client`; production code imports
the explicit version (`from aito.v2 import Client`).

## The trap: `aito.client` must NOT follow the default

Every existing caller writes `from aito.client import AitoClient` and means v1 —
including our own CLI and `aito.api`. If `aito.client` tracked the default, all of them
would switch to v2 on upgrade and break: different methods, response types and errors.

So `aito.client` becomes a **frozen v1 shim**: it re-exports `aito.v1` and emits a
`DeprecationWarning` naming the replacement. The "latest" alias is a *new* name,
`aito.Client`, which no existing code uses. `aito.client.v2` likewise becomes a
re-export of `aito.v2` for one release.

## The second trap: `aito/__init__.py` must stay lazy

Importing a submodule runs its parent's `__init__`. If `aito/__init__.py` did
`from aito.v1 import Client`, then `import aito.v2` would load the whole v1 stack —
exactly the import-weight defect fixed in 0.6.2, reintroduced one level up.

So `aito.Client` is resolved lazily with a module-level `__getattr__` (PEP 562), and
`tests/sdk/test_v2_import_weight.py` gains a case asserting `import aito.v2` loads no
v1 module. This also finishes the 0.6.2 fix properly: `aito.v2` no longer lives under
`aito.client`, so the lazy leaf imports stop being load-bearing.

Sphinx note: a lazy attribute is invisible to autodoc's introspection, and the docs build
runs `-W`. Document `aito.v1.Client` and `aito.v2.Client` as the real classes and
describe `aito.Client` in prose as an alias, rather than autodoc-ing it.

## 0.7.0 — the restructure (default unchanged)

- `aito.v1` and `aito.v2` packages; code physically moves there.
- `aito.Client` added, resolving to `aito.v1.Client`.
- `aito.client` / `aito.client.v2` become deprecated re-export shims.
- **Extras split** (the install-weight half of td-20260829212355565150): pandas,
  fastparquet, openpyxl, xlrd, langdetect and argcomplete move behind `aitoai[cli]`.
  *Amended during implementation:* the base keeps `jsonschema`, `aiohttp` and `ndjson`
  as well as `requests` and `packaging`, because the v1 client — the default throughout
  0.x — imports `jsonschema` on every response and `aito.v1.api` imports `ndjson` at load.
  A base install that could not run `aito.Client` would break the default. All three are
  small; the ~100 MB is pandas/numpy/fastparquet, and that is what moved. Breaking for anyone who expected the CLI
  from a bare `pip install aitoai`, which is why it rides the same release. Requires
  ending setup.py's verbatim reading of `requirements/build.txt` as `install_requires`.
- Dead `pandas~=1.0; python_version < "3.9"` marker removed.
- Changelog opens with a migration section.

Nothing changes meaning in 0.7.0: every existing import still resolves to what it did.

## 1.0.0 — the flip

- `aito.Client` resolves to `aito.v2.Client`.
- **The CLI is ported to v2.** The largest piece of work in the plan: every subcommand
  and `aito.api` helper maps to a v1 request class today, and 11 paths are hardcoded to
  `/api/v1`. The v1 CLI does not survive as a flag — `aitoai<1` is the way to keep it.
- The error-code taxonomy the v2 client exports (`QUERY_CODES`, `SCHEMA_DATA_CODES`) is
  published in the public v2 docs (td-20260922181543335267), or the SDK stops presenting
  it as stable. 1.0 promises stability; the codes must be part of what is promised.
- `aito.client` shims removed.

1.0 lands where it means something: the v2 contract has been stable across a few engine
releases, and the default API is the one the platform recommends.

## Open

- The `aito server start` onboarding work (td-20260830190739726601) sits on the v2 CLI,
  so it naturally follows 1.0 rather than preceding it.
- The MCP server (td-20260830205607129725) should import `aito.v2` from day one, and
  ship as `aitoai[mcp]` — which needs the extras machinery from 0.7.0.
