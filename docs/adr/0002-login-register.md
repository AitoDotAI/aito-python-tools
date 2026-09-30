# ADR 0002: `aito login`, cloud databases from the CLI, and `aito register` (design note, no code yet)

- Status: **draft for discussion**. No code in this repo or in aito-mission-control changes until Antti and the mission-control lane
  agree on the open questions at the end.
- Date: 30.9.2026. Follows ADR 0001 (`aito start`, profiles), phase 2. Ticket td-20260930083456875743.
- Facts about the console were checked against aito-mission-control `origin/master` at bf508d44b, which includes #1019 (the trial
  front door, affe38142). Paths below are in that repo.

## What exists today

| Need | Today | Where |
|---|---|---|
| Sign in | Auth0 **implicit flow** in the browser (password, Google, GitHub, Azure AD). The access token is exchanged once for a Redis-backed `aitosession` cookie. | console/src/config.js:24-29; console-server authentication.service.ts:153-249, main.ts:202-267 |
| Act as a user from a program | **Nothing.** No personal access token, no device flow, no client-credentials grant. The session cookie (with CSRF) is the only user credential. | searched: token, pat, device, client_credentials, bearer |
| Create a trial database | `POST /api/users/:userId/instances` with `instanceType: SANDBOX`. **Synchronous**: it returns a READY shared-engine database (`shared.aito.ai/db/<name>`). Name check: `GET /api/instance-names/:name`. | customer-api users.controller.ts:169-215, instances.service.ts:529-691 |
| Read a database's keys | `GET …/instances/:id/api-keys` lists `{id, type}`. `GET /api/instances/:id/api-keys/:keyId/key` returns the secret; guests get 401 for READ_WRITE. | instances.controller.ts:195-221 |
| Rotate keys | Admin only. | admin.controller.ts:199-217 |
| Self-hosted | Anonymous `POST /public/licenses/validate`. Anonymous telemetry `POST /public/telemetry/hello` (`boot` / `heartbeat`, the image's `instanceId`). **Nothing links an instance to an account**; `licenses.customerId` is free text. | licenses.controller.ts:19-51, telemetry.controller.ts:13-101 |
| customer-api | Internal. It trusts a shared `CUSTOMER_API_SECRET` plus an `x-user-id` header set by console-server. **Never callable by end users**, so every new route below belongs on console-server. | ApiKeyMiddleware.ts:22-31, authorization.ts:184-191 |
| Limits | Sandbox: 5,000 calls/month, 100 MB, removed after 30 days inactive (#1038, deletion still dry-run). **No per-user cap on how many sandboxes one user creates.** | stripe.service.ts:2140-2147, uniqueSandbox.service.ts |

## Proposal

The CLI never holds the user's password or the console session. It holds a **console token** that can do a few named things, and it
turns what it gets into the same profiles `aito start` writes. After `aito db create`, `aito.Client()` just works, exactly as after
`aito start`.

### 1. Two ways to get a console token

**(a) `aito login`: OAuth 2.0 Device Authorization Grant (RFC 8628) through Auth0, for a human at a terminal.**
1. The CLI requests a device code from Auth0.
2. It prints `Open https://console.aito.ai/activate and enter WDJB-MJHT`, and opens the browser when it can.
3. It polls the token endpoint until the user approves or the code expires (15 min).
4. It receives an access token (audience: a new "Aito CLI" API in Auth0, short-lived) and a refresh token (`offline_access`,
   rotating).

Auth0 supports this grant natively for a Native application. The tenant is not in Terraform (infrastructure/auth0.tf only has log
groups), so enabling it is a manual tenant change. The approval page shows the device and the app name ("Aito CLI on
<hostname>"), because device-code phishing works by the victim approving someone else's code.

**(b) Personal access tokens (PATs), for agents and CI, which have no human at the moment of use.**
- Created on a new console page, "Access tokens": a name, scopes, an expiry (default 90 days, max 1 year).
- Shown once, as `aito_pat_<random>`. Stored **hashed** (SHA-256), with created and last-used times.
- Revocable in the console, and listed with their last use.
- An agent is given a PAT through `AITO_CONSOLE_TOKEN`. `aito login --token` reads it from stdin, never from argv.

Both reach console-server through one new guard, `CliAuthGuard`, on a new route prefix `/api/cli/v1/*`:
- `Authorization: Bearer <Auth0 access token>`, verified against Auth0's JWKS (audience and issuer checked), `sub` mapped to the
  Aito user as the session exchange already does;
- or `Authorization: Bearer aito_pat_…`, looked up by hash, scopes enforced.

No cookie and no CSRF on this prefix, because it's not a browser surface. console-server then calls customer-api as it already does
(`x-user-id` plus the shared secret), so **customer-api does not change** except for new internal endpoints.

**Scopes**, deliberately few:

| Scope | Allows | Not |
|---|---|---|
| `databases:read` | list your databases; read their keys, including READ_WRITE for databases you own | team databases you are only a guest on (READ_WRITE refused, as today) |
| `databases:create` | create a **sandbox** (trial) database | paid or dedicated instances, anything that bills |
| `selfhosted:register` | link a self-hosted instance (§3) | reading its data |

Deleting databases, billing, team membership and key rotation stay in the console. Rotation isn't user-facing even there today.

### 2. CLI commands (cloud)

```
aito login                      # device flow; `aito login --token` reads a PAT from stdin
aito logout                     # forgets the console token (revoking it server-side for a PAT is a console action)
aito db list                    # your databases: name, type, state, url
aito db create NAME             # sandbox: name check, create (READY at once), fetch keys, write profile NAME
aito db use NAME                # (re)fetch keys into profile NAME and make it active
```

- `db create` / `db use` write an ordinary profile (`instance_url = https://shared.aito.ai/db/NAME`, `api_key` = read-write,
  `read_only_api_key`) through the same 0600 writer. So the SDK, `aito.Client()`, the v1 CLI and `AITO_PROFILE` all work unchanged,
  and ADR 0001's pairing rule applies.
- **Where tokens live.** The console token goes to `~/.config/aito/console` (0600): a separate file from `credentials` because it is
  a different class of secret, so `aito logout` removes only it and never a database key. What's stored:
  - for a device login, the refresh token (the access token stays in memory);
  - for a PAT given by `--token`, the PAT;
  - `AITO_CONSOLE_TOKEN` is never written to disk.
  OS keychain storage (macOS Keychain, Windows Credential Manager, Secret Service) is a later option through the optional `keyring`
  package. The file stays the default because headless Linux and CI have no keychain.
- **Resolution** for console calls: `AITO_CONSOLE_TOKEN`, then `~/.config/aito/console`, else "run `aito login`". The same order as
  database credentials: the environment wins.

### 3. What an agent can do non-interactively

With a PAT in `AITO_CONSOLE_TOKEN` and no human present:
1. `aito db create my-eval --json` creates a sandbox and writes profile `my-eval`. With `--json` it prints
   `{"profile","instance_url","read_only_api_key"}` and never prints the read-write key unless asked with `--show-keys`.
2. `AITO_PROFILE=my-eval python agent.py`: `aito.Client()` works.
3. `aito db list --json` and `aito db use NAME` recover keys on a fresh machine.

It can't: log in without a PAT (the device flow needs a human once), create anything that bills, delete, change a team, or read
READ_WRITE keys of databases it doesn't own.

**The one server-side prerequisite before PATs ship: a cap on sandbox creation.** Today nothing stops a looping agent from creating
sandboxes without end. Proposed: at most 3 live sandboxes per user and 10 creations per user per day, enforced in customer-api, with
a clear 429 the CLI shows as it is.

### 4. `aito register`: linking a self-hosted instance (sketch)

- **Interactive:** `aito register` (after `aito login`, with a local profile from `aito start`) reads the instance's `instanceId`
  (the image already reports it in telemetry; it would need a small `GET /version` field or a file on the volume). It sends
  `POST /api/cli/v1/selfhosted {instanceId, name}`. The console then lists "Self-hosted: <name>, last seen <heartbeat>, version,
  row counts".
  - That's **metadata only**, from the telemetry the image already sends.
  - The console never holds the instance's keys and never calls it (no CORS or relay problem; see Q2).
- **Production, non-interactive:** the operator creates a PAT with only `selfhosted:register` and passes it to the container as
  `AITO_REGISTRATION_TOKEN`. On `boot`, the image's telemetry call carries it (`Authorization: Bearer`), and the server links that
  `instanceId` to the token's user once. That's an aito-core entrypoint change plus the telemetry endpoint accepting the optional
  header. Without the token, telemetry stays anonymous exactly as today.
- **Licences:** once instances are linked, a licence can bind to the account (`customerId` becomes the user or team id) instead of a
  free-text string. `aito license set` would then write `AITO_LICENSE_KEY` for `aito start`. The binding model is Q4.

## Phasing (each is shippable alone)

1. **P2a: PATs plus `aito db create/list/use`.**
   - mission-control: the PAT table and page, `CliAuthGuard`, 4 `/api/cli/v1` routes proxying existing customer-api calls, and the
     sandbox cap.
   - This repo: `aito db`, `aito login --token`.
   - Smallest useful slice, and it serves agents first.
2. **P2b: `aito login` device flow.** An Auth0 tenant change (Native app, device grant, the CLI API audience) plus JWKS verification
   in the same guard.
3. **P2c: `aito register` / `AITO_REGISTRATION_TOKEN`.** After Q1 and Q4 are decided. Touches aito-core's entrypoint and the
   telemetry endpoint.

## Security notes

- **Hashing.** PATs are stored hashed; database keys are fetched on demand and stored only on the user's machine (0600). (Shared
  sandboxes' keys sit in plaintext in `instance_api_keys.secret_key` on the server today. That's out of scope here, but worth its
  own ticket.)
- **Isolation.** The CLI route prefix is separate from the cookie routes, so a token can't be replayed as a browser session, or the
  other way round.
- **Audit.** Every `/api/cli/v1` call logs the user, the token id (never the token) and the scope used.
- **Device flow.** Short code expiry, rate-limited polling, and the approval page names the requesting device.

## Open questions (for Antti and mission-control)

1. **What does register send, and is register the telemetry consent?** Linking makes telemetry non-anonymous for that instance. An
   explicit opt-in (the operator adds the token) is cleaner than anything implicit. This interacts with td-20260929225128511425
   §4(b)/§5.
2. **Should the console ever talk to a self-hosted instance?** This note says no: metadata only, from telemetry. A live console
   view of a local instance would need browser-to-localhost access (CORS plus Private Network Access) or a relay service. It's worth
   deciding explicitly that we don't.
3. **PAT first (P2a), or the device flow first?** PAT-first serves agents and CI immediately and needs no Auth0 change.
   Device-flow-first is the nicer human experience.
4. **Licence binding:** to the account, the team, or the `instanceId`? And an offline or air-gapped licence (a signed file
   `aito license set FILE`, no call-home)?
5. **Sandbox caps:** are 3 live and 10 per day right, and should an agent-created sandbox be marked (the token id on the instance)
   so it can be cleaned up separately?
6. **Console hostname for the CLI:** `console.aito.ai` (Heroku today) or `aws-console.aito.ai` (being prepared)? The CLI should use
   one stable name that survives that migration.
7. **Teams:** should `databases:create` ever create in a team (a paid, billed path), or stay personal-sandbox-only? This note
   proposes personal-only.
