# ADR 0001: `aito start`, a local server CLI, and profile-aware `aito.Client()`

- Status: **proposed**. A working prototype is on branch `feat/aito-start`.
- Date: 30.9.2026
- Ticket: td-20260930083456875743 (asked by Antti; scoped by the CPO)
- Image contract agreed with azure-81, who owns the free image (aito-core `docker/free`).

## Context

The 29.9 first-hour audit (`ai/org/dx/2026-09-29-docker-first-hour.md`) timed a newcomer reaching the first authenticated request through
aito.ai/docker. It took 3:17 at agent speed; a careful human should budget 10-15 minutes, plus a 47 s pull. The steps were:
1. generate two keys with openssl;
2. export them;
3. `docker run` with two `-e` flags, a `-v` and two `-p` flags;
4. read the banner;
5. carry the key into code.

Its top traps are key lifecycle:
- a new terminal loses `$AITO_KEY`, and `READ_WRITE_APIKEY=""` silently regenerates the keys;
- a missing `-v` orphans the data;
- keys are echoed to the logs on every boot.

The homepage snippet fails outright.

Many databases ship a CLI that runs the server for you. `pip install aitoai` already puts an `aito` command on the PATH.

## Decision

### Commands

The commands are additive to the existing CLI:

| Command | Does |
|---|---|
| `aito start` | Checks Docker; pulls the **pinned** image; creates or reuses the `aito-state` volume; starts the container on **127.0.0.1**; waits for health; stores the profile; prints one copy-paste block (URL, keys, Python, shell export, curl, psql). Idempotent: rerunning it on a healthy server changes nothing. |
| `aito status` | Container state, `GET /version`, and whether the stored key is accepted. Exits 1 when not ready. |
| `aito logs [-f] [--tail N]` | The container's logs. |
| `aito stop` | Stops the container. The volume, and with it the data and keys, stays; `aito start` resumes. |
| `aito keys [--rotate] [--export]` | Shows the keys, or replaces both. Data is unchanged; the old keys stop working. |
| `aito upgrade [--image]` | Moves to the image this SDK version pins, with the same keys and data. |
| `aito profile [list \| use NAME]` | Lists profiles; selects the active one. |

- **Naming: `aito start`.** Antti delegated this ("follow common practice"), and the CPO decided it on 30.9. Tools that start a
  *background* local server container and return use `start` / `stop` / `status`: `supabase start`, `localstack start`,
  `neo4j start`, `pg_ctl start`, `brew services start`. This command returns once the server is healthy and pairs with `stop`,
  `status` and `logs`, so it is `start`.
- **Reserved: `serve`, `run`, `up`.** All three read as *foreground* operations: `ollama serve`, `npx serve`, `docker run`, and
  `docker compose up`, which stays attached unless `-d`. People and agents will guess them, so each prints a one-line hint and exits
  non-zero (2): "`aito start` starts a local Aito in the background (see `aito start -h`); a foreground mode may come later".
  They're not aliases, so no silent behaviour exists that would have to be kept, and they stay free for a foreground mode (logs
  attached, Ctrl-C stops the server). They aren't in the help listing either.
- **Additive.** These commands are dispatched in `aito.cli.main` *before* the `[cli]` extra is imported, so they run on a bare
  `pip install aitoai`. The existing v1 commands, their names and their behaviour are unchanged, and none of the new names collides.
  The CLI's v1 default through 1.x (the 1.0 promise) is untouched.

### Profiles: one store, shared with `aito configure`

- **Credentials.** `~/.config/aito/credentials` (honouring `XDG_CONFIG_HOME`) is the INI file `aito configure` has always written: one
  section per profile, with `instance_url` and `api_key`. A profile written by `aito start` adds `read_only_api_key` and the container
  it manages (`container`, `volume`, `image`, `port`, `sql_port`).
- **Active profile.** Recorded in `~/.config/aito/config` (`[aito] active_profile`), so settings never share a namespace with profile
  names.
- **Permissions.** Files are `0600` and the directory `0700`. Writes create the file with its final mode and `os.replace` it into
  place, so there's no window where it is readable.
  `aito configure` used to write the same file with the default umask; it now writes it through the same 0600 helper.
- **Activation.** `aito start` makes `local` the active profile only when the currently active one does not exist, or with
  `--activate`, so it never hijacks a configured cloud profile.

### Resolution order: the same for the SDK and the CLI, highest first

1. **Explicit arguments**: `aito.Client(url, key)`, `--instance-url` / `--api-key`.
2. **Environment**: `AITO_URL` (new, the name `aito start` prints) or `AITO_INSTANCE_URL` (kept), plus `AITO_API_KEY`.
3. **The active profile**: `AITO_PROFILE` if set, else `config`'s `active_profile`, else `default`. The CLI's `--profile` overrides it.

The ticket listed env, then the active profile, then explicit args. Explicit arguments stay highest, as in every SDK and in this CLI
since 0.x (documented in its `--help`): an argument you typed must not be overridden by a variable you forgot. The ticket's order is
kept for the other two.

**Pairing rule.** A key is never paired with a URL it was not stored with:
- a URL given alone takes the key of the profile stored for *that* URL, or fails;
- a key given alone fails instead of being sent to whatever the active profile points at.

So after `aito start`, `aito.Client()` just works, and so does `aito.Client('http://localhost:9005')`: `localhost` and
`127.0.0.1` count as the same host when a URL is matched to its stored key.

### Keys

- `aito start` generates both keys itself (`secrets.token_hex(24)`), passes them as `READ_WRITE_APIKEY` / `APIKEY` through a temporary
  0600 env file (never argv, so never the process list), and stores them in the profile.
- No log scraping, so it doesn't depend on the image's banner or on log masking.
- A restart reuses the profile's keys, so they never change unless `aito keys --rotate`.
- **Adoption.** If the volume was first started by a plain `docker run` that generated keys into `/io/state/.aito-api-keys`, `aito start`
  reads them with a throwaway container and adopts them, so clients already using them keep working.
  Verified: the key generated by `docker run` answered 200 against the `aito start`d container.
- If the volume holds no readable keys (the /docker page pins keys by env, and v2.11.1 does not write those to the file), new keys are
  generated and the note says so. With #1528 the image writes pinned keys to the file too, so this case disappears.

### Docker only

- A bundled-JAR fallback needs a JRE 17, and a JVM the user did not choose (a heap, GC and flags this tool would own). The free JAR
  exists.
- Docker is already the documented self-host path, gives state isolation (a volume) and a clean `upgrade`, and is what the audit's
  newcomers used.
- **Decided (CPO, 30.9): Docker-only in phase 1.** A JAR fallback comes back only if data shows Docker-less evaluators.
- `aito start` without Docker says what to install, or that the daemon is not running.

### Pinned image, 127.0.0.1, one volume name

- **Pin.** Each SDK release pins one engine image **by digest** (a tag can be re-pointed; a digest cannot). `aito upgrade` applies a
  newer SDK's pin, and `--image` overrides it. The prototype pins `v2.11.1@sha256:904295cf…`, the audited image.
  **Ship on the first multi-arch release** (v2.11.2, pending Antti's publish yes) and pin its **index** digest: v2.11.1 is amd64-only,
  so Apple Silicon would run it under emulation.
- **Bind.** Both ports are published on **127.0.0.1** only. Docker's `-p 9005:9005` publishes on 0.0.0.0 and bypasses ufw/firewalld.
  - HTTP: 9005.
  - SQL: 5432, or the next free port when a local Postgres holds 5432, which is common on developer Macs (Postgres.app,
    Homebrew). The start output names the chosen port in a note and in its psql line; the start never fails over SQL.
  - **The URL is `http://127.0.0.1:<port>`, never `localhost`**, in the profile and in every printed line (`AITO_URL`, curl,
    psql). macOS, and some Linux setups, resolve `localhost` to `::1` first, and a port published on IPv4 loopback does not
    answer there, so the connection is refused (azure-81's Mac check). The SDK uses the stored URL as it is. A URL a user types as
    `localhost` still finds its stored key.
- **Volume.** `aito-state`, the name aito.ai/docker already uses. A user moving from the page to `aito start` keeps their data.
  aito-core #1533 (azure-81) renames the compose file's volume from `aito-data` to `aito-state`, so the CLI, compose and the docs
  agree. It declares `name: aito-state`, because compose prefixes volume names with the project name: a bare `aito-state:` would
  create `<project>_aito-state`, a different, empty volume. The same PR moves compose's host port from 8080 to 9005 and binds
  127.0.0.1.
- **Managed label.** Containers carry `ai.aito.managed-by=aitoai-cli`. A container `aito start` did not create is never removed or
  stopped; the error names the fix.

### Image contract (azure-81, checked against code and running images)

- `READ_WRITE_APIKEY` / `APIKEY` are read by the **engine** (`Config.scala`), not just the entrypoint. They are the server's contract.
- When both are set, the entrypoint generates nothing.
  With #1528, pinned keys are also written to the key file, and a key change logs one warning, not a failure. This suits `--rotate`.
- `READ_WRITE_APIKEY=""` is FATAL with #1528, and silently regenerates on v2.11.1. The CLI never passes an empty key.
- `GET /version` is 200 without a key. That's load-bearing for the compose healthcheck and aito-core CI, though not a written
  guarantee. `aito start` polls it, then proves the key with an authenticated `GET /api/v2/schema`.
- Ports are 9005 (HTTP) and 5432 (Postgres wire); state lives in `/io/state`.

**Out of scope here.** `Authorization: Bearer` support in the engine goes to a core lane separately. The SDK sends `x-api-key` and needs
no change when Bearer lands.

## Measured: time and steps to the first authenticated query

| Path | Steps to type | Wall clock (image cached) | Traps |
|---|---|---|---|
| Today, aito.ai/docker (audit, 29.9) | 5: two `openssl rand`, exports, `docker run` with 5 flags, read the banner, carry the key into code | 3:17 at agent speed; 10-15 min for a careful human | key lost in a new terminal; empty key regenerates; no `-v` orphans data |
| `aito start` (prototype, 30.9) | **3**: `pip install aitoai`, `aito start`, `aito.Client()` | **9.1 s and 11.5 s** (two fresh runs: install 1.5 s with uv, serve 5.5 / 8.2 s, first query 2.0 s) | none of the above; keys persist in a 0600 profile |

- The cold pull (169 MB compressed, 47 s per the audit) is the same image on both paths, so it is added to both.
- The run was a fresh venv with **no `[cli]` extra**, an empty config directory, and `create_collection`, upload, then `predict`
  through `aito.Client()` with no arguments.
- This was an agent run on Linux. The ticket's fresh-eyes first-hour rerun belongs after the release, against the published package.
  **Untested:** Docker Desktop on macOS and Windows, rootless Docker, podman-as-docker.

Also verified live:
- `status` / `stop` / `start` again (resumes);
- `keys --export`;
- `keys --rotate` (the old key stops working, the data stays, `Client()` follows);
- `upgrade`;
- `logs`;
- a second profile on other ports (`--profile adopt --port 19005`), selected with `AITO_PROFILE`;
- key adoption from a plain-`docker run` volume;
- a real port conflict, which is refused with the fix named.

Offline tests: `tests/sdk/test_local_profiles.py` covers resolution order, pairing, 0600, profile preservation, `Client()` with no
arguments, `aito start -h` with the `[cli]` extra unimportable, and `serve` / `run` / `up` each giving the hint and a non-zero exit without being listed.

## Consequences

- The keys are visible to anyone who can run `docker inspect`, because the image takes keys by env. On a single-user machine that's
  the same person who can read the profile.
- The profile is plaintext, 0600, like `~/.pgpass`, `~/.aws/credentials` and `~/.docker/config.json`. OS keychains are a later option.
- The SDK now reads `~/.config/aito` when constructed with no arguments. With arguments, nothing changes.

## Phase 2 (a sketch, not a build): `aito login` / `aito register`

The shape:
- `aito login` is an OAuth device flow against console.aito.ai.
- `aito register` links a self-hosted instance, by the image's `instanceId`, to the user's console account.
- The console then shows it with its keys, health and UI; a licence bought on aito.ai binds to it (`aito license set`).
- For production there is no interactive CLI: a declarative `AITO_REGISTRATION_TOKEN` env var in the container registers it on boot.

Open questions for **Antti** and **mission-control**:

1. **What register sends, and how often.** One link event, or a heartbeat (version, row counts, health)? This interacts with the
   telemetry and licence decision (td-20260929225128511425, §4(b)/§5). An explicit, opt-in link is cleaner than implicit telemetry.
   Is register *the* telemetry consent?
2. **How the console reaches a local instance.**
   - Browser to `localhost` (CORS plus the Private Network Access preflight; works only on the same machine).
   - A relay: the instance dials out and the console talks to the relay (reachable anywhere, but a new service with its own security
     surface).
   - Metadata only: the console shows what register pushed, never live data.
3. **Keys in the console.** Should the console ever hold a self-hosted instance's keys? It could show only "key set, last rotated"
   and have the CLI rotate them.
4. **Licence binding.** Bind to the `instanceId` (moves with the volume), to the account (any instance), or to a host fingerprint?
   What happens offline or on air-gapped hosts: a signed licence file (`aito license set FILE`) that needs no call-home?
5. **Identity.**
   - Is `aito login` the same identity as console.aito.ai today?
   - Where does the token live: the profile, the OS keychain, or both?
   - How long does it last?
6. **Production.** Does `AITO_REGISTRATION_TOKEN` register once or on every boot? Can it be revoked from the console? What does a
   cloned volume (two instances, one `instanceId`) do?
7. **Scope for 1.x.** Should `aito login` also select a cloud database (writing a profile, like `aito configure`), so the same profile
   mechanism covers cloud and local?

Recommendation: decide 1, 2 and 4 before any build. They set whether phase 2 is one endpoint in the console or a relay service.
