# The v2 client: design decisions

Written while implementing `aito.client.v2` (todo `td-20260815095526935128`,
"[aito-python-tools] Add v2 support to the Python SDK").

This file records the decisions a stranger reading `aito/client/v2/` would
otherwise have to reverse-engineer. It is the answer to task item (2) of the
ticket: *"Decide and WRITE DOWN whether v2 is a new client class or a flag on
the existing one — that choice is the API a stranger sees."*

## Where this design came from

Not from the spec. Two v2 clients were hand-rolled in the tree before this SDK
existed, and per the ticket the harvest starts by diffing them:

| | `aito-erp-demo/src/aito_client.py` | `aito-accounting-demo/src/aito_v2_client.py` |
|---|---|---|
| Shape | ONE client, `api_version` flag | SEPARATE `AitoV2Client` class |
| Endpoints | verb endpoints on both versions (`/_predict`, `/_relate`, …) | everything through `/_query`, except `_evaluate` |
| Normalisation direction | v1 → v2 ("the destination, not the legacy") | v2 → v1 (drop-in for existing v1 callers) |
| Errors | string-match the message body | string-match the message body |
| Env routing | none | `/env/<name>` path segment |
| Transport | pooled `httpx.Client` | pooled `httpx.Client`, + retry, + semaphore |

**What is common is what is actually needed** (the ticket's rule):

- a pooled connection with an `x-api-key` header;
- a typed exception carrying HTTP status *and* body;
- `$value` (not v1's `feature`) as the predicted-value key;
- `relate` takes a **list** of fields in v2, a bare string in v1;
- `_evaluate` is its own endpoint on both, and its v2 payload is enveloped;
- an `optimize` step after bulk load, or `predict` is degraded.

**What differs is a real disagreement worth resolving.** Three were resolved
against the engine source (`~/episto/src/_branches/aito-core-3`,
`core/server/.../apis/v2/ApiV2EndPoints.scala` + `core/docs/v2-response-format.md`)
and against a live instance, not by picking a favourite:

1. **Named endpoints exist in v2 and are worth using.** The accounting demo's
   "v2 has no separate `_search`; it's `_query`" is out of date. A3 shipped:
   `_predict` / `_search` / `_recommend` / `_relate` are *enforced shortcuts*
   for `_query` — the server validates that the body matches the mode and
   returns a `400` naming the right endpoint on a mismatch. Verified live:

   ```
   POST /api/v2/_predict  {"from":"products","limit":1}
   → 400 {"kind":"error","data":{"code":"request.invalid",
          "message":"the _predict endpoint expects a 'predict' query, but got a
                     rows/search body. Post to _query for a general query."}}
   ```

   That check is free error-detection, so the SDK addresses named endpoints and
   keeps `query()` as the documented escape hatch.

2. **Normalise toward v2, not toward v1.** The ERP demo is right here: v2 is
   the destination. The SDK does *not* alias `$value` back to `feature`.

3. **Do not string-match errors.** Both demos do, because they predate the
   structured `error` kind. It has shipped, so the SDK exposes
   `AitoV2Error.code` (`not_found`, `request.invalid`, `query.invalid`,
   `proposition.unsupported`, `operation.unsupported`, `json.malformed`,
   `internal`, plus the schema/data codes) and callers branch on that.

## Decision 1 — a separate class, not a flag

**`AitoClientV2` is a new class in a new `aito.client.v2` subpackage. The v1
`AitoClient` is untouched.**

The ERP demo's flag works well *for the ERP demo*, because it normalises both
versions onto one shape and its services only ever use five verbs. An SDK
cannot make that trade: it has to expose both surfaces honestly, and the
surfaces genuinely differ.

Four reasons, in order of weight:

1. **The response types differ, so a flag makes every accessor ambiguous.**
   `EstimateResponse.estimate` reads `json['estimate']` on v1 and
   `json['data']['value']` on v2 (the scalar was deliberately renamed —
   `v2-response-format.md` §11 slice 3). A version flag would make every such
   property a two-branch conditional whose correct branch depends on state set
   in the constructor. Separate types make the shape a compile-time-ish fact.

2. **`/api/v1` is hardcoded into the existing request hierarchy's validation.**
   `AitoRequest._api_version_endpoint_prefix` feeds `_check_endpoint`,
   `QueryAPIRequest._endpoint_pattern`, `_SchemaAPIRequest.endpoint_prefix` and
   `DataAPIRequest.endpoint_prefix`, and `AitoRequest.make_request` dispatches
   by matching those patterns. Threading a version through all of it would edit
   the dispatch of a shipped, released v1 surface to add a second one — the
   riskiest possible way to spend this ticket.

3. **v2 has addressing v1 does not.** Environments are a URL path segment
   (`/db/<db>/env/<name>/api/v2/…`). There is nowhere to put that on a v1
   client, and the two traps around it (below) need a home.

4. **It matches the retirement story.** When v1 goes, `aito/client/v2/` is
   promoted and `aito/client/` is deleted — rather than unpicking `if
   self._api_version == 'v1'` from inside every method.

Cost, stated plainly: two client classes to document, and a caller migrating
from v1 changes an import and one constructor call. That is a smaller and more
visible break than silently changing what `EstimateResponse.estimate` means.

## Decision 2 — the response envelope, and the trap in it

`v2-response-format.md` §3 gives clients this contract: **`kind ?? "rows"`**.
Rows responses stay bare (`{offset, total, hits}`, no `kind`) for v1
compatibility; everything else carries an explicit `kind` and puts its payload
under `data`.

**That contract is not sufficient, and following it literally produces a bug.**
The non-rows shapes are produced by the *rep2-native* builders only; a legacy
`type: table` served through the v2 endpoint falls to the rep1 compat shim,
which returns the bare v1 shape with no `kind`. Verified live on the same
build (`3de8f4f7ede5`, built 2026-08-27), same endpoint, two shapes:

```
POST /api/v2/_estimate  {"from":"products",…}      (legacy table)
→ {"estimate": 0.754…, "why": {…}}                  ← no kind, v1 shape

POST /api/v2/_estimate  {"from":"sdk_v2_probe",…}  (v2 collection)
→ {"kind":"estimate","data":{"value": 325.79…}}     ← enveloped
```

`kind ?? "rows"` classifies the first one as `rows`, and the caller then reads
`.hits` off a response that has none.

So the SDK dispatches on **the requested operation** (which it knows, because
it chose the endpoint) and treats the envelope as optional:
`_unwrap(json, kind)` returns `json['data']` when `json['kind'] == kind`, the
bare object when there is no `kind`, and raises when the `kind` is a *different*
one — a genuine mismatch. `EstimateResponse.value` then reads `value` with
`estimate` as a fallback, and works on both paths. This is filed as a bug
(see the ticket report); the adapter is deliberately one function so that when
the engine unifies the shapes, one edit removes it.

## Decision 3 — warnings are surfaced, not swallowed

v2 has a `warnings` channel (`{code, message, severity, field}`) that the demos
predate and neither reads. It is the only in-band signal for the class of
failure that hurt the accounting migration worst: **a query the server silently
broadened**. V2-13 was a disjunctive tenant filter dropped without error,
returning another tenant's rows with a `200`.

Every `V2Response` therefore exposes `.warnings`, and the client takes
`on_warning='ignore' | 'log' | 'raise'` (default `'log'`, via
`logging.getLogger('AitoClientV2')`). `'raise'` exists so a multi-tenant caller
can make "the server changed my query" a hard failure.

Verified live:

```
POST /api/v2/_query {"from":"sdk_v2_probe","where":{"no_such_col":"x"},…}
→ 200 {"offset":0,"total":0,"hits":[],
       "warnings":[{"code":"where.unknown_field","severity":"warning",
                    "field":"no_such_col","message":"…possible typo…"}]}
```

## Decision 4 — env addressing owns its two traps

`env=` builds `<base>/env/<name>/api/v2`. Two mistakes cost the accounting
migration a session each, so the client rejects both locally with a message
naming the fix rather than letting the server answer confusingly:

- **`env.` prefix** (V2-11). `env.` became reserved; a stored `env.v2-demo`
  started returning `400 … reserved`. `AitoClientV2(env='env.foo')` raises
  `ValueError` up front.
- **the dotted path** (V2-3). `…/db/<db>/env.<name>/api/v2` misses the `/env/`
  segment. It used to fall through to the master permission check and answer
  `403`, which reads as an auth problem; the engine now 404s it
  (`V2ErrorSurfaceTest/addressing-404s`). The client only ever builds the
  two-segment form, so it cannot be reached by accident.

## Decision 5 — `requests`, not `httpx`

Both demos use `httpx`. The SDK already depends on `requests` (and `aiohttp`
for the async path) and ships to PyPI as `aitoai`; adding a third HTTP library
to every install to match a demo's preference is not a trade worth making. The
pooling both demos care about is `requests.Session`, which is what
`AitoClientV2` holds.

## What is deliberately NOT here

- **No v1↔v2 shape translation.** The demos translate because they have live v1
  callers. An SDK that quietly renamed v2's keys to v1's would teach the wrong
  vocabulary to every new integration.
- **No retry.** The accounting demo retries once on 5xx. Retrying a
  non-idempotent write by default is a policy decision that belongs to the
  caller, not the transport.
- **No `_match` convenience method.** `_match` cannot rank on v2 (V2-12: hits
  carry a raw `$f`, never `$p`, and every candidate ties at 0 for unseen
  input). `query()` reaches it; wrapping it in a named SDK method would
  advertise a working feature.
