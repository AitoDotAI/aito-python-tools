# Retries, timeouts and partial uploads in the SDK clients

An audit of what the clients do when a request fails, as of 6.11.2026 (master after #103, engine
v2.12.0). It answers a go-live checklist: retries, timeouts, and whether a failed upload can
safely be repeated.

## What each failure does

| Failure | v1 `AitoClient` (sync) | v1 async (`async_request`, `batch_requests`) | v2 `AitoClientV2` |
|---|---|---|---|
| 409 `write.contention` | retried, 3 by default (`write_contention_retries`) | retried, same | retried, same |
| 429 | raised (`RequestError`) | raised | retried, 3 by default, at most 60 s of waiting (`rate_limit_retries`) |
| 502 / 503 / 504 | raised | raised | raised; with `retries=n`, retried for requests that change nothing |
| connection refused / reset | raised | raised | raised; with `retries=n`, as above |
| read timeout | never happens: no timeout | after 300 s in all (aiohttp's default) | after `timeout` (30 s); with `retries=n`, as above |
| any other 4xx / 5xx | raised | raised | raised |

`AitoV2Error.is_retryable` says whether repeating the same request later can succeed and is
safe: true for 409 `write.contention` and 429, false for everything else. That includes a
503, because a write answered 503 may or may not have been applied.

## Timeouts

- **v1 sync: none.** `requests` is called without `timeout`, so a request to an engine that
  accepts the connection and never answers waits forever. You can't set one per client or per
  call.
- **v1 async:** aiohttp's default session timeout, 300 s in all and 30 s to connect. It can't be
  set.
- **v2:** `AitoClientV2(timeout=30.0)` per client; `client.timeout` can be changed between calls.
  A per-call `timeout=` exists on `request`, `query`, `evaluate` (600 s by default) and `batch`,
  but not on `search`, `predict`, `recommend`, `relate`, `match`, `estimate` or `aggregate`.

## Are uploads safe to repeat?

**Only into a v2 collection with a primary key, sent with `?on_conflict=update`.** Checked on
v2.12.0:

- **A v1 table, or a v2 collection without a primary key:** the batch insert appends. The same
  rows sent twice are stored twice: two rows became 4, then 6, through
  `POST /api/v1/data/{t}/batch` and then `/api/v2/data/{t}/batch`.
- **A v2 collection created with a top-level `"primaryKey": "id"`:** the key is kept and
  enforced. A repeated row is refused with 400 `data.bad_request` ("primary key 'id' = 1 already
  exists ... use on_conflict=update to upsert"). `POST /api/v2/data/{t}/batch?on_conflict=update`
  upserts, so resending it is safe.
- **Silently dropped, with no error or warning:** a top-level `primaryKey` in a v1 schema
  (`PUT /api/v1/schema/{t}`), and a column-level `"primaryKey": true` in a v2 schema. Both answer
  200 and create a table with no key.

Other engine facts, checked on v2.11.2 and v2.12.0:
- A batch is all or nothing: one bad row (say, two rows with the same key) fails it, and none
  of it lands.
- `on_conflict` takes `error` (the default), `update` or `ignore`. On a collection without a
  key, `update` and `ignore` are refused with 400 ("requires a declared primaryKey"), so they
  can never append twice.

In the v2 SDK:

```python
client.create_collection('invoices', columns, primary_key='id')   # or a list: a composite key
client.upload_entries('invoices', rows, on_conflict='update')      # resending is safe
```

Into a table without a key, a batch whose response was lost (a reset, a timeout, a 503) may
have landed, and sending it again can duplicate it. No client retries such a batch. The 409
`write.contention` retry is safe because the engine commits nothing before answering it.

**A failed batch mid-upload:**
- **v1 `api.upload_entries`** logs the failed batch as an error and carries on with the next.
  It raises only if no batch at all succeeded. A partial upload returns normally, and the
  caller can't tell from the return value which rows are missing (the log names the batch's
  index range).
- **v2 `AitoClientV2.upload_entries`** raises at the failed batch. The batches before it have
  landed, and the count of those isn't in the error.

To resume after a failure into a table without a key, a caller has to find out what landed,
e.g. by querying the batch's ids or counting the table, rather than resending blindly.
Alternatively, it loads into a fresh table and swaps it in. With a keyed v2 collection, it can
resend the upload with `on_conflict='update'`.

## Opt-in transient retries (v2)

`AitoClientV2(retries=n, backoff=0.5)` repeats a request up to n times after a 502, 503 or 504, a
dropped connection or a timeout. Retry k waits up to `backoff * 2**k` seconds (full jitter, at
most 8 s), on top of a `Retry-After` the response carries.

The default `retries=0` keeps the old behaviour. Only requests that change nothing are
repeated:
- GETs;
- the queries `_query`, `_search`, `_predict`, `_recommend`, `_relate`, `_match`, `_estimate`,
  `_aggregate`, `_evaluate` and `_batch`;
- `optimize`;
- a batch sent with `on_conflict=update` or `ignore` (`upload_entries(..., on_conflict=...)`):
  the same rows land once however often it's sent.

  This relies on the engine refusing `update` and `ignore` on a collection without a key
  (checked on v2.11.2 and v2.12.0). The client doesn't look up the schema before retrying, so
  an engine that accepted them there would make the retry duplicate rows. The live suite pins
  the refusal on every CI engine
  (`test_on_conflict_without_a_primary_key_is_refused`), so such an engine fails CI first.

Never a plain batch insert (or one with `on_conflict=error`), `import`, `stream`, a delete, a
schema write, `_modify`, an env or a job submission. Tests: `tests/sdk/test_transient_retry.py`.

## Not changed here: defaults, for Antti

- **A default timeout for the v1 sync client**, which has none. Adding one makes a slow call
  that succeeds today fail.
- **v1 `upload_entries` raising on the first failed batch** instead of returning a partial
  upload.
- **Transient retries on by default.**
- **The same opt-in retries for the v1 clients.** That one isn't a default change, but v1 is
  legacy, so it's left until someone needs it.
