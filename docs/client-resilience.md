# Retries, timeouts and partial uploads in the SDK clients

An audit of what the clients do when a request fails, as of 6.11.2026 (master after #103, engine
v2.12.0). It answers a go-live checklist: retries, timeouts, and whether a failed upload can
safely be repeated.

## What each failure does

| Failure | v1 `AitoClient` (sync) | v1 async (`async_request`, `batch_requests`) | v2 `AitoClientV2` |
|---|---|---|---|
| 409 `write.contention` | retried, 3 by default (`write_contention_retries`) | retried, same | retried, same |
| 429 | raised (`RequestError`) | raised | retried, 3 by default, at most 60 s of waiting (`rate_limit_retries`) |
| 502 / 503 / 504 | raised | raised | retried, 2 by default (`retries`), for requests that change nothing |
| connection refused / reset, connect timeout | raised | raised | retried, as above |
| read timeout | after 300 s (`timeout`), raised | after 300 s in all (aiohttp's default) | after `timeout` (30 s), raised; retried only with `retry_read_timeouts=True` |
| any other 4xx / 5xx | raised | raised | raised |

`AitoV2Error.is_retryable` says whether repeating the same request later can succeed and is
safe: true for 409 `write.contention` and 429, false for everything else. That includes a
503, because a write answered 503 may or may not have been applied.

## Timeouts

- **v1 sync:** `AitoClient(timeout=300.0)` by default since 1.3.1, the credential check
  included; `client.timeout` can be changed between calls. A request that runs out raises a
  `RequestError`. `timeout=None` waits forever, as every version before 1.3.1 did.
- **v1 async:** aiohttp's default session timeout, 300 s in all and 30 s to connect. It can't be
  set.
- **v2:** `AitoClientV2(timeout=30.0)` per client; `client.timeout` can be changed between calls.
  Every query method also takes a per-call `timeout=` that overrides the client's for that
  call: `request`, `query`, `search`, `predict`, `recommend`, `relate`, `match`, `estimate`,
  `aggregate`, `batch`, and `evaluate` (600 s by default).

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
  It raises only if no batch at all succeeded, and an upload with some batches failed ends
  with an ERROR naming their row ranges. `raise_on_failure=True` stops at the first failed
  batch instead (see the end of this section).
- **v2 `AitoClientV2.upload_entries`** raises at the failed batch, with the same `AitoV2Error`
  as any other failure. The batches before it have landed. The error says how far the upload
  got:
  - `batches_landed`;
  - `rows_landed`, the rows of those batches;
  - `failed_batch`, its 0-based index.

  - `failed_batch_outcome`:
    - `'refused'` (a 4xx): none of the failed batch landed, and `rows_landed` is exact;
    - `'unknown'` (a 5xx, a reset, a timeout): the failed batch may have landed on the server
      even though the client saw an error, possibly after the client gave up, so
      `rows_landed` is a lower bound.

  All four are `None` on any other error.

### Resuming a failed upload (v2)

**Is a v2 batch atomic?** Measured atomic for 1000-row batches: each landed whole or not at
all. The engine commits in flushes of 50,000 rows, so a batch of up to 50,000 is expected to be
atomic but isn't measured, and larger batches may land in parts.
- **Refusals** (checked on v2.11.2 and v2.12.0): a batch with one bad row, such as a missing key
  or two rows with the same key, is refused whole, and none of its rows land.
- **Crashes and timeouts** (measured by core-3 on 6–7.11.2026, board ticket
  td-20261107000050914187): 20 SIGKILLs across 517 batches, and client timeouts across 614
  batches. Every batch landed with all of its rows or none, and every acknowledged batch
  survived a restart. The batches were of 1000 rows, the client's default `batch_size`.
- **Bigger batches:** the 50,000-row flush is from the engine's limits page (`/v2/limits`,
  "Data writes"; raised with `?batchSize=`). Measuring above it is board ticket
  td-20261113082157454471. Until then, keep `batch_size` at the default if you rely on
  atomicity.

**But a timed-out batch can land late.** In the same runs, a batch the client had given up on
still committed afterwards: the row total held still for 15 s, then grew by 13 whole batches
when the engine drained them at shutdown. So waiting, or counting the table, can't show that
an `'unknown'` batch didn't land.

After `'refused'`, resending `rows[err.rows_landed:]` is exact. After `'unknown'`, the only
safe resend is an upsert into a collection with a primary key.

**Into a collection with a primary key: resend the rest as an upsert.** Every row that's already
there is replaced by itself, so the batch that may have landed can't duplicate:

```python
client.create_collection('invoices', columns, primary_key='id')
try:
    client.upload_entries('invoices', rows, on_conflict='update')
except AitoV2Error as err:
    if err.rows_landed is None:
        raise
    client.upload_entries('invoices', rows[err.rows_landed:], on_conflict='update')
```

With the client's `retries` (2 by default), the upsert batches are also retried through
502/503/504, resets and connect timeouts before the error is raised at all, and through read
timeouts too with `retry_read_timeouts=True`. In core-3's measurement, 40 batches of 1000 rows
resent with `on_conflict=update` after a 0.02 s timeout gave exactly 40,000 rows; the same
resend into a keyless collection gave 72,000.

**Into a collection without a key:**
- `'refused'`: none of the failed batch landed, so resend `rows[err.rows_landed:]` after fixing
  what the error names.
- `'unknown'`: no resend is safe. The batch may land after any check you make, and then a
  resend duplicates it. Either load into a fresh collection that has a primary key and resend
  with `on_conflict='update'`, or load the whole upload into a fresh table and swap it in.

A v1 `api.upload_entries` goes on past a failed batch by default: each failed batch is logged at
ERROR with its row range, and an upload that finished with some failed ends with an ERROR naming
them all. It raises only when no batch landed. Since 1.3.1, `raise_on_failure=True` stops at the
first failed batch instead, with an `UploadEntriesError` whose `rows_uploaded` says where to
resume (`entries[rows_uploaded:]`) and whose `failed_range` is the batch's rows.

## Transient retries (v2)

`AitoClientV2(retries=2, backoff=0.5)` repeats a request up to `retries` times after a 502, 503
or 504, a dropped connection or a connect timeout. Retry k waits up to `backoff * 2**k` seconds
(full jitter, at most 8 s), on top of a `Retry-After` the response carries.

Both are the defaults since 1.3.1 (opt-in in 1.3.0); `retries=0` turns them off. A read
timeout (the server didn't answer within `timeout`) is not retried by default: a server too
busy to answer in time would get the same work again, and a 30 s timeout would become about
90 s. `retry_read_timeouts=True` retries it too. Only requests that change nothing are
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

- **v1 `upload_entries` raising on the first failed batch by default.** It's opt-in
  (`raise_on_failure=True`) in 1.x; making it the default waits for 2.0, since some uploads
  that complete today would raise.
- **The same opt-in retries for the v1 clients.** That one isn't a default change, but v1 is
  legacy, so it's left until someone needs it.
