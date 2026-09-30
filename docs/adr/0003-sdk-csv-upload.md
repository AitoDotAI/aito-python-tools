# ADR 0003: `Client.upload_csv`, a CSV into a v2 collection in one call (design note)

- Status: **implemented** on branch `feat/upload-csv` (local), after the CPO answered the open questions (below).
- Date: 30.9.2026
- Source of the need: both first-hour audits (29.9 Docker, 30.9 `aito start`) hit the same wall. There is no CSV path in the SDK, and
  the obvious workaround (`csv.DictReader` → `upload_entries`) creates every column as `String`. Then:
  - a free-text description predicts nothing: Cloud 0.199, against Office 0.94 with a typed schema;
  - a numeric column fails with `Encountered VALUE_STRING`.
- Depends on aito-core #1535 (open), which gives `/import` `text/csv` and infers free text as `Text`. This design gives the same
  result on engines without #1535, so it doesn't wait for it.

## The call

```python
import aito
client = aito.Client()
res = client.upload_csv('invoices', 'invoices.csv')
res.rows          # 200
res.inferred      # {'invoice_id': 'String', 'amount': 'Decimal', 'description': 'Text', 'category': 'String', ...}
res.via           # 'server' (the engine parsed it) or 'client' (parsed here, same rules)
res.warnings      # e.g. ["description: inferred as Text (free text); declare a schema to choose an analyzer"]
client.predict(from_table='invoices', where={'description': 'printer toner'}, predict='category')
```

The signature:

```python
def upload_csv(self, name, source, *, schema=None, via='auto', delimiter=',', encoding='utf-8',
               batch_size=1000) -> CsvUploadResult
```

- **`source`** is a path (`str` / `PathLike`), an open text or binary file, or the CSV content as `bytes`.
  A plain `str` is always a path. Content has to be passed as bytes, so a mistyped filename can't silently become a one-row CSV.
- **The collection doesn't exist:** it's created, and its types are inferred as below. `schema` (the `create_collection` column map)
  overrides the inference for the columns it names, which is how an analyzer is chosen.
- **The collection exists:** the rows are appended. Each cell is converted to that column's declared type, and a cell that doesn't
  fit fails **before anything is sent**, with the row, column and fix named:
  `row 17, column 'amount': 'twelve' is not a Decimal`. That replaces the engine's `1:11 … VALUE_STRING`.
- **`via`**:
  - `'auto'`: the server path when the engine accepts `text/csv`, otherwise the client path.
  - `'server'` or `'client'` forces one. `'client'` is for testing and for an engine whose inference you don't want.

## The two paths

**Server (#1535 and later).** `POST /api/v2/data/{name}/import` with `Content-Type: text/csv` and the file as the body.
- The engine parses, types, infers Text, and returns `inferred` and warnings.
- `request()` gains a private raw-body option: today it only sends JSON.
- **Detection without a version table.** An engine before #1535 parses a `text/csv` body as JSON. Observed on v2.11.1, it answers:
  - 400 `json.malformed` (`Unrecognized token 'invoice_id'…`) for an unquoted header;
  - 400 `data.bad_request` with **exactly** `Expected JSON array for import` for a quoted header (`"a","b"`).

  Only those two answers mean "no CSV support", and `auto` then takes the client path.
  - #1535 reports a real CSV problem as `data.bad_request` with `CSV import: …`, and a bad row as `import.failed`. Both are raised,
    **never retried**, with a test for each direction.
  - `data.bad_request` is shared with the old engine's quoted-header answer, so the match on the exact old message is load-bearing.
    **Asked of core:** give #1535's CSV errors a code of their own (for example `import.csv_invalid`).
  - The CSV is parsed here before anything is sent, so a malformed file fails locally on either engine. The server path only ever
    receives files that parse.
  - The path taken is in `result.via` and in a DEBUG log line on `aito.v2`.
- One request per call. The engine's request-size limit applies, and a too-large body is reported with "use `via='client'`, which
  batches".

**Client (any engine).** Standard library only: the `csv` module, no pandas. So `upload_csv` works on a bare `pip install aitoai`, and
`import aito.v2` stays light.

It reproduces **#1535's rules exactly**, so both paths give the same collection:

| Step | Rule (from aito-core #1535, `CollectionDbCsvImport` / `DocumentType.StringStats`) |
|---|---|
| Parse | Header row required; comma, `"` quotes, doubled quotes, quoted newlines. An empty header cell, a duplicate header or a row with the wrong cell count is an error with the row number. |
| Missing | An empty unquoted cell is a missing value: the field is left out of that row, and the column becomes nullable. |
| Column kind, read from **all** its non-missing cells | Any cell with a leading zero (`len > 1`, starts with `0`, not `0.`) → String. All match `^-?[0-9]+$` and fit a 64-bit long → Int (Long if outside 32-bit). All match `^-?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$` → Decimal. All `true`/`false` (any case) → Boolean. Otherwise String. |
| Text | A String column is Text when, over its values: `n > 0`, more than half are multi-word (`value.strip()` contains a space), the average length is `> 15`, and the distinct count (capped at 64) is `> min(20, n // 2)`. Text gets `{"type": "Text"}` with **no analyzer**, as the engine does. |

On the client path the collection is created with those types (`create_collection`), and the typed rows go in batches of
`batch_size` through the existing `upload_entries`. **The rules live in one module** (`aito/_csv_types.py`) with a public
`infer_csv(source) -> {column: type}`, so a user can see the types before uploading. A shared test vector file of boundary cases
(15 vs 16 characters on average, exactly half multi-word, 20 vs 21 distinct values, `00100`, `0.5`, `-0`, `1e3`, `TRUE`) runs
against the Python rules here and against the engine through the server path in the live suite. If the engine's rule ever moves,
that test goes red.

## Deliberately not in this

- **pandas / DataFrames.** `upload_entries(df.to_dict('records'))` already works for someone holding a DataFrame, and
  `aito.utils.DataFrameHandler` exists behind the `[cli]` extra. `upload_csv` is for the newcomer holding a file.
- **Other formats** (TSV and `;`-separated are covered by `delimiter`; Excel and JSON lines are not). **Streaming files beyond
  memory:** the client path reads the whole file, because the column-kind rule needs every cell. The note in the docstring gives the
  size where to batch by hand.
- **The CLI.** `upload-file` is v1. With #77's `--api-version v2`, a follow-up can route `upload-file` for a `.csv` through
  `upload_csv`. That's also the fix for the audit's CLI dead end on a local engine.
- **Analyzer choice.** No analyzer is inferred, the same as the engine; picking one is `schema=`. Whether Text should default to a
  language analyzer is core-a's open question, and the SDK follows the engine.

## Decided (CPO, 30.9)

1. **The name is `upload_csv`**, matching the SDK's `upload_*` family.
2. **The v1 `upload-file` warns** when a String column of the target table receives free text, using the same rule. It's a warning
   on `aito.cli`; the declared type is kept.
3. **The client path stays permanently**, for older engines and for the free image's lag behind core.

## As implemented

- **Appends to an existing collection always take the client path.** The declared types are applied here, so a bad cell is named
  before anything is sent.
- **`schema=`, or a delimiter other than a comma, also takes the client path,** because that's where they apply.
  `via='server'` with either one is refused.
- **Live, v2.11.1 (no #1535):** a 200-row invoice CSV went `via='client'` and `description` was inferred as Text. Predicting
  `category` from a new description gave **Office 0.932**. The 29.9 audit's `String` workaround got 0.199 on its data.
  - A quoted-header CSV also fell back.
  - An append converted to the declared types.
  - A bad cell failed as `row 2, column 'amount': 'twelve' is not a Decimal`.
- **The engine side of the shared boundary vectors runs once #1535 is in a released image;** until then only the Python rules are
  tested.
