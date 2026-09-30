# ADR 0003: `Client.upload_csv`, a CSV into a v2 collection in one call (design note)

- Status: **draft**. No code until this is agreed.
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
- **Detection without a version table:** an engine before #1535 answers a `text/csv` body with 400 `json.malformed`. That exact code
  means "no CSV support", and `auto` then retries on the client path with the same bytes. Any other error (a real CSV problem, auth)
  is raised as it is.
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

## Open questions

1. **Name:** `upload_csv` matches `upload_entries`, while `import_csv` matches the endpoint. This note proposes `upload_csv`.
2. **Should the CLI's v1 `upload-file` warn** when it creates String columns out of prose, until #77 ships?
3. **Once #1535 is in every supported engine**, should the client path stay, as the offline and `via='client'` path, or go? This
   note proposes it stays: it's small, and it's the only place the rules are testable without an engine.
