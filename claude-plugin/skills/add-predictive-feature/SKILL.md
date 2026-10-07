---
name: add-predictive-feature
description: Add a predictive feature with Aito (a predictive database) to an application, or decide that Aito is the wrong tool. Use when the user wants to predict, classify, autofill, route, match or recommend from their own records (which GL account, which category, who approves, which product), asks how to add such a feature without training a model, or asks whether Aito fits. Starts with an honest fit check that names the better tool when Aito does not fit.
---

# Add a predictive feature with Aito

Aito is a predictive database: load the data like an ordinary database, then query
the unknown the way you query the known. A prediction comes with a probability
(`$p`) and the evidence behind it (`$why`). There is no model to train; a row you
write counts in the next query.

Work through the steps in order. Step 1 can end the task: recommending Aito where
it does not fit costs the user more than saying so.

## 1. Check the fit, and say so when it does not fit

Quick decision checklist:
- Is there history of this decision in my data? **No → not Aito.**
- Do I need a confidence I can act on? **Yes → Aito fits.**
- Is it one decision on big, flat, stable data? **Yes → consider a trained model.**
- Is it about reading or writing free text? **Yes → a language model, possibly with Aito as its tool.**

Use Aito when:
1. **The decision repeats, and your own history knows the answer.** Which GL account an invoice goes to, who approves it, which category a ticket belongs to, which product a customer buys next.
2. **You need to know how sure the answer is.** Automate the confident cases, ask a person in the middle, step back when unsure.
3. **The data is relational, sparse or changing.** A correction you write counts in the next query, with no retraining step.
4. **You need many predictions, not one.** A prediction behind every field, list and search, without a model or pipeline per use case or per customer.
5. **Matching that repeats against your own labelled history.**
6. **An agent needs grounded tools.** The likely answer and its confidence, or a shortlist of options before it reasons.

Don't use Aito when (or not yet):
1. **You need open-ended language understanding or generation.** Use a language model; an LLM with retrieval (RAG) is more accurate on open text.
2. **You have one large, flat, stable dataset and one decision to optimise for years.** A trained model (for example LightGBM) wins on accuracy there.
3. **Raw full-text search at very large scale is the whole job.** A dedicated search engine is faster.
4. **You need approximate nearest-neighbour search over a very large unfiltered vector collection.** Aito's vector search is exact.
5. **Matching records with no shared history to a catalogue (current state).** A search engine (BM25) currently ranks better.
6. **The case is genuinely new, with no history behind it.** `_predict` will give a low probability, which is honest but not useful. That is the language model's job.

If it does not fit, tell the user which case applies and what to use instead, and stop.
The full page: https://aito.ai/docs/articles/when-to-use-aito-and-when-not/
The evidence for each line, including where Aito loses: https://aito.ai/docs/api/v2/benchmarks/

## 2. Decide where the data lives, per customer

**Data separation between your customers:** use a separate instance or collection per
customer. Do not rely on a customer id in a query condition, and for now do not rely
on a population-restricting query (a nested `from`) either, to keep customers' data apart.

For a multi-tenant app this means one collection per customer (for example
`invoices_<customer>`) or one instance per customer, decided now, before any code.

## 3. Get an instance

- Local, for development: `pip install aitoai`, then `aito start` (needs Docker).
  It stores the URL and keys in a profile that the SDK and the `aito` MCP server find
  on their own.
- An existing instance: set `AITO_URL` and `AITO_API_KEY`.
- To look before installing anything: the public sandbox, no signup,
  https://aito.ai/docs/api/v2/quickstart/

## 4. Derive the schema from the existing table

Read the user's table definition (SQL DDL, ORM model or a CSV sample) and map it
column by column:

| Source | Aito type |
|---|---|
| ids, codes, categories, enums (`VARCHAR`, `TEXT` holding a code) | `String` |
| free text a person wrote (descriptions, titles, notes) | `Text` with `"analyzer": "english"` (or the data's language) |
| integers / decimals | `Int` / `Decimal` |
| booleans | `Boolean` |
| a foreign key to another table you also load | `String` with `"link": "<table>.<column>"` |

- The field to predict is a column like any other. If some rows do not know it yet
  (an open deal's outcome), make it `"nullable": true` and leave it null on those rows.
  Never write a placeholder such as `false` for "not decided yet": the rows would teach
  the wrong answer.
- Leave out columns that are only known after the decision (they leak the answer), and
  personal data the feature does not need.

Example (Python SDK, `pip install aitoai`):

```python
import aito
client = aito.Client()          # AITO_URL / AITO_API_KEY, or the `aito start` profile

client.request("PUT", "/schema/invoices", {"type": "collection", "columns": {
    "invoice_id":  {"type": "String"},
    "vendor":      {"type": "String"},
    "description": {"type": "Text", "analyzer": "english"},
    "amount":      {"type": "Decimal"},
    "gl_account":  {"type": "String", "nullable": True},
}})
client.upload_csv("invoices", "invoices.csv")    # or client.upload_entries("invoices", rows)
```

With the `aito` MCP server: `get_schema`, then `put_schema` and `upload_rows` (these
two are writes; the user enables them with `AITO_MCP_ALLOW_WRITES=1`).

## 5. The first prediction

```python
top = client.predict(from_table="invoices", predict="gl_account",
                     where={"vendor": "Acme Oy", "description": "printer toner"},
                     limit=3, why=True).hits[0]
print(top["$value"], top["$p"])
```

MCP: `predict` with `{"from": "invoices", "where": {...}, "predict": "gl_account", "limit": 3}`.

**Check the evidence on every answer:** each result carries `$p` and `$why`. For "which
X fits this record", prefer `_predict`. When you rank linked items with `_recommend`,
check that the top candidates have supporting history before you act on them.

## 6. Choose the threshold on held-out rows

`$p` is a probability you can set a threshold on; check it on the user's own data
before anything acts on it. `_evaluate` holds rows out, predicts them from the rest,
and with `"select": ["accuracy", "cases"]` returns each case's top `$p` and whether it
was right. Hold out only rows whose outcome is known.

```python
res = client.evaluate({
    "test": {"$index": {"$mod": [5, 0]}},                     # every 5th row
    "evaluate": {"from": "invoices",
                 "where": {"vendor": {"$get": "vendor"},
                           "description": {"$get": "description"}},
                 "predict": "gl_account"},
    "select": ["accuracy", "cases"]})
cases = sorted(res.data["cases"], key=lambda c: -c["top"]["$p"])


def threshold_for(cases, target):
    """the lowest $p whose cases above it are right at least `target` of the time,
    and the share of rows that would pass it"""
    found, right = None, 0
    for n, case in enumerate(cases, 1):
        right += case["accurate"]
        if right / n >= target:
            found = (case["top"]["$p"], n / len(cases))
    return found


print("overall accuracy", res.data["accuracy"])
print("act above", threshold_for(cases, 0.98), "suggest above", threshold_for(cases, 0.80))
```

Report to the user what the evaluation measured (accuracy, the share of rows above
each threshold), not a general claim about Aito.

## 7. Wire act / assist / abstain

```python
ACT, SUGGEST = 0.93, 0.60          # from step 6, per feature

def gl_account_for(invoice):
    top = client.predict(from_table="invoices", predict="gl_account",
                         where={"vendor": invoice["vendor"],
                                "description": invoice["description"]}).hits[0]
    if top["$p"] >= ACT:
        return {"value": top["$value"], "mode": "act"}      # fill it in
    if top["$p"] >= SUGGEST:
        return {"value": top["$value"], "mode": "assist"}   # prefill; a person confirms
    return {"value": None, "mode": "abstain"}               # ask a person
```

- Write every confirmed or corrected answer back as a row (`upload_entries`). It
  counts in the next query; there is no retraining step.
- Show `$why` next to an assisted suggestion so the person sees why.
- Re-run step 6 when the data changes a lot, and adjust the thresholds.

## Before you finish

- The fit check from step 1 is stated in your answer, including what to use instead
  if it did not fit.
- The data separation decision from step 2 is in the code, not a customer id in `where`.
- Thresholds come from step 6 on the user's data, with the numbers reported.
