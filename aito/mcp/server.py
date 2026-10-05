"""An MCP server over the Aito v2 API, for AI assistants and coding agents

Each tool says when to use it and when not to. Aito is the right tool for a narrow
class of questions; an assistant that recommends it where it does not fit loses the
user's trust in every answer after that. The fit and non-fit lists follow the "When
to use Aito, and when not" page (https://aito.ai/docs/articles/when-to-use-aito-and-when-not/);
https://aito.ai/docs/api/v2/benchmarks/ has the evidence.

Tools take a v2 query body as is (the same JSON as the REST API and its docs) and
return the response. Inference results carry ``$p`` and a ``$why`` flattened to the
base rate and the lift of each piece of evidence. Writes (``put_schema``,
``upload_rows``) are refused unless ``AITO_MCP_ALLOW_WRITES=1``: read-only by default.

Run: ``aito-mcp`` (stdio). It finds the instance the way the SDK does: ``AITO_URL``
(or ``AITO_INSTANCE_URL``) and ``AITO_API_KEY``, else the active profile, which
``aito start`` writes for a local instance.
"""

import math
import os
from typing import Any, Dict, List, Optional

try:  # mcp >= 2: FastMCP was renamed MCPServer
    from mcp.server.mcpserver import MCPServer as _Server
    from mcp.server.mcpserver.exceptions import ToolError
except ImportError:  # mcp 1.x
    from mcp.server.fastmcp import FastMCP as _Server
    from mcp.server.fastmcp.exceptions import ToolError
from mcp.types import ToolAnnotations

from aito.local.profiles import NoCredentialsError
from aito.v2 import AitoClientV2, AitoV2Error

WRITES_ENV = 'AITO_MCP_ALLOW_WRITES'
#: the page the fit and non-fit lists below quote
WHEN_TO_USE_URL = 'https://aito.ai/docs/articles/when-to-use-aito-and-when-not/'

FIT = """Use Aito when:
1. The decision repeats, and your own history knows the answer. (Which GL account an invoice goes to, who approves it, which category a ticket belongs to, which product a customer buys next.)
2. You need to know how sure the answer is. Automate the confident cases, ask a person in the middle, step back when unsure.
3. The data is relational, sparse or changing. A correction you write counts in the next query, with no retraining step.
4. You need many predictions, not one. A prediction behind every field, list and search, without a model or pipeline per use case or per customer.
5. Matching that repeats against your own labelled history.
6. An agent needs grounded tools. The likely answer and its confidence, or a shortlist of options before it reasons."""

NON_FIT = """Don't use Aito when (or not yet):
1. You need open-ended language understanding or generation. Use a language model; an LLM with retrieval (RAG) is more accurate on open text.
2. You have one large, flat, stable dataset and one decision to optimise for years. A trained model (for example LightGBM) wins on accuracy there.
3. Raw full-text search at very large scale is the whole job. A dedicated search engine is faster.
4. You need approximate nearest-neighbour search over a very large unfiltered vector collection. Aito's vector search is exact.
5. Matching records with no shared history to a catalogue (current state). A search engine (BM25) currently ranks better.
6. The case is genuinely new, with no history behind it. _predict will give a low probability, which is honest but not useful. That is the language model's job."""

CHECKLIST = """Quick decision checklist:
- Is there history of this decision in my data? No -> not Aito.
- Do I need a confidence I can act on? Yes -> Aito fits.
- Is it one decision on big, flat, stable data? Yes -> consider a trained model.
- Is it about reading or writing free text? Yes -> a language model, possibly with Aito as its tool."""

SEPARATION = ("Data separation between your customers: use a separate instance or collection per "
              "customer. Do not rely on a customer id in a query condition, and for now do not rely "
              "on a population-restricting query (a nested `from`) either, to keep customers' data apart.")

EVIDENCE = ("Check the evidence on every answer: each result carries `$p` and `$why`. For \"which X "
            "fits this record\", prefer _predict. When you rank linked items with _recommend, check "
            "that the top candidates have supporting history before you act on them.")

THRESHOLD = ("$p is a probability you can set a threshold on; check it on your own data with the "
             "evaluate tool before you act on it: act above the threshold, suggest in the middle, "
             "ask a person below.")

INSTRUCTIONS = '\n\n'.join([
    'Aito is a predictive database: load your data like an ordinary database, then query the '
    'unknown the way you query the known. You get a predicted value, a probability ($p) and the '
    'evidence behind it ($why). There is no model to train.',
    FIT, NON_FIT, CHECKLIST, SEPARATION, EVIDENCE, THRESHOLD,
    f'When to use Aito, and when not: {WHEN_TO_USE_URL}',
    'Evidence, method and where Aito loses: https://aito.ai/docs/api/v2/benchmarks/',
])

READ = ToolAnnotations(readOnlyHint=True, destructiveHint=False, openWorldHint=False)
WRITE = ToolAnnotations(readOnlyHint=False, destructiveHint=True, openWorldHint=False)
INFERENCE_SELECT = ['$value', '$p', '$why']


def flatten_why(why: Any) -> Dict[str, Any]:
    """``$why`` as ``{base_p, factors: [{proposition, lift}]}``, strongest effect first

    v2 nests the ``relatedPropositionLift`` factors under inner ``product`` nodes,
    so the tree is walked at any depth.
    """
    base_p = None
    factors: List[Dict[str, Any]] = []

    def walk(node: Any) -> None:
        nonlocal base_p
        if not isinstance(node, dict):
            return
        if node.get('type') == 'baseP':
            base_p = node.get('value')
        elif node.get('type') == 'relatedPropositionLift':
            factors.append({'proposition': node.get('proposition'), 'lift': node.get('value')})
        for sub in node.get('factors', []):
            walk(sub)

    walk(why)
    factors.sort(key=lambda f: -abs(math.log(f['lift'])) if f['lift'] else 0.0)
    return {'base_p': base_p, 'factors': factors}


def shape(response: Any) -> Any:
    """the response, each hit's ``$why`` flattened (see :func:`flatten_why`)"""
    if isinstance(response, dict) and isinstance(response.get('hits'), list):
        hits = [{**h, '$why': flatten_why(h['$why'])} if isinstance(h, dict) and '$why' in h else h
                for h in response['hits']]
        return {**response, 'hits': hits}
    return response


def _with_select(query: Dict[str, Any]) -> Dict[str, Any]:
    return query if 'select' in query else {**query, 'select': INFERENCE_SELECT}


def build_server(client: AitoClientV2, allow_writes: Optional[bool] = None) -> Any:
    """the MCP server with every tool bound to ``client``

    :param allow_writes: enable put_schema and upload_rows; default: the
        ``AITO_MCP_ALLOW_WRITES`` environment variable is ``1``
    """
    if allow_writes is None:
        allow_writes = os.environ.get(WRITES_ENV) == '1'
    server = _Server(name='aito', instructions=INSTRUCTIONS)

    def call(path: str, body: Any) -> Any:
        try:
            return shape(client.request('POST', path, body))
        except AitoV2Error as e:
            # the engine's message says what to fix in the query; pass it on whole
            # (a ToolError reaches the agent with its text, any other error as an opaque failure)
            raise ToolError(str(e)) from e

    def refuse_write(what: str) -> None:
        if not allow_writes:
            raise ToolError(f'{what} is a write, and writes are off. The user enables them by '
                             f'starting the server with {WRITES_ENV}=1.')

    @server.tool(name='predict', annotations=READ, description=f"""Predict the most likely value of one field for a row, learned from the table's own history. Returns each candidate value with $p (its probability) and $why (the base rate and the lift of each piece of evidence).

Use when: the decision repeats and the user's own records know the answer, e.g. which GL account an invoice goes to, which category a ticket belongs to, who approves it. For "which X fits this record", prefer this tool over recommend.
Don't use when: the task is open-ended language understanding or generation (use a language model); the case is genuinely new with no history (_predict will give a low probability, which is honest but not useful); one large, flat, stable dataset with one decision to optimise for years (consider a trained model).

{THRESHOLD}
{SEPARATION}

`query` is a v2 _predict body, e.g. {{"from": "invoices", "where": {{"vendor": "Acme Oy", "description": "printer toner"}}, "predict": "gl_account", "limit": 3}}. If `select` is omitted, $value, $p and $why are returned.""")
    def predict(query: Dict[str, Any]) -> Any:
        return call('/_predict', _with_select(query))

    @server.tool(name='recommend', annotations=READ, description=f"""Rank the values of a field by how likely they make a goal true, e.g. which product to offer so that the customer buys, learned from the history. Returns each option with $p and $why.

Use when: the user wants options ranked toward an outcome (next product, best channel, the lever that moves a rate).
Don't use when: the question is "which X fits this record" (use predict); the candidates have no history behind the goal.
Caveat: when you rank a LINK target (rows of another table), candidates with no supporting history can rank high. Check each top candidate's $p and $why for supporting evidence before you act on it, and prefer predict for "which X for this row".
{SEPARATION}

`query` is a v2 _recommend body, e.g. {{"from": "impressions", "where": {{"customer": "c42"}}, "recommend": "product", "goal": {{"purchased": true}}, "limit": 5}}. If `select` is omitted, $value, $p and $why are returned.""")
    def recommend(query: Dict[str, Any]) -> Any:
        return call('/_recommend', _with_select(query))

    @server.tool(name='relate', annotations=READ, description=f"""Find which conditions statistically go together with a given condition, with their lift and counts, e.g. what is typical of deals that were won. Descriptive statistics over the history, not a prediction for one row.

Use when: the user asks why, or what drives an outcome, or which features matter.
Don't use when: you need a value for one row (use predict) or a ranked choice (use recommend).
{SEPARATION}

`query` is a v2 _relate body, e.g. {{"from": "deals", "where": {{"won": true}}, "relate": "industry"}}.""")
    def relate(query: Dict[str, Any]) -> Any:
        return call('/_relate', query)

    @server.tool(name='match', annotations=READ, description=f"""Find the rows of a linked table that best match a row, learned from how such rows were linked before, e.g. which product an invoice line refers to. Returns ranked candidates with $p and $why.

Use when: the matching repeats against the user's own labelled history (the history is the signal).
Don't use when: matching records with no shared history to a catalogue (current state): a search engine (BM25) currently ranks better.
{SEPARATION}

`query` is a v2 _match body, e.g. {{"from": "invoice_lines", "where": {{"text": "toner cartridge black"}}, "match": "product", "limit": 5}}. If `select` is omitted, $value, $p and $why are returned.""")
    def match(query: Dict[str, Any]) -> Any:
        return call('/_match', _with_select(query))

    @server.tool(name='search', annotations=READ, description=f"""Find, filter and sort rows: an ordinary database lookup, no inference. Use it to look at the data before predicting, or to fetch rows to act on.

Don't use when: raw full-text search at very large scale is the whole job (a dedicated search engine is faster).
{SEPARATION}

`query` is a v2 _search body, e.g. {{"from": "invoices", "where": {{"vendor": "Acme Oy"}}, "orderBy": {{"$desc": "date"}}, "limit": 10}}.""")
    def search(query: Dict[str, Any]) -> Any:
        return call('/_search', query)

    @server.tool(name='query', annotations=READ, description=f"""Run any v2 query body on the general _query endpoint: search, predict, recommend, relate and match combined, including select expressions and nested queries. Use it when a specific tool does not take the shape you need; prefer the specific tools otherwise, since their descriptions say when they fit.

{EVIDENCE}
{SEPARATION}

`query` is a v2 _query body, e.g. {{"from": "deals", "where": {{"stage": "lead"}}, "select": ["deal_id", "value_eur"], "limit": 20}}.""")
    def query(query: Dict[str, Any]) -> Any:
        return call('/_query', query)

    @server.tool(name='evaluate', annotations=READ, description="""Measure how good a prediction is on the user's own data before anyone acts on it: Aito holds out the test rows, predicts them from the rest, and reports accuracy and how the predicted $p compare with what happened.

Use when: before wiring a prediction into an app or an automation, and to choose the confidence threshold for act / suggest / ask a person. Run it once per use case, and again when the data changes a lot.
Don't use when: you only need one prediction now (use predict).

`query` is a v2 _evaluate body: {"test": <which rows to hold out>, "evaluate": <a _predict body>}, e.g. {"test": {"$index": {"$mod": [4, 0]}}, "evaluate": {"from": "invoices", "where": {"vendor": {"$get": "vendor"}, "description": {"$get": "description"}}, "predict": "gl_account"}}. Add "select": ["accuracy", "cases"] to get each held-out row's top $value and $p and whether it was right: sort the cases by $p and pick the lowest threshold whose accuracy above it meets the user's target. Hold out only rows whose outcome is known. It can take minutes on a large table.""")
    def evaluate(query: Dict[str, Any]) -> Any:
        try:
            response = client.request('POST', '/_evaluate', query, timeout=600.0)
        except AitoV2Error as e:
            raise ToolError(str(e)) from e
        # v2 wraps the metrics as {"kind": "evaluation", "data": {...}}; return the metrics
        if isinstance(response, dict) and response.get('kind') == 'evaluation':
            return response.get('data')
        return response

    @server.tool(name='get_schema', annotations=READ, description="""Read the schema: every table with its columns, types and links, or one table's. Start here to see what the data holds and which fields can be predicted.""")
    def get_schema(table: Optional[str] = None) -> Any:
        try:
            return client.request('GET', f'/schema/{table}' if table else '/schema')
        except AitoV2Error as e:
            raise ToolError(str(e)) from e

    @server.tool(name='put_schema', annotations=WRITE, description=f"""Create a table (a v2 collection) from a schema. A WRITE: refused unless the user started the server with {WRITES_ENV}=1.

Derive the schema from the user's existing table: one column per field, with type String for ids and categories, Text for free text (with an analyzer, e.g. "english"), Int / Decimal for numbers, Boolean for flags, and "link": "<table>.<column>" where a field refers to another table's row.
{SEPARATION}

`schema` is a v2 schema body, e.g. {{"type": "collection", "columns": {{"invoice_id": {{"type": "String"}}, "vendor": {{"type": "String"}}, "description": {{"type": "Text", "analyzer": "english"}}, "amount": {{"type": "Decimal"}}, "gl_account": {{"type": "String"}}}}}}.""")
    def put_schema(table: str, schema: Dict[str, Any]) -> Any:
        refuse_write('put_schema')
        try:
            return client.request('PUT', f'/schema/{table}', schema)
        except AitoV2Error as e:
            raise ToolError(str(e)) from e

    @server.tool(name='upload_rows', annotations=WRITE, description=f"""Add rows to a table. A WRITE: refused unless the user started the server with {WRITES_ENV}=1. The rows count in the very next query; there is no training step.

{SEPARATION}

`rows` is a list of objects matching the table's schema, at most a few thousand per call.""")
    def upload_rows(table: str, rows: List[Dict[str, Any]]) -> Any:
        refuse_write('upload_rows')
        try:
            client.request('POST', f'/data/{table}/batch', rows)
        except AitoV2Error as e:
            raise ToolError(str(e)) from e
        return {'table': table, 'uploaded': len(rows)}

    return server


def main() -> None:
    """``aito-mcp``: serve the tools over stdio"""
    try:
        client = AitoClientV2(check_credentials=False)
    except NoCredentialsError as e:
        raise SystemExit(f'aito-mcp: {e}') from e
    build_server(client).run()


if __name__ == '__main__':
    main()
