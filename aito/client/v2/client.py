"""A client for the Aito **v2** API

v2 is a different API, not a version bump, so it is a different client class.
The reasoning is written down in ``docs/v2-client-design.md``; the short form:

- **One engine, named endpoints.** ``_query`` is the universal surface, and
  ``_predict`` / ``_search`` / ``_recommend`` / ``_relate`` are *enforced*
  shortcuts for it — the server rejects a body whose mode does not match the
  endpoint, naming the right one. This client addresses the named endpoints so
  that check works for you, and :meth:`~AitoClientV2.query` is the escape hatch.
- **Collections vs legacy tables.** ``type: collection`` is v2's native table;
  anything created through v1 is a legacy table answered by a compatibility
  shim. Both are queryable through v2, and a few responses differ in shape
  between them — handled in :mod:`aito.client.v2.responses`.
- **Environments in the URL path.** An environment is addressed as
  ``/db/<db>/env/<name>/api/v2/...``. There is no env-scoped key; the database
  key authorizes every environment.
"""

import logging
from typing import Any, Dict, List, Optional, Union

import requests as requestslib

from .errors import AitoV2Error
from .responses import (
    V2AggregateResponse, V2BatchResponse, V2EstimateResponse, V2EvaluationResponse,
    V2RowsResponse, V2Response,
)

LOG = logging.getLogger('AitoClientV2')

#: environment names may not start with these — the engine reserves them
_RESERVED_ENV_PREFIXES = ('_', 'env.', 'release.')

#: what to do when a response carries warnings
_ON_WARNING_CHOICES = ('ignore', 'log', 'raise')


class AitoClientV2:
    """A client that connects to the Aito v2 API

    :param instance_url: the database URL, e.g. ``https://shared.aito.ai/db/my-db``,
        with no ``/api/...`` suffix
    :type instance_url: str
    :param api_key: the database API key
    :type api_key: str
    :param env: the environment to address; ``None`` addresses master
    :type env: Optional[str]
    :param meta: request the ``meta`` block on every response, which names the
        engine that answered. Off by default, matching the API's own opt-in
    :type meta: bool
    :param on_warning: what to do when a response carries warnings — ``'ignore'``,
        ``'log'`` (the default), or ``'raise'``
    :type on_warning: str
    :param timeout: the request timeout in seconds
    :type timeout: float
    :param check_credentials: verify the URL and key by fetching the schema
    :type check_credentials: bool
    :raises ValueError: the environment name is one the engine reserves
    :raises AitoV2Error: the credentials could not be verified

    >>> client = AitoClientV2(your_instance_url, your_api_key) # doctest: +SKIP
    >>> res = client.predict( # doctest: +SKIP
    ...     from_table='invoices', where={'vendor': 'Elenia Oy'}, predict='gl_code')
    >>> res.first.value, res.first.probability # doctest: +SKIP
    ('6110', 0.9656853940913004)
    """

    def __init__(
            self,
            instance_url: str,
            api_key: str,
            env: Optional[str] = None,
            meta: bool = False,
            on_warning: str = 'log',
            timeout: float = 30.0,
            check_credentials: bool = True,
    ):
        if on_warning not in _ON_WARNING_CHOICES:
            raise ValueError(
                f"invalid on_warning '{on_warning}', expected one of {'|'.join(_ON_WARNING_CHOICES)}")
        if env is not None:
            self._validate_env_name(env)

        self.instance_url = instance_url.rstrip('/')
        self.api_key = api_key
        self.env = env
        self.meta = meta
        self.on_warning = on_warning
        self.timeout = timeout
        # A pooled session keeps the TCP+TLS connection alive across calls. A
        # fresh connection per request pays the handshake every time, which
        # dominates the per-call wall-clock against a shared instance.
        self._session = requestslib.Session()
        self._session.headers.update(self.headers)

        if check_credentials:
            self.get_schema()

    @staticmethod
    def _validate_env_name(env: str) -> None:
        """reject an environment name the engine will refuse

        ``env.`` in particular became reserved after environments named that way
        already existed, so a stored ``env.v2-demo`` now fails at the server with
        a message about reservation. Failing here names the fix instead.
        """
        for prefix in _RESERVED_ENV_PREFIXES:
            if env.startswith(prefix):
                raise ValueError(
                    f"invalid environment name '{env}': names may not start with "
                    f"'_', 'env.' or 'release.'. Use '{env[len(prefix):]}' — the engine "
                    f"renamed environments created under the old 'env.' convention."
                )

    @property
    def headers(self) -> Dict:
        """the headers sent with every request

        :rtype: Dict
        """
        return {'Content-Type': 'application/json', 'x-api-key': self.api_key}

    @property
    def api_url(self) -> str:
        """the base URL of the v2 API, including the environment segment

        An environment is two path segments, ``/env/<name>``. The single dotted
        segment (``/db/<db>/env.<name>``) does not match the route at all.

        :rtype: str
        """
        env_segment = f'/env/{self.env}' if self.env else ''
        return f'{self.instance_url}{env_segment}/api/v2'

    def request(
            self,
            method: str,
            path: str,
            query: Optional[Union[Dict, List]] = None,
            timeout: Optional[float] = None,
    ) -> Any:
        """make a raw request to a v2 endpoint and return the parsed JSON

        The transport underneath every other method. Use it to reach an endpoint
        the client has no named method for.

        :param method: the HTTP method
        :type method: str
        :param path: the path below ``/api/v2``, e.g. ``/_query``
        :type path: str
        :param query: the request body, if any
        :type query: Optional[Union[Dict, List]]
        :param timeout: override the client's timeout for this call
        :type timeout: Optional[float]
        :raises AitoV2Error: the request failed or the response was not 2xx
        :rtype: Any
        """
        url = f'{self.api_url}{path}'
        params = {'meta': 'true'} if self.meta else None
        try:
            resp = self._session.request(
                method=method, url=url, json=query, params=params,
                timeout=self.timeout if timeout is None else timeout,
            )
        except requestslib.RequestException as e:
            raise AitoV2Error(f'Aito v2 request failed: {method} {path}: {e}') from e

        try:
            parsed = resp.json()
        except ValueError:
            parsed = None

        if resp.status_code >= 400:
            raise AitoV2Error.from_response(resp.status_code, resp.text, parsed)
        if parsed is None:
            raise AitoV2Error(
                f'Aito v2 returned a non-JSON body for {method} {path}: {resp.text[:200]}',
                status_code=resp.status_code, body=resp.text,
            )
        return parsed

    def _respond(self, response_cls, method: str, path: str,
                 query: Optional[Union[Dict, List]] = None,
                 timeout: Optional[float] = None) -> V2Response:
        """run a request and wrap it in a response class, handling warnings"""
        response = response_cls(self.request(method, path, query, timeout=timeout))
        self._handle_warnings(response, path)
        return response

    def _handle_warnings(self, response: V2Response, path: str) -> None:
        """apply the client's ``on_warning`` policy to a response

        Warnings are the only in-band signal that the engine answered a *different*
        query than the one that was asked — a ``where`` term it could not apply,
        for instance. Silently discarding them is how a broadened query reaches
        production looking like a success.
        """
        if not response.warnings or self.on_warning == 'ignore':
            return
        rendered = '; '.join(str(w) for w in response.warnings)
        if self.on_warning == 'raise':
            raise AitoV2Error(f'Aito v2 returned warnings for {path}: {rendered}',
                              body=response.json)
        LOG.warning('Aito v2 returned warnings for %s: %s', path, rendered)

    # --- introspection -------------------------------------------------

    def get_version(self) -> Dict:
        """the version of the Aito instance

        :rtype: Dict
        """
        return self.request('GET', '/_version')

    def get_operators(self) -> Dict:
        """the live operator inventory of this instance

        The set of query operators the instance actually supports, generated from
        the engine's own registry rather than from documentation — useful for
        checking whether a capability exists on the deploy you are talking to.

        :rtype: Dict
        """
        return self.request('GET', '/_ops')

    # --- schema --------------------------------------------------------

    def get_schema(self, table: Optional[str] = None) -> Dict:
        """the schema of the database, or of one table or collection

        :param table: the table or collection name; ``None`` returns the database
        :type table: Optional[str]
        :rtype: Dict
        """
        return self.request('GET', f'/schema/{table}' if table else '/schema')

    def create_collection(self, name: str, columns: Dict) -> Dict:
        """create a v2 collection

        :param name: the collection name
        :type name: str
        :param columns: the column definitions, the same map a v1 table schema
            uses — types ``String`` / ``Text`` / ``Decimal`` / ``Int`` /
            ``Boolean`` and the array and ``Json`` types, plus ``link``,
            ``analyzer`` and ``nullable``
        :type columns: Dict
        :rtype: Dict

        >>> client.create_collection('invoices', { # doctest: +SKIP
        ...     'vendor': {'type': 'String'},
        ...     'description': {'type': 'Text', 'analyzer': 'english'},
        ...     'gl_code': {'type': 'String'},
        ... })
        {'status': 'created', 'table': 'invoices', 'type': 'collection'}
        """
        return self.request('PUT', f'/schema/{name}', {'type': 'collection', 'columns': columns})

    def delete_collection(self, name: str) -> Dict:
        """delete a collection or legacy table and its data

        :param name: the collection or table name
        :type name: str
        :rtype: Dict
        """
        return self.request('DELETE', f'/schema/{name}')

    def copy_schema(self, query: Dict) -> Dict:
        """copy a schema, via ``POST /schema/_copy``

        :param query: the copy specification
        :type query: Dict
        :rtype: Dict
        """
        return self.request('POST', '/schema/_copy', query)

    # --- data ----------------------------------------------------------

    def upload_entries(self, name: str, entries: List[Dict], batch_size: int = 1000) -> int:
        """insert rows into a collection, in batches

        :param name: the collection name
        :type name: str
        :param entries: the rows to insert
        :type entries: List[Dict]
        :param batch_size: the number of rows per request
        :type batch_size: int
        :return: the number of rows inserted
        :rtype: int
        """
        total = 0
        for start in range(0, len(entries), batch_size):
            chunk = entries[start:start + batch_size]
            result = self.request('POST', f'/data/{name}/batch', chunk)
            total += int(result.get('count', len(chunk)))
        return total

    def optimize(self, name: str) -> Dict:
        """rebuild a collection's index after a bulk load

        Worth doing, not optional: on a freshly bulk-loaded collection the
        per-segment statistics have not been merged, so ``predict`` returns
        flatter and batch-count-dependent posteriors until this runs. It is
        idempotent.

        :param name: the collection name
        :type name: str
        :rtype: Dict
        """
        return self.request('POST', f'/data/{name}/optimize', {})

    def delete_entries(self, query: Dict) -> Dict:
        """delete rows, via ``POST /data/_delete``

        :param query: the delete specification, e.g. ``{'from': 'invoices',
            'where': {'gl_code': '6110'}}``
        :type query: Dict
        :rtype: Dict
        """
        return self.request('POST', '/data/_delete', query)

    def modify(self, query: Union[Dict, List]) -> Dict:
        """modify rows, via ``POST /data/_modify``

        :param query: the modify specification
        :type query: Union[Dict, List]
        :rtype: Dict
        """
        return self.request('POST', '/data/_modify', query)

    # --- environments --------------------------------------------------

    def list_envs(self) -> Dict:
        """the environments of this database

        :rtype: Dict
        """
        return self.request('GET', '/_envs')

    def branch_env(self, name: str) -> Dict:
        """branch a new environment off master

        Branching returns no key — the database key authorizes the new
        environment.

        :param name: the new environment name
        :type name: str
        :raises ValueError: the name is one the engine reserves
        :rtype: Dict
        """
        self._validate_env_name(name)
        return self.request('POST', '/_envs', {'name': name})

    def delete_env(self, name: str) -> Dict:
        """delete an environment

        :param name: the environment name
        :type name: str
        :rtype: Dict
        """
        return self.request('DELETE', f'/_envs/{name}')

    # --- queries -------------------------------------------------------

    def query(self, query: Dict, timeout: Optional[float] = None) -> V2RowsResponse:
        """run any query body against ``POST /_query``

        The universal surface and the escape hatch: ``_query`` enforces no mode,
        so anything the grammar accepts runs here. The named methods below are
        preferable where one fits, because their endpoints validate that the body
        matches the operation.

        :param query: the query body
        :type query: Dict
        :param timeout: override the client's timeout for this call
        :type timeout: Optional[float]
        :rtype: V2RowsResponse
        """
        return self._respond(V2RowsResponse, 'POST', '/_query', query, timeout=timeout)

    def search(
            self,
            from_table: str,
            where: Optional[Dict] = None,
            select: Optional[List] = None,
            order_by: Optional[Any] = None,
            limit: Optional[int] = None,
            offset: Optional[int] = None,
    ) -> V2RowsResponse:
        """retrieve matching rows

        The rows surface. The endpoint rejects a body carrying ``predict``,
        ``recommend`` or ``relate``, naming the endpoint that wants it.

        :param from_table: the collection or table to read
        :type from_table: str
        :param where: the filter
        :type where: Optional[Dict]
        :param select: the columns to return
        :type select: Optional[List]
        :param order_by: the ordering
        :type order_by: Optional[Any]
        :param limit: the maximum number of hits; pass ``0`` to read only ``total``
        :type limit: Optional[int]
        :param offset: the number of hits to skip
        :type offset: Optional[int]
        :rtype: V2RowsResponse
        """
        query = self._body(
            {'from': from_table}, where=where, select=select, orderBy=order_by,
            limit=limit, offset=offset)
        return self._respond(V2RowsResponse, 'POST', '/_search', query)

    def predict(
            self,
            from_table: str,
            predict: str,
            where: Optional[Dict] = None,
            select: Optional[List] = None,
            limit: Optional[int] = None,
            why: bool = False,
    ) -> V2RowsResponse:
        """predict the values of a field, ranked by probability

        :param from_table: the collection or table to learn from
        :type from_table: str
        :param predict: the field to predict
        :type predict: str
        :param where: the evidence
        :type where: Optional[Dict]
        :param select: the columns to return, defaulting to ``['$p', '$value']``
            (plus ``$why`` when ``why`` is set)
        :type select: Optional[List]
        :param limit: the maximum number of candidate values to return. Note that
            the API's own default returns only the top handful, so raise it to
            read a full distribution
        :type limit: Optional[int]
        :param why: include the ``$why`` explanation tree in the default select
        :type why: bool
        :rtype: V2RowsResponse

        >>> res = client.predict( # doctest: +SKIP
        ...     from_table='invoices', where={'vendor': 'Elenia Oy'}, predict='gl_code')
        >>> res.first.value # doctest: +SKIP
        '6110'
        """
        if select is None:
            select = ['$p', '$value', '$why'] if why else ['$p', '$value']
        query = self._body(
            {'from': from_table, 'predict': predict}, where=where, select=select, limit=limit)
        return self._respond(V2RowsResponse, 'POST', '/_predict', query)

    def recommend(
            self,
            from_table: str,
            recommend: str,
            goal: Dict,
            where: Optional[Dict] = None,
            select: Optional[List] = None,
            limit: Optional[int] = None,
    ) -> V2RowsResponse:
        """rank the values of a field by how well they achieve a goal

        :param from_table: the collection or table to learn from
        :type from_table: str
        :param recommend: the field to recommend a value of
        :type recommend: str
        :param goal: the outcome to optimize for, e.g. ``{'purchase': True}``
        :type goal: Dict
        :param where: the context
        :type where: Optional[Dict]
        :param select: the columns to return. When ``recommend`` names a link
            column, the default already returns every column of the linked row
        :type select: Optional[List]
        :param limit: the maximum number of hits
        :type limit: Optional[int]
        :rtype: V2RowsResponse
        """
        query = self._body(
            {'from': from_table, 'recommend': recommend, 'goal': goal},
            where=where, select=select, limit=limit)
        return self._respond(V2RowsResponse, 'POST', '/_recommend', query)

    def relate(
            self,
            from_table: str,
            relate: Union[str, List[str], Dict],
            where: Optional[Dict] = None,
            select: Optional[List] = None,
            order_by: Optional[Any] = None,
            limit: Optional[int] = None,
    ) -> V2RowsResponse:
        """find the statistical relationships between a condition and some fields

        :param from_table: the collection to analyse. Relate runs on collections;
            a legacy table answers ``501``
        :type from_table: str
        :param relate: the field or fields to relate, or a ``$patterns``
            specification. v2 takes a **list** of field names where v1 took a bare
            string, so a single name given as a string is wrapped for you
        :type relate: Union[str, List[str], Dict]
        :param where: the condition to relate against
        :type where: Optional[Dict]
        :param select: the columns to return, defaulting to
            ``['related', 'condition', 'lift', 'fs']``
        :type select: Optional[List]
        :param order_by: the ordering, defaulting to ``'lift'``
        :type order_by: Optional[Any]
        :param limit: the maximum number of hits
        :type limit: Optional[int]
        :rtype: V2RowsResponse
        """
        if isinstance(relate, str):
            relate = [relate]
        if select is None:
            select = ['related', 'condition', 'lift', 'fs']
        if order_by is None:
            order_by = 'lift'
        query = self._body(
            {'from': from_table, 'relate': relate},
            where=where, select=select, orderBy=order_by, limit=limit)
        return self._respond(V2RowsResponse, 'POST', '/_relate', query)

    def estimate(
            self,
            from_table: str,
            estimate: str,
            where: Optional[Dict] = None,
            select: Optional[List] = None,
    ) -> V2EstimateResponse:
        """estimate the numeric value of a field

        :param from_table: the collection or table to learn from
        :type from_table: str
        :param estimate: the numeric field to estimate
        :type estimate: str
        :param where: the evidence
        :type where: Optional[Dict]
        :param select: the payload keys to return, e.g. ``['value', 'why']``
        :type select: Optional[List]
        :rtype: V2EstimateResponse
        """
        query = self._body({'from': from_table, 'estimate': estimate}, where=where, select=select)
        return self._respond(V2EstimateResponse, 'POST', '/_estimate', query)

    def aggregate(
            self,
            from_table: str,
            aggregate: List[str],
            where: Optional[Dict] = None,
    ) -> V2AggregateResponse:
        """compute aggregates over the rows a filter selects

        :param from_table: the collection or table to read
        :type from_table: str
        :param aggregate: the aggregate expressions, e.g. ``['amount.$mean']``.
            The supported operators are ``$mean``, ``$sum``, ``$min`` and ``$max``
        :type aggregate: List[str]
        :param where: the filter
        :type where: Optional[Dict]
        :rtype: V2AggregateResponse
        """
        query = self._body({'from': from_table, 'aggregate': aggregate}, where=where)
        return self._respond(V2AggregateResponse, 'POST', '/_aggregate', query)

    def evaluate(self, query: Dict, timeout: Optional[float] = 600.0) -> V2EvaluationResponse:
        """run a held-out evaluation and return its metrics

        ``_evaluate`` is its own endpoint on v2 — it is not a ``_query`` key, and
        the grammar rejects one. The body is the v1 body unchanged: a ``test``
        (or ``testSource``) selector plus an ``evaluate`` query.

        :param query: the evaluation body
        :type query: Dict
        :param timeout: the request timeout, defaulting to 10 minutes because an
            evaluation over a large collection is slow by nature
        :type timeout: Optional[float]
        :rtype: V2EvaluationResponse

        >>> res = client.evaluate({ # doctest: +SKIP
        ...     'test': {'$index': {'$mod': [10, 0]}},
        ...     'evaluate': {
        ...         'from': 'invoices',
        ...         'where': {'vendor': {'$get': 'vendor'}},
        ...         'predict': 'gl_code',
        ...     },
        ... })
        >>> res.accuracy, res.base_accuracy # doctest: +SKIP
        (1.0, 0.2777777777777778)
        """
        return self._respond(V2EvaluationResponse, 'POST', '/_evaluate', query, timeout=timeout)

    def batch(self, queries: List[Dict], timeout: Optional[float] = None) -> V2BatchResponse:
        """run several queries in one request

        :param queries: the query bodies
        :type queries: List[Dict]
        :param timeout: override the client's timeout for this call
        :type timeout: Optional[float]
        :rtype: V2BatchResponse
        """
        return self._respond(V2BatchResponse, 'POST', '/_batch', queries, timeout=timeout)

    @staticmethod
    def _body(base: Dict, **parts) -> Dict:
        """add the optional clauses that were actually given to a query body

        Absent clauses are left out rather than sent as ``null``: v2 validates
        the grammar strictly, and a ``"where": null`` is a different thing from
        no ``where`` at all.
        """
        body = dict(base)
        for key, value in parts.items():
            if value is not None:
                body[key] = value
        return body

    def __repr__(self):
        env = f", env='{self.env}'" if self.env else ''
        return f"AitoClientV2('{self.instance_url}'{env})"
