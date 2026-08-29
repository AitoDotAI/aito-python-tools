"""Response classes returned by the :class:`~aito.client.v2.client.AitoClientV2`

The v2 envelope (``core/docs/v2-response-format.md`` §3) is::

    {"kind": "<rows|estimate|aggregate|evaluation|batch|error>",
     <payload>,          # `hits` (+ offset, total) for rows; `data` otherwise
     "meta":     {...},  # opt-in, see AitoClientV2(meta=True)
     "warnings": [...]}  # non-fatal notes, always additive

Two things about that envelope are worth knowing before reading this module.

**`rows` is bare.** For v1 compatibility a rows response carries no ``kind`` at
all — ``{offset, total, hits}``. The documented client rule is ``kind ?? "rows"``.

**...but an absent `kind` does not prove `rows`.** The non-rows envelopes are
built by the rep2-native code paths only. A legacy ``type: table`` answered
through a v2 endpoint falls to the rep1 compatibility shim, which returns the
flat v1 shape — so ``POST /api/v2/_estimate`` returns ``{"estimate": ..., "why":
...}`` for a legacy table and ``{"kind": "estimate", "data": {"value": ...}}``
for a collection, on the same build. Applying ``kind ?? "rows"`` to the first
one classifies a scalar as a page of rows.

:func:`unwrap_payload` is the single place that copes with this: the caller says
which kind it *asked* for, an envelope of that kind is unwrapped, a bare body is
passed through, and a *different* kind raises. When the engine unifies the two
shapes, that one function is the only thing to delete.
"""

from typing import Any, Dict, Iterator, List, Optional

from .errors import AitoV2ResponseError


class V2Warning:
    """A non-fatal note attached to a v2 response

    The engine emits these where it would otherwise have to choose between
    failing and staying silent — e.g. a ``where`` naming a column that is not
    declared on the collection.
    """

    def __init__(self, json: Dict):
        """

        :param json: the warning object
        :type json: Dict
        """
        self._json = json

    @property
    def json(self) -> Dict:
        """the raw warning object

        :rtype: Dict
        """
        return self._json

    @property
    def code(self) -> Optional[str]:
        """the machine-readable, dotted warning code, e.g. ``where.unknown_field``

        :rtype: Optional[str]
        """
        return self._json.get('code')

    @property
    def message(self) -> Optional[str]:
        """the human-readable warning message

        :rtype: Optional[str]
        """
        return self._json.get('message')

    @property
    def severity(self) -> str:
        """``info`` or ``warning``, defaulting to ``warning``

        :rtype: str
        """
        return self._json.get('severity', 'warning')

    @property
    def field(self) -> Optional[str]:
        """the field the warning is scoped to, if it is field-scoped

        :rtype: Optional[str]
        """
        return self._json.get('field')

    def __str__(self):
        scope = f" (field '{self.field}')" if self.field else ''
        return f'[{self.code}]{scope} {self.message}'

    def __repr__(self):
        return f'V2Warning({self._json!r})'


def unwrap_payload(json: Dict, kind: str) -> Any:
    """return the payload of a v2 response, whether or not it is enveloped

    :param json: the parsed response body
    :type json: Dict
    :param kind: the result kind the called endpoint produces
    :type kind: str
    :raises AitoV2ResponseError: the response carries a different ``kind``
    :return: ``json['data']`` when enveloped, otherwise ``json`` itself
    :rtype: Any

    An enveloped response of the expected kind is unwrapped:

    >>> unwrap_payload({'kind': 'estimate', 'data': {'value': 12.4}}, 'estimate')
    {'value': 12.4}

    A bare response — the rep1 compatibility shim's v1 shape — is passed through
    rather than misread as a page of rows:

    >>> unwrap_payload({'estimate': 12.4, 'why': {}}, 'estimate')
    {'estimate': 12.4, 'why': {}}

    A *different* kind is a real mismatch and raises:

    >>> unwrap_payload({'kind': 'aggregate', 'data': {}}, 'estimate')
    Traceback (most recent call last):
       ...
    aito.client.v2.errors.AitoV2ResponseError: expected a 'estimate' response from Aito v2, got 'aggregate'
    """
    if not isinstance(json, dict) or 'kind' not in json:
        return json
    if json['kind'] != kind:
        raise AitoV2ResponseError(
            f"expected a '{kind}' response from Aito v2, got '{json['kind']}'"
        )
    return json.get('data', json)


class V2Response:
    """The base class of a v2 API response

    Wraps the raw JSON and exposes the parts of the envelope that every kind
    shares: :attr:`kind`, :attr:`warnings` and :attr:`meta`.
    """

    #: the result kind this response class represents
    kind: str = 'rows'

    def __init__(self, json: Dict):
        """

        :param json: the parsed JSON response
        :type json: Dict
        """
        self._json = json
        self._warnings = [V2Warning(w) for w in (json.get('warnings') or [])] \
            if isinstance(json, dict) else []

    @property
    def json(self) -> Dict:
        """the raw JSON response

        :rtype: Dict
        """
        return self._json

    @property
    def warnings(self) -> List[V2Warning]:
        """the non-fatal notes the engine attached to this response

        Empty for the great majority of responses. Worth checking when a result
        is unexpectedly broad or unexpectedly empty — the engine reports a
        ``where`` it could not apply here rather than failing the request.

        :rtype: List[V2Warning]
        """
        return self._warnings

    @property
    def meta(self) -> Dict:
        """the response metadata, empty unless the client was built with ``meta=True``

        Currently carries ``engine`` — ``v2`` for a collection served by the rep2
        engine, ``v1`` for a legacy table served by the compatibility shim.

        :rtype: Dict
        """
        return self._json.get('meta', {}) if isinstance(self._json, dict) else {}

    @property
    def engine(self) -> Optional[str]:
        """which engine answered, if the client requested ``meta``

        :rtype: Optional[str]
        """
        return self.meta.get('engine')

    def __contains__(self, item):
        return item in self._json

    def __getitem__(self, item):
        if item not in self._json:
            raise KeyError(f'Response does not contain field `{item}`')
        return self._json[item]

    def __repr__(self):
        return f'{self.__class__.__name__}({self._json!r})'


class V2Hit:
    """A single hit of a ``rows`` response

    User columns are read by name; the computed columns keep the ``$`` prefix
    they were requested with in ``select`` (``$p``, ``$value``, ``$why``), which
    is why they are guaranteed not to collide with a user column.
    """

    def __init__(self, json: Dict):
        """

        :param json: the content of the hit
        :type json: Dict
        """
        self._json = json

    @property
    def json(self) -> Dict:
        """the raw content of the hit

        :rtype: Dict
        """
        return self._json

    def __getitem__(self, item):
        if item not in self._json:
            raise KeyError(f'The hit does not contain field `{item}`. '
                           f'Please specify the field in the `select` clause of the query')
        return self._json[item]

    def __contains__(self, item):
        return item in self._json

    def __iter__(self):
        return iter(self._json)

    def get(self, item, default=None):
        """read a field of the hit, returning ``default`` when it is absent

        :rtype: Any
        """
        return self._json.get(item, default)

    @property
    def probability(self) -> float:
        """the ``$p`` of a predict, recommend or match hit

        :raises KeyError: ``$p`` was not selected
        :rtype: float
        """
        return self.__getitem__('$p')

    @property
    def value(self) -> Any:
        """the predicted value, i.e. ``$value``

        v2 names this ``$value``; v1 named it ``feature``. The SDK does not alias
        it back — see ``docs/v2-client-design.md``.

        :raises KeyError: ``$value`` was not selected
        :rtype: Any
        """
        return self.__getitem__('$value')

    @property
    def why(self) -> Dict:
        """the ``$why`` explanation tree

        :raises KeyError: ``$why`` was not selected
        :rtype: Dict
        """
        return self.__getitem__('$why')

    def __repr__(self):
        return f'V2Hit({self._json!r})'


class V2RowsResponse(V2Response):
    """A ``rows`` response — a page of hits

    The shape returned by ``_query``, ``_search``, ``_predict``, ``_recommend``,
    ``_relate`` and ``_match``. It stays bare (``{offset, total, hits}``) in v2
    for v1 compatibility.
    """

    kind = 'rows'

    def __init__(self, json: Dict):
        super().__init__(json)
        self._hits = [V2Hit(hit) for hit in json.get('hits', [])]

    @property
    def hits(self) -> List[V2Hit]:
        """the returned hits

        :rtype: List[V2Hit]
        """
        return self._hits

    @property
    def total(self) -> int:
        """the total number of matching rows, which may exceed ``len(hits)``

        :rtype: int
        """
        return self._json.get('total', len(self._hits))

    @property
    def offset(self) -> int:
        """the number of hits skipped

        :rtype: int
        """
        return self._json.get('offset', 0)

    @property
    def first(self) -> Optional[V2Hit]:
        """the first hit, or ``None`` when nothing matched

        A predict, recommend or match response is ranked, so this is the top
        result. Returns ``None`` rather than raising because an empty result is
        an ordinary outcome, not an error.

        :rtype: Optional[V2Hit]
        """
        return self._hits[0] if self._hits else None

    def __iter__(self) -> Iterator[V2Hit]:
        return iter(self._hits)

    def __len__(self) -> int:
        return len(self._hits)


class V2EstimateResponse(V2Response):
    """An ``estimate`` response — a single predicted numeric value"""

    kind = 'estimate'

    def __init__(self, json: Dict):
        super().__init__(json)
        self._data = unwrap_payload(json, self.kind)

    @property
    def data(self) -> Dict:
        """the unwrapped payload

        :rtype: Dict
        """
        return self._data

    @property
    def value(self) -> float:
        """the estimated value

        v2 names the scalar ``value``; the rep1 compatibility shim still names it
        ``estimate``. Both are read here so the property works on a collection
        and on a legacy table alike.

        :raises KeyError: the payload carries neither key
        :rtype: float
        """
        if 'value' in self._data:
            return self._data['value']
        if 'estimate' in self._data:
            return self._data['estimate']
        raise KeyError("The estimate response contains neither 'value' nor 'estimate'")

    @property
    def why(self) -> Optional[Dict]:
        """the explanation of how the estimate was computed, when ``why`` was selected

        :rtype: Optional[Dict]
        """
        return self._data.get('why')


class V2AggregateResponse(V2Response):
    """An ``aggregate`` response — a small object of aggregate results

    Keys are the requested aggregate expressions (``amount.$mean``), alongside
    the derived ones the engine adds (``amount.$mean.samples``,
    ``amount.$mean.variance``, ``.standardDeviation``, ``.standardError``).
    """

    kind = 'aggregate'

    def __init__(self, json: Dict):
        super().__init__(json)
        self._data = unwrap_payload(json, self.kind)

    @property
    def data(self) -> Dict:
        """the aggregate results

        :rtype: Dict
        """
        return self._data

    def __getitem__(self, item):
        if item not in self._data:
            raise KeyError(f'The aggregate response does not contain `{item}`')
        return self._data[item]

    def __contains__(self, item):
        return item in self._data


class V2EvaluationResponse(V2Response):
    """An ``evaluation`` response — the metrics of a held-out evaluation"""

    kind = 'evaluation'

    def __init__(self, json: Dict):
        super().__init__(json)
        self._data = unwrap_payload(json, self.kind)

    @property
    def data(self) -> Dict:
        """the metrics

        :rtype: Dict
        """
        return self._data

    @property
    def accuracy(self) -> float:
        """the share of test rows whose top prediction was correct

        :rtype: float
        """
        return self._data['accuracy']

    @property
    def base_accuracy(self) -> float:
        """the accuracy of always predicting the most common value

        The number ``accuracy`` has to beat to mean anything.

        :rtype: float
        """
        return self._data['baseAccuracy']

    @property
    def test_sample_count(self) -> int:
        """the number of rows held out and predicted

        :rtype: int
        """
        return self._data['testSamples']

    @property
    def train_sample_count(self) -> int:
        """the number of rows Aito learned from during the evaluation

        :rtype: int
        """
        return self._data['trainSamples']

    @property
    def cases(self) -> List[Dict]:
        """the per-case results, when ``cases`` was selected

        :rtype: List[Dict]
        """
        return self._data.get('cases', [])


class V2BatchResponse(V2Response):
    """A ``batch`` response — an array of typed sub-results

    Each element is itself a full typed response, so a caller dispatches per
    element with :func:`response_for_kind`.

    In practice the engine returns a **bare JSON array** here, not the
    ``{"kind": "batch", "data": [...]}`` envelope the response-format spec
    documents. Both are accepted: the bare array is what actually arrives
    today, and the envelope is what the spec promises, so the class keeps
    working if the engine is brought in line. Filed against the engine.
    """

    kind = 'batch'

    def __init__(self, json: Dict):
        super().__init__(json)
        payload = unwrap_payload(json, self.kind)
        self._data = payload if isinstance(payload, list) else [payload]

    @property
    def data(self) -> List[Dict]:
        """the raw sub-results

        :rtype: List[Dict]
        """
        return self._data

    @property
    def responses(self) -> List[V2Response]:
        """the sub-results, each parsed into its own response class

        :rtype: List[V2Response]
        """
        return [response_for_kind(sub.get('kind', 'rows') if isinstance(sub, dict) else 'rows')(sub)
                for sub in self._data]

    def __len__(self) -> int:
        return len(self._data)

    def __getitem__(self, index):
        return self._data[index]


#: result kind -> the response class that parses it
KIND_TO_RESPONSE_CLS = {
    'rows': V2RowsResponse,
    'estimate': V2EstimateResponse,
    'aggregate': V2AggregateResponse,
    'evaluation': V2EvaluationResponse,
    'batch': V2BatchResponse,
}


def response_for_kind(kind: str):
    """return the response class for a result kind, defaulting to :class:`V2RowsResponse`

    :param kind: the result kind
    :type kind: str
    :rtype: type
    """
    return KIND_TO_RESPONSE_CLS.get(kind, V2RowsResponse)
