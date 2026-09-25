"""The Aito **v2** API client

This package's meaning never changes: ``aito.v2`` is v2 for as long as it exists.
``aito.Client`` is the one name that follows the current default version — see
``docs/versioned-namespaces.md``. The v2 client is a separate class from the v1
:class:`~aito.v1.client.AitoClient` rather than a flag on it; the reasoning is in
``docs/v2-client-design.md``.

>>> from aito.v2 import Client # doctest: +SKIP
>>> client = Client(instance_url, api_key) # doctest: +SKIP
>>> client.predict(from_table='invoices', where={'vendor': 'Elenia Oy'}, # doctest: +SKIP
...                predict='gl_code').first.value
'6110'
"""

from .client import AitoClientV2
from .errors import AitoV2Error, AitoV2ResponseError

#: Version-neutral names. The prefixed ones remain as aliases.
Client = AitoClientV2
Error = AitoV2Error
ResponseError = AitoV2ResponseError
from .responses import (
    V2AggregateResponse, V2BatchResponse, V2EstimateResponse, V2EvaluationResponse,
    KIND_TO_RESPONSE_CLS, V2RowsResponse, V2Hit, V2Response, V2Warning,
    response_for_kind, unwrap_payload,
)

__all__ = [
    'Client',
    'Error',
    'ResponseError',
    'AitoClientV2',
    'AitoV2Error',
    'AitoV2ResponseError',
    'V2Response',
    'V2Hit',
    'V2Warning',
    'V2RowsResponse',
    'V2EstimateResponse',
    'V2AggregateResponse',
    'V2EvaluationResponse',
    'V2BatchResponse',
    'KIND_TO_RESPONSE_CLS',
    'response_for_kind',
    'unwrap_payload',
]
