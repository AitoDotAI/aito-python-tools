"""The Aito **v2** API client

``AitoClientV2`` is a separate class from the v1 :class:`~aito.client.aito_client.AitoClient`
rather than a flag on it. The reasoning is in ``docs/v2-client-design.md``.

>>> from aito.client.v2 import AitoClientV2 # doctest: +SKIP
>>> client = AitoClientV2(instance_url, api_key) # doctest: +SKIP
>>> client.predict(from_table='invoices', where={'vendor': 'Elenia Oy'}, # doctest: +SKIP
...                predict='gl_code').first.value
'6110'
"""

from .client import AitoClientV2
from .errors import AitoV2Error, AitoV2ResponseError
from .responses import (
    V2AggregateResponse, V2BatchResponse, V2EstimateResponse, V2EvaluationResponse,
    KIND_TO_RESPONSE_CLS, V2RowsResponse, V2Hit, V2Response, V2Warning,
    response_for_kind, unwrap_payload,
)

__all__ = [
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
