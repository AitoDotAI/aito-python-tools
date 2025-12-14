"""Aito `Query API <https://aito.ai/docs/api/#query-api>`__ Request Class"""

import re
from abc import ABC
from typing import Dict, Optional, Union, List

from .aito_request import AitoRequest, _PatternEndpoint, _PostRequest, _FinalRequest
from ..responses import SearchResponse, PredictResponse, RecommendResponse, EvaluateResponse, SimilarityResponse, \
    MatchResponse, RelateResponse, HitsResponse, EstimateResponse, AggregateResponse, BatchResponse


class QueryAPIRequest(_PostRequest, _PatternEndpoint, AitoRequest, ABC):
    """Request to a `Query API <https://aito.ai/docs/api/#query-api>`__
    """
    #: the Query API path
    path: str = None  # get around of not having abstract class attribute
    _query_api_paths = ['_search', '_predict', '_recommend', '_evaluate', '_similarity', '_match', '_relate', '_query', '_estimate', '_aggregate']

    def __init__(self, query: Dict):
        """

        :param query: an Aito query if applicable, optional
        :type query: Dict
        """
        if self.path is None:
            raise NotImplementedError(f'The API path must be implemented')
        endpoint = self._endpoint_from_path(self.path)
        super().__init__(method=self.method, endpoint=endpoint, query=query)

    @classmethod
    def _endpoint_pattern(cls):
        return re.compile(f"^{cls._api_version_endpoint_prefix}/({'|'.join(cls._query_api_paths)})$")

    @classmethod
    def make_request(cls, method: str, endpoint: str, query: Optional[Union[Dict, List]]) -> 'AitoRequest':
        for sub_cls in cls.__subclasses__():
            if method == sub_cls.method and endpoint == QueryAPIRequest._endpoint_from_path(sub_cls.path):
                return sub_cls(query=query)
        raise ValueError(f"invalid {cls.__name__} with '{method}({endpoint})'")

    @classmethod
    def _endpoint_from_path(cls, path: str):
        """return the query api endpoint from the Query API path"""
        if path not in cls._query_api_paths:
            raise ValueError(f"path must be one of {'|'.join(cls._query_api_paths)}")
        return f'{cls._api_version_endpoint_prefix}/{path}'


class SearchRequest(QueryAPIRequest):
    """Request to the `Search API <https://aito.ai/docs/api/#post-api-v1-search>`__"""

    #: the Query API path
    path: str = '_search'
    response_cls = SearchResponse


class PredictRequest(QueryAPIRequest):
    """Request to the `Predict API <https://aito.ai/docs/api/#post-api-v1-predict>`__"""
    #: the Query API path
    path: str = '_predict'
    #: the class of the response for this request class
    response_cls = PredictResponse


class RecommendRequest(QueryAPIRequest):
    """Request to the `Recommend API <https://aito.ai/docs/api/#post-api-v1-recommend>`__"""
    #: the Query API path
    path: str = '_recommend'
    response_cls = RecommendResponse


class EvaluateRequest(QueryAPIRequest):
    """Request to the `Evaluate API <https://aito.ai/docs/api/#post-api-v1-evaluate>`__"""
    #: the Query API path
    path: str = '_evaluate'
    response_cls = EvaluateResponse


class SimilarityRequest(QueryAPIRequest):
    """Request to the `Similarity API <https://aito.ai/docs/api/#post-api-v1-similarity>`__"""
    #: the Query API path
    path: str = '_similarity'
    response_cls = SimilarityResponse


class MatchRequest(QueryAPIRequest):
    """Request to the `Match query <https://aito.ai/docs/api/#post-api-v1-match>`__"""
    #: the Query API path
    path: str = '_match'
    response_cls = MatchResponse


class RelateRequest(QueryAPIRequest):
    """Request to the `Relate API <https://aito.ai/docs/api/#post-api-v1-relate>`__"""
    #: the Query API path
    path: str = '_relate'
    response_cls = RelateResponse


class GenericQueryRequest(QueryAPIRequest):
    """Request to the `Generic Query API <https://aito.ai/docs/api/#post-api-v1-query>`__"""
    path: str = '_query'
    response_cls = HitsResponse


class EstimateRequest(QueryAPIRequest):
    """Request to the `Estimate API <https://aito.ai/docs/api/#post-api-v1-estimate>`__"""
    #: the Query API path
    path: str = '_estimate'
    #: the class of the response for this request class
    response_cls = EstimateResponse


class AggregateRequest(QueryAPIRequest):
    """Request to the `Aggregate API <https://aito.ai/docs/api/#post-api-v1-aggregate>`__"""
    #: the Query API path
    path: str = '_aggregate'
    #: the class of the response for this request class
    response_cls = AggregateResponse


class BatchRequest(_PostRequest, _FinalRequest, AitoRequest):
    """Request to the `Batch API <https://aito.ai/docs/api/#post-api-v1-batch>`__

    Allows executing multiple queries (predict, similarity, search, etc.) in a single request.

    Example::

        from aito.client.requests import BatchRequest

        # Execute multiple queries in one request
        request = BatchRequest([
            {"from": "products", "where": {"name": "rye bread"}, "predict": "category"},
            {"from": "products", "similarity": {"name": "rye bread"}}
        ])
    """
    endpoint = f'{AitoRequest._api_version_endpoint_prefix}/_batch'
    response_cls = BatchResponse

    def __init__(self, queries: List[Dict]):
        """
        :param queries: A list of query dictionaries to execute in batch
        :type queries: List[Dict]
        """
        super().__init__(query=queries)