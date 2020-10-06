"""Request objects that is sent by an :class:`~aito.client.AitoClient` to an Aito instance

"""

import logging
from abc import ABC, abstractmethod
from typing import Optional, Union, Dict, List, TypeVar, Generic, Type

from aito.client_response import BaseResponse, SearchResponse, PredictResponse, RecommendResponse, EvaluateResponse, \
    SimilarityResponse, MatchResponse, RelateResponse, HitsResponse

LOG = logging.getLogger('AitoClientRequest')


class AitoRequest(ABC):
    """Request abstract class"""
    _query_paths = ['_search', '_predict', '_recommend', '_evaluate', '_similarity', '_match', '_relate', '_query']
    _query_endpoints = [f'/api/v1/{p}' for p in _query_paths] + ['/version']
    _database_endpoints = ['/api/v1/schema', '/api/v1/data']
    _job_endpoint = '/api/v1/jobs'
    _request_methods = ['PUT', 'POST', 'GET', 'DELETE']

    def __init__(self, method: str, endpoint: str, query: Optional[Union[Dict, List]] = None):
        """

        :param method: request method
        :type method: str
        :param endpoint: request endpoint
        :type endpoint: str
        :param query: an Aito query if applicable, optional
        :type query: Optional[Union[Dict, List]]
        """
        self.method = self._check_method(method)
        self.endpoint = self._check_endpoint(endpoint)
        self.query = query

    def _check_endpoint(self, endpoint: str):
        """raise error if erroneous endpoint and warn if the unrecognized endpoint, else return the endpoint
        """
        if not endpoint.startswith('/'):
            raise ValueError('endpoint must start with the `/` character')
        is_database_path = any([endpoint.startswith(db_endpoint) for db_endpoint in self._database_endpoints])
        is_job_path = endpoint.startswith(self._job_endpoint)
        if not is_database_path and not is_job_path and endpoint not in self._query_endpoints:
            LOG.warning(f'unrecognized endpoint {endpoint}')
        return endpoint

    def _check_method(self, method: str):
        """raise error if incorrect method else return the method
        """
        method = method.upper()
        if method not in self._request_methods:
            raise ValueError(
                f"incorrect request method `{method}`. Method must be one of {'|'.join(self._request_methods)}"
            )
        return method

    def __str__(self):
        query_str = str(self.query)
        if len(query_str) > 100:
            query_str = query_str[:100] + '...'
        return f'{self.method}({self.endpoint}): {query_str}'

    @property
    @abstractmethod
    def response_cls(self) -> Type[BaseResponse]:
        """the class of the response for this request class

        :rtype: Type[BaseResponse]
        """
        pass

    @classmethod
    def make_request(cls, method: str, endpoint: str, query: Optional[Union[Dict, List]]):
        for sub_cls in cls.__subclasses__():
            if hasattr(sub_cls, 'method') and hasattr(sub_cls, 'endpoint'):
                if method == sub_cls.method and endpoint == sub_cls.endpoint:
                    return sub_cls(query=query)
        return BaseRequest(method=method, endpoint=endpoint, query=query)


class BaseRequest(AitoRequest):
    """Base request to the Aito instance"""
    @property
    def response_cls(self) -> Type[BaseResponse]:
        return BaseResponse


class SearchRequest(AitoRequest):
    """Request to the `Search API <https://aito.ai/docs/api/#post-api-v1-search>`__"""
    method = 'POST'
    endpoint = '/api/v1/_search'

    def __init__(self, query: Dict):
        super().__init__(self.method, self.endpoint, query)

    @property
    def response_cls(self) -> Type[BaseResponse]:
        return SearchResponse


class PredictRequest(AitoRequest):
    """Request to the `Predict API <https://aito.ai/docs/api/#post-api-v1-predict>`__"""
    method = 'POST'
    endpoint = '/api/v1/_predict'

    def __init__(self, query: Dict):
        super().__init__(self.method, self.endpoint, query)

    @property
    def response_cls(self) -> Type[BaseResponse]:
        return PredictResponse


class RecommendRequest(AitoRequest):
    """Request to the `Recommend API <https://aito.ai/docs/api/#post-api-v1-recommend>`__"""
    method = 'POST'
    endpoint = '/api/v1/_recommend'

    def __init__(self, query: Dict):
        super().__init__(self.method, self.endpoint, query)

    @property
    def response_cls(self) -> Type[BaseResponse]:
        return RecommendResponse


class EvaluateRequest(AitoRequest):
    """Request to the `Evaluate API <https://aito.ai/docs/api/#post-api-v1-evaluate>`__"""
    method = 'POST'
    endpoint = '/api/v1/_evaluate'

    def __init__(self, query: Dict):
        super().__init__(self.method, self.endpoint, query)

    @property
    def response_cls(self) -> Type[BaseResponse]:
        return EvaluateResponse


class SimilarityRequest(AitoRequest):
    """Request to the `Similarity API <https://aito.ai/docs/api/#post-api-v1-similarity>`__"""
    method = 'POST'
    endpoint = '/api/v1/_similarity'

    def __init__(self, query: Dict):
        super().__init__(self.method, self.endpoint, query)

    @property
    def response_cls(self) -> Type[BaseResponse]:
        return SimilarityResponse


class MatchRequest(AitoRequest):
    """Request to the `Match query <https://aito.ai/docs/api/#post-api-v1-match>`__"""
    method = 'POST'
    endpoint = '/api/v1/_match'

    def __init__(self, query: Dict):
        super().__init__(self.method, self.endpoint, query)

    @property
    def response_cls(self) -> Type[BaseResponse]:
        return MatchResponse


class RelateRequest(AitoRequest):
    """Request to the `Relate API <https://aito.ai/docs/api/#post-api-v1-relate>`__"""
    method = 'POST'
    endpoint = '/api/v1/_relate'

    def __init__(self, query: Dict):
        super().__init__(self.method, self.endpoint, query)

    @property
    def response_cls(self) -> Type[BaseResponse]:
        return RelateResponse


class GenericQueryRequest(AitoRequest):
    """Response to the `Generic Query API <https://aito.ai/docs/api/#post-api-v1-query>`__"""
    method = 'POST'
    endpoint = '/api/v1/_query'

    def __init__(self, query: Dict):
        super().__init__(self.method, self.endpoint, query)

    @property
    def response_cls(self) -> Type[BaseResponse]:
        return HitsResponse
