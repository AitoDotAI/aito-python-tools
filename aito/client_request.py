"""Request objects that is sent by an :class:`~aito.client.AitoClient` to an Aito instance

"""

import logging
from abc import ABC, abstractmethod
from typing import Optional, Union, Dict, List, Type

import aito.client_response as aito_resp

LOG = logging.getLogger('AitoClientRequest')


class AitoRequest(ABC):
    """The base class of Request"""
    _endpoint_prefix = '/api/v1'
    _query_api_paths = ['_search', '_predict', '_recommend', '_evaluate', '_similarity', '_match', '_relate', '_query']
    _schema_api_path = 'schema'
    _data_api_path = 'data'
    _jobs_api_path = 'jobs'
    _version_endpoint = '/version'
    _request_methods = ['PUT', 'POST', 'GET', 'DELETE']

    def __init__(self, method: str, endpoint: str, query: Optional[Union[Dict, List]] = None):
        """

        :param method: the method of the request
        :type method: str
        :param endpoint: the endpoint of the request
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
            raise ValueError(f"endpoint must start with the '/' character")
        is_query_ep = endpoint in [f'{self._endpoint_prefix}/{path}' for path in self._query_api_paths]
        is_prefix_ep = any([
            endpoint.startswith(f'{self._endpoint_prefix}/{path}')
            for path in [self._data_api_path, self._schema_api_path, self._jobs_api_path]]
        )
        if endpoint != self._version_endpoint and not is_query_ep and not is_prefix_ep:
            raise ValueError(f'invalid endpoint {endpoint}')
        return endpoint

    def _check_method(self, method: str):
        """raise error if incorrect method else return the method
        """
        method = method.upper()
        if method not in self._request_methods:
            raise ValueError(
                f"invalid request method `{method}`. Method must be one of {'|'.join(self._request_methods)}"
            )
        return method

    def __str__(self):
        query_str = str(self.query)
        if len(query_str) > 100:
            query_str = query_str[:100] + '...'
        return f'{self.method}({self.endpoint}): {query_str}'

    @property
    @abstractmethod
    def response_cls(self) -> Type[aito_resp.BaseResponse]:
        """the class of the response for this request class

        :rtype: Type[BaseResponse]
        """
        pass

    @classmethod
    def make_request(cls, method: str, endpoint: str, query: Optional[Union[Dict, List]]):
        """return the appropriate request class instance with the similar method and endpoint"""
        # Check if is a QueryAPIRequest
        for sub_cls in QueryAPIRequest.__subclasses__():
            if method == sub_cls.method and endpoint == QueryAPIRequest.endpoint_from_path(sub_cls.path):
                return sub_cls(query=query)
        return BaseRequest(method=method, endpoint=endpoint, query=query)


class BaseRequest(AitoRequest):
    """Base request to the Aito instance"""
    @property
    def response_cls(self) -> Type[aito_resp.BaseResponse]:
        return aito_resp.BaseResponse


class QueryAPIRequest(AitoRequest, ABC):
    """Request to a `Query API <https://aito.ai/docs/api/#query-api>`__
    """
    method: str = 'POST'
    path: str = None

    def __init__(self, query: Optional[Union[Dict, List]]):
        """

        :param query: an Aito query if applicable, optional
        :type query: Optional[Union[Dict, List]]
        """
        if self.path is None:
            raise NotImplementedError(f'The API path must be implemented')
        endpoint = self.endpoint_from_path(self.path)
        super().__init__(method=self.method, endpoint=endpoint, query=query)

    @classmethod
    def endpoint_from_path(cls, path: str):
        """return the correct endpoint from the API path"""
        if path not in cls._query_api_paths:
            raise ValueError(f"path must be one of {'|'.join(cls._query_api_paths)}")
        return f'{cls._endpoint_prefix}/{path}'


class SearchRequest(QueryAPIRequest):
    """Request to the `Search API <https://aito.ai/docs/api/#post-api-v1-search>`__"""
    path: str = '_search'

    @property
    def response_cls(self) -> Type[aito_resp.BaseResponse]:
        return aito_resp.SearchResponse


class PredictRequest(QueryAPIRequest):
    """Request to the `Predict API <https://aito.ai/docs/api/#post-api-v1-predict>`__"""
    path: str = '_predict'

    @property
    def response_cls(self) -> Type[aito_resp.BaseResponse]:
        return aito_resp.PredictResponse


class RecommendRequest(QueryAPIRequest):
    """Request to the `Recommend API <https://aito.ai/docs/api/#post-api-v1-recommend>`__"""
    path: str = '_recommend'

    @property
    def response_cls(self) -> Type[aito_resp.BaseResponse]:
        return aito_resp.RecommendResponse


class EvaluateRequest(QueryAPIRequest):
    """Request to the `Evaluate API <https://aito.ai/docs/api/#post-api-v1-evaluate>`__"""
    path: str = '_evaluate'

    @property
    def response_cls(self) -> Type[aito_resp.BaseResponse]:
        return aito_resp.EvaluateResponse


class SimilarityRequest(QueryAPIRequest):
    """Request to the `Similarity API <https://aito.ai/docs/api/#post-api-v1-similarity>`__"""
    path: str = '_similarity'

    @property
    def response_cls(self) -> Type[aito_resp.BaseResponse]:
        return aito_resp.SimilarityResponse


class MatchRequest(QueryAPIRequest):
    """Request to the `Match query <https://aito.ai/docs/api/#post-api-v1-match>`__"""
    path: str = '_match'

    @property
    def response_cls(self) -> Type[aito_resp.BaseResponse]:
        return aito_resp.MatchResponse


class RelateRequest(QueryAPIRequest):
    """Request to the `Relate API <https://aito.ai/docs/api/#post-api-v1-relate>`__"""
    path: str = '_relate'

    @property
    def response_cls(self) -> Type[aito_resp.BaseResponse]:
        return aito_resp.RelateResponse


class GenericQueryRequest(QueryAPIRequest):
    """Request to the `Generic Query API <https://aito.ai/docs/api/#post-api-v1-query>`__"""
    path: str = '_query'

    @property
    def response_cls(self) -> Type[aito_resp.BaseResponse]:
        return aito_resp.HitsResponse
