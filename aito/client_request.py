"""Request objects that is sent by an :class:`~aito.client.AitoClient` to an Aito instance

"""

import logging
from abc import ABC, abstractmethod
from typing import Optional, Union, Dict, List, Type

import aito.client_response as aito_resp

LOG = logging.getLogger('AitoClientRequest')


class AitoRequest(ABC):
    """The base class of Request"""
    api_version_endpoint_prefix = '/api/v1'
    request_methods = ['PUT', 'POST', 'GET', 'DELETE']

    _data_api_path = 'data'
    _jobs_api_path = 'jobs'

    def __init__(self, method: str, endpoint: str, query: Optional[Union[Dict, List]] = None):
        """

        :param method: the method of the request
        :type method: str
        :param endpoint: the endpoint of the request
        :type endpoint: str
        :param query: an Aito query if applicable, optional
        :type query: Optional[Union[Dict, List]]
        """
        self.method = method
        self.endpoint = endpoint
        self.query = query

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

    def check_endpoint(self, endpoint: str) -> bool:
        """check if the input endpoint is a valid Aito endpoint
        """
        if not endpoint.startswith('/'):
            LOG.debug(f"endpoint must start with the '/' character")
            return False
        is_version_ep = endpoint == GetVersionRequest.endpoint
        is_schema_ep = SchemaAPIRequest.check_endpoint(endpoint)
        is_query_ep = QueryAPIRequest.check_endpoint(endpoint)
        is_prefix_ep = any([
            endpoint.startswith(f'{self.api_version_endpoint_prefix}/{path}')
            for path in [self._data_api_path, self._jobs_api_path]]
        )
        if not any([is_version_ep, is_schema_ep, is_query_ep, is_prefix_ep]):
            return False
        return True

    def check_method(self, method: str) -> bool:
        """returns True if the input request method is valid"""
        return method in self.request_methods

    def __init__(self, method: str, endpoint: str, query: Optional[Union[Dict, List]] = None):
        """

        :param method: the method of the request
        :type method: str
        :param endpoint: the endpoint of the request
        :type endpoint: str
        :param query: an Aito query if applicable, optional
        :type query: Optional[Union[Dict, List]]
        """
        if not self.check_endpoint(endpoint):
            raise ValueError(f"invalid endpoint '{endpoint}'")
        method = method.upper()
        if not self.check_method(method):
            raise ValueError(
                f"invalid request method `{method}`. Method must be one of {'|'.join(self.request_methods)}"
            )
        super().__init__(method=method, endpoint=endpoint, query=query)

    @property
    def response_cls(self) -> Type[aito_resp.BaseResponse]:
        return aito_resp.BaseResponse


class GetVersionRequest(AitoRequest):
    """Request to get the Aito instance version"""
    method = 'GET'
    endpoint = '/version'

    def __init__(self):
        super().__init__(method=self.method, endpoint=self.endpoint, query=None)

    @property
    def response_cls(self) -> Type[aito_resp.BaseResponse]:
        return aito_resp.GetVersionResponse


class QueryAPIRequest(AitoRequest, ABC):
    """Request to a `Query API <https://aito.ai/docs/api/#query-api>`__
    """
    method: str = 'POST'
    path: str = None # get around of not having abstract class attribute
    query_api_paths = ['_search', '_predict', '_recommend', '_evaluate', '_similarity', '_match', '_relate', '_query']

    def __init__(self, query: Optional[Union[Dict, List]]):
        """

        :param query: an Aito query if applicable, optional
        :type query: Optional[Union[Dict, List]]
        """
        if self.path is None:
            raise NotImplementedError(f'The API path must be implemented')
        if self.path not in self.query_api_paths:
            raise ValueError(f"invalid path, path must be one of {'|'.join(self.query_api_paths)}")
        endpoint = self.endpoint_from_path(self.path)
        super().__init__(method=self.method, endpoint=endpoint, query=query)

    @classmethod
    def check_endpoint(cls, endpoint: str):
        """returns True if the input endpoint is a Query API endpoint"""
        return endpoint in [f'{cls.api_version_endpoint_prefix}/{path}' for path in cls.query_api_paths]

    @classmethod
    def endpoint_from_path(cls, path: str):
        """return the query api endpoint from the query API path"""
        if path not in cls.query_api_paths:
            raise ValueError(f"path must be one of {'|'.join(cls.query_api_paths)}")
        return f'{cls.api_version_endpoint_prefix}/{path}'


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


class SchemaAPIRequest(AitoRequest, ABC):
    """Request to manipulate the schema"""
    endpoint_prefix = f'{AitoRequest.api_version_endpoint_prefix}/schema'

    @classmethod
    def check_endpoint(cls, endpoint: str):
        """returns True if the input endpoint is a Data API endpoint"""
        return endpoint.startswith(cls.endpoint_prefix)


class GetDatabaseSchemaRequest(SchemaAPIRequest):
    """Request to `Get the schema of the database <https://aito.ai/docs/api/#database-api>`__"""

    method = 'GET'
    endpoint = SchemaAPIRequest.endpoint_prefix

    def __init__(self):
        super().__init__(method=self.method, endpoint=self.endpoint)

    @property
    def response_cls(self) -> Type[aito_resp.BaseResponse]:
        return aito_resp.GetDatabaseSchemaResponse


class GetTableSchemaRequest(SchemaAPIRequest):
    """Request to `Get the schema of a table <https://aito.ai/docs/api/#get-api-v1-schema-table>`__"""

    method = 'GET'

    def __init__(self, table_name: str):
        """

        :param table_name: the name of the table
        :type table_name: str
        """
        endpoint = f'{self.endpoint_prefix}/{table_name}'
        super().__init__(method=self.method, endpoint=endpoint)

    @property
    def response_cls(self) -> Type[aito_resp.BaseResponse]:
        return aito_resp.GetTableSchemaResponse
