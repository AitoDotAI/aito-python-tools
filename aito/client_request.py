"""Request objects that is sent by an :class:`~aito.client.AitoClient` to an Aito instance

"""

import logging
from abc import ABC, abstractmethod
from typing import Optional, Union, Dict, List, Type

import aito.client_response as aito_resp
import re

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
    @abstractmethod
    def check_endpoint(cls, endpoint: str) -> bool:
        """check if the input endpoint is a valid endpoint of the Request class

        :param endpoint: the input endpoint
        :type endpoint: str
        :return: True if the endpoint is valid
        :rtype: bool
        """
        pass

    @classmethod
    @abstractmethod
    def check_method(cls, method: str) -> bool:
        """check if the input method is a valid endpoint of the Request class

        :param method: the input method
        :type method: str
        :return: True if the method is valid
        :rtype: bool
        """
        pass

    @classmethod
    @abstractmethod
    def make_request(cls, method: str, endpoint: str, query: Optional[Union[Dict, List]]) -> 'AitoRequest':
        """factory method to return the appropriate request class instance after checking the input endpoint and method
        :param method: the method of the request
        :type method: str
        :param endpoint: the endpoint of the request
        :type endpoint: str
        :param query: an Aito query if applicable, optional
        :type query: Optional[Union[Dict, List]]
        :return: the appropriate request class intsnace
        :rtype: AitoRequest
        """
        for sub_cls in cls.__subclasses__():
            if sub_cls != BaseRequest and sub_cls.check_method(method) and sub_cls.check_endpoint(endpoint):
                try:
                    instance = sub_cls.make_request(method=method, endpoint=endpoint, query=query)
                    return instance
                except Exception as e:
                    LOG.debug(f"invalid {sub_cls.__name__} with '{method}({endpoint}): {e}'")
        return BaseRequest(method=method, endpoint=endpoint, query=query)


class BaseRequest(AitoRequest):
    """Base request to the Aito instance"""

    @classmethod
    def check_endpoint(cls, endpoint: str) -> bool:
        """check if the input endpoint is a valid Aito endpoint
        """
        if not endpoint.startswith('/'):
            LOG.debug(f"endpoint must start with the '/' character")
            return False
        is_version_ep = endpoint == GetVersionRequest.endpoint
        is_schema_ep = _SchemaAPIRequest.check_endpoint(endpoint)
        is_query_ep = _QueryAPIRequest.check_endpoint(endpoint)
        is_prefix_ep = any([
            endpoint.startswith(f'{cls.api_version_endpoint_prefix}/{path}')
            for path in [cls._data_api_path, cls._jobs_api_path]]
        )
        if not any([is_version_ep, is_schema_ep, is_query_ep, is_prefix_ep]):
            return False
        return True

    @classmethod
    def check_method(cls, method: str) -> bool:
        """returns True if the input request method is valid"""
        return method in cls.request_methods

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

    @classmethod
    def make_request(cls, method: str, endpoint: str, query: Optional[Union[Dict, List]]) -> 'AitoRequest':
        return cls(method=method, endpoint=endpoint, query=query)

    @property
    def response_cls(self) -> Type[aito_resp.BaseResponse]:
        return aito_resp.BaseResponse


class GetVersionRequest(AitoRequest):
    """Request to get the Aito instance version"""
    method = 'GET'
    endpoint = '/version'

    def __init__(self):
        super().__init__(method=self.method, endpoint=self.endpoint, query=None)

    @classmethod
    def check_method(cls, method: str) -> bool:
        return method == cls.method

    @classmethod
    def check_endpoint(cls, endpoint: str) -> bool:
        return endpoint == cls.endpoint

    @classmethod
    def make_request(cls, method: str, endpoint: str, query: Optional[Union[Dict, List]]) -> 'AitoRequest':
        return cls()

    @property
    def response_cls(self) -> Type[aito_resp.BaseResponse]:
        return aito_resp.GetVersionResponse


class _QueryAPIRequest(AitoRequest, ABC):
    """Request to a `Query API <https://aito.ai/docs/api/#query-api>`__
    """

    method: str = 'POST'
    path: str = None  # get around of not having abstract class attribute
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
        return endpoint in [f'{cls.api_version_endpoint_prefix}/{path}' for path in cls.query_api_paths]

    @classmethod
    def check_method(cls, method: str) -> bool:
        return method == cls.method

    @classmethod
    def make_request(cls, method: str, endpoint: str, query: Optional[Union[Dict, List]]) -> 'AitoRequest':
        for sub_cls in cls.__subclasses__():
            if method == sub_cls.method and endpoint == _QueryAPIRequest.endpoint_from_path(sub_cls.path):
                return sub_cls(query=query)
        raise ValueError(f"invalid {cls.__name__} with '{method}({endpoint})'")

    @classmethod
    def endpoint_from_path(cls, path: str):
        """return the query api endpoint from the query API path"""
        if path not in cls.query_api_paths:
            raise ValueError(f"path must be one of {'|'.join(cls.query_api_paths)}")
        return f'{cls.api_version_endpoint_prefix}/{path}'


class SearchRequest(_QueryAPIRequest):
    """Request to the `Search API <https://aito.ai/docs/api/#post-api-v1-search>`__"""
    path: str = '_search'

    @property
    def response_cls(self) -> Type[aito_resp.BaseResponse]:
        return aito_resp.SearchResponse


class PredictRequest(_QueryAPIRequest):
    """Request to the `Predict API <https://aito.ai/docs/api/#post-api-v1-predict>`__"""
    path: str = '_predict'

    @property
    def response_cls(self) -> Type[aito_resp.BaseResponse]:
        return aito_resp.PredictResponse


class RecommendRequest(_QueryAPIRequest):
    """Request to the `Recommend API <https://aito.ai/docs/api/#post-api-v1-recommend>`__"""
    path: str = '_recommend'

    @property
    def response_cls(self) -> Type[aito_resp.BaseResponse]:
        return aito_resp.RecommendResponse


class EvaluateRequest(_QueryAPIRequest):
    """Request to the `Evaluate API <https://aito.ai/docs/api/#post-api-v1-evaluate>`__"""
    path: str = '_evaluate'

    @property
    def response_cls(self) -> Type[aito_resp.BaseResponse]:
        return aito_resp.EvaluateResponse


class SimilarityRequest(_QueryAPIRequest):
    """Request to the `Similarity API <https://aito.ai/docs/api/#post-api-v1-similarity>`__"""
    path: str = '_similarity'

    @property
    def response_cls(self) -> Type[aito_resp.BaseResponse]:
        return aito_resp.SimilarityResponse


class MatchRequest(_QueryAPIRequest):
    """Request to the `Match query <https://aito.ai/docs/api/#post-api-v1-match>`__"""
    path: str = '_match'

    @property
    def response_cls(self) -> Type[aito_resp.BaseResponse]:
        return aito_resp.MatchResponse


class RelateRequest(_QueryAPIRequest):
    """Request to the `Relate API <https://aito.ai/docs/api/#post-api-v1-relate>`__"""
    path: str = '_relate'

    @property
    def response_cls(self) -> Type[aito_resp.BaseResponse]:
        return aito_resp.RelateResponse


class GenericQueryRequest(_QueryAPIRequest):
    """Request to the `Generic Query API <https://aito.ai/docs/api/#post-api-v1-query>`__"""
    path: str = '_query'

    @property
    def response_cls(self) -> Type[aito_resp.BaseResponse]:
        return aito_resp.HitsResponse


class _SchemaAPIRequest(AitoRequest, ABC):
    """Request to manipulate the schema"""
    endpoint_prefix = f'{AitoRequest.api_version_endpoint_prefix}/schema'

    @classmethod
    @abstractmethod
    def check_endpoint(cls, endpoint: str) -> bool:
        return endpoint.startswith(cls.endpoint_prefix)

    @classmethod
    @abstractmethod
    def make_request(cls, method: str, endpoint: str, query: Optional[Union[Dict, List]]) -> 'AitoRequest':
        for sub_cls in cls.__subclasses__():
            if sub_cls.check_method(method) and sub_cls.check_endpoint(endpoint):
                return sub_cls.make_request(method=method, endpoint=endpoint, query=query)
        raise ValueError(f"invalid {cls.__name__} with '{method}({endpoint})'")


class _DatabaseSchemaRequest:
    """Request to manipulate the database schema"""
    @classmethod
    def check_endpoint(cls, endpoint: str):
        return endpoint == _SchemaAPIRequest.endpoint_prefix


class _TableSchemaRequest:
    """Request to manipulate a table schema"""
    @classmethod
    def check_endpoint(cls, endpoint: str):
        pattern = re.compile(f'^{_SchemaAPIRequest.endpoint_prefix}/[^/".$\r\n\s]+$')
        return pattern.match(endpoint) is not None


class _ColumnSchemaRequest:
    """Request to manipulate a column schema"""
    @classmethod
    def check_endpoint(cls, endpoint: str):
        pattern = re.compile(f'^{_SchemaAPIRequest.endpoint_prefix}/[^/".$\r\n\s]+/[^/".$\r\n\s]$')
        return pattern.match(endpoint) is not None


class _GetSchemaRequest:
    """Request to get schema"""
    method = 'GET'

    @classmethod
    def check_method(cls, method: str):
        return method == 'GET'


class GetDatabaseSchemaRequest(_DatabaseSchemaRequest, _GetSchemaRequest, _SchemaAPIRequest):
    """Request to `Get the schema of the database <https://aito.ai/docs/api/#database-api>`__"""
    endpoint = _SchemaAPIRequest.endpoint_prefix

    def __init__(self):
        super().__init__(method=self.method, endpoint=self.endpoint)

    @property
    def response_cls(self) -> Type[aito_resp.BaseResponse]:
        return aito_resp.GetDatabaseSchemaResponse

    @classmethod
    def make_request(cls, method: str, endpoint: str, query: Optional[Union[Dict, List]]) -> 'AitoRequest':
        return cls()


class GetTableSchemaRequest(_TableSchemaRequest, _GetSchemaRequest, _SchemaAPIRequest):
    """Request to `Get the schema of a table <https://aito.ai/docs/api/#get-api-v1-schema-table>`__"""
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

    @classmethod
    def make_request(cls, method: str, endpoint: str, query: Optional[Union[Dict, List]]) -> 'AitoRequest':
        pattern = re.compile(f'^{_SchemaAPIRequest.endpoint_prefix}/([^/".$\r\n\s]+)$')
        matched = pattern.search(endpoint)
        if matched is None:
            raise ValueError(f"invalid {cls.__name__} endpoint: '{endpoint}'")
        table_name = matched.group(1)
        return cls(table_name=table_name)
