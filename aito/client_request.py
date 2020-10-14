"""Request objects that is sent by an :class:`~aito.client.AitoClient` to an Aito instance

"""

import logging
import re
from abc import ABC, abstractmethod
from typing import Optional, Union, Dict, List, Type, Tuple

import aito.client_response as aito_resp
from aito.schema import AitoDatabaseSchema, AitoTableSchema, AitoColumnTypeSchema

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

    def __eq__(self, other):
        return self.method == other.method and self.endpoint == other.endpoint and self.query == other.query

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
        method = method.upper()
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

    def __init__(self, query: Dict):
        """

        :param query: an Aito query if applicable, optional
        :type query: Dict
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
    def check_method(cls, method: str) -> bool:
        return method in cls.request_methods

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
    endpoint = _SchemaAPIRequest.endpoint_prefix

    @classmethod
    def check_endpoint(cls, endpoint: str):
        return endpoint == _SchemaAPIRequest.endpoint_prefix


class _TableSchemaRequest:
    """Request to manipulate a table schema"""
    @classmethod
    def check_endpoint(cls, endpoint: str) -> bool:
        pattern = re.compile(f'^{_SchemaAPIRequest.endpoint_prefix}/[^/".$\r\n\s]+$')
        return pattern.match(endpoint) is not None

    @classmethod
    def endpoint_to_table_name(cls, endpoint) -> str:
        pattern = re.compile(f'^{_SchemaAPIRequest.endpoint_prefix}/([^/".$\r\n\s]+)$')
        matched = pattern.search(endpoint)
        if matched is None:
            raise ValueError(f"invalid {cls.__name__} endpoint: '{endpoint}'")
        table_name = matched.group(1)
        return table_name


class _ColumnSchemaRequest:
    """Request to manipulate a column schema"""
    @classmethod
    def check_endpoint(cls, endpoint: str) -> bool:
        pattern = re.compile(f'^{_SchemaAPIRequest.endpoint_prefix}/[^/".$\r\n\s]+/[^/".$\r\n\s]+$')
        return pattern.match(endpoint) is not None

    @classmethod
    def endpoint_to_table_name_and_column_name(cls, endpoint: str) -> Tuple[str, str]:
        pattern = re.compile(f'^{_SchemaAPIRequest.endpoint_prefix}/([^/".$\r\n\s]+)/([^/".$\r\n\s]+)$')
        matched = pattern.search(endpoint)
        if matched is None:
            raise ValueError(f"invalid {cls.__name__} endpoint: '{endpoint}'")
        table_name, column_name = matched.group(1), matched.group(2)
        return table_name, column_name


class _GetSchemaRequest:
    """Request to get schema"""
    method = 'GET'

    @classmethod
    def check_method(cls, method: str):
        return method == cls.method


class GetDatabaseSchemaRequest(_DatabaseSchemaRequest, _GetSchemaRequest, _SchemaAPIRequest):
    """Request to `Get the schema of the database <https://aito.ai/docs/api/#get-api-v1-schema>`__"""
    def __init__(self):
        super().__init__(method=self.method, endpoint=self.endpoint)

    @property
    def response_cls(self) -> Type[aito_resp.BaseResponse]:
        return aito_resp.DatabaseSchemaResponse

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
        return aito_resp.TableSchemaResponse

    @classmethod
    def make_request(cls, method: str, endpoint: str, query: Optional[Union[Dict, List]]) -> 'AitoRequest':
        table_name = cls.endpoint_to_table_name(endpoint=endpoint)
        return cls(table_name=table_name)


class GetColumnSchemaRequest(_ColumnSchemaRequest, _GetSchemaRequest, _SchemaAPIRequest):
    """Request to `Get the schema of a column <https://aito.ai/docs/api/#get-api-v1-schema-column>`__"""
    def __init__(self, table_name: str, column_name: str):
        """

        :param table_name: the name of the table containing the column
        :type table_name: str
        :param column_name: the name of the column
        :type column_name: str
        """
        endpoint = f'{self.endpoint_prefix}/{table_name}/{column_name}'
        super().__init__(method=self.method, endpoint=endpoint)

    @property
    def response_cls(self) -> Type[aito_resp.BaseResponse]:
        return aito_resp.ColumnSchemaResponse

    @classmethod
    def make_request(cls, method: str, endpoint: str, query: Optional[Union[Dict, List]]) -> 'AitoRequest':
        table_name, column_name = cls.endpoint_to_table_name_and_column_name(endpoint=endpoint)
        return cls(table_name=table_name, column_name=column_name)


class _CreateSchemaRequest:
    """Request to create schema"""
    method = 'PUT'

    @classmethod
    def check_method(cls, method: str):
        return method == cls.method


class CreateDatabaseSchemaRequest(_DatabaseSchemaRequest, _CreateSchemaRequest, _SchemaAPIRequest):
    """Request to `Create the schema of the database <https://aito.ai/docs/api/#put-api-v1-schema>`__"""
    endpoint = _SchemaAPIRequest.endpoint_prefix

    def __init__(self, schema: Union[AitoDatabaseSchema, Dict]):
        """

        :param schema: Aito database schema
        :type schema: Union[AitoDatabaseSchema, Dict]
        """
        query = schema.to_json_serializable() if isinstance(schema, AitoDatabaseSchema) else schema
        super().__init__(method=self.method, endpoint=self.endpoint, query=query)

    @property
    def response_cls(self) -> Type[aito_resp.BaseResponse]:
        return aito_resp.DatabaseSchemaResponse

    @classmethod
    def make_request(cls, method: str, endpoint: str, query: Optional[Union[Dict, List]]) -> 'AitoRequest':
        return cls(schema=query)


class CreateTableSchemaRequest(_TableSchemaRequest, _CreateSchemaRequest, _SchemaAPIRequest):
    """Request to `Create a table <https://aito.ai/docs/api/#put-api-v1-schema-table>`__"""
    def __init__(self, table_name: str, schema: Union[AitoTableSchema, Dict]):
        """

        :param table_name: the name of the table
        :type table_name: str
        :param schema: the schema of the table
        :type schema: Union[AitoDatabaseSchema, Dict]
        """
        endpoint = f'{self.endpoint_prefix}/{table_name}'
        query = schema.to_json_serializable() if isinstance(schema, AitoTableSchema) else schema
        super().__init__(method=self.method, endpoint=endpoint, query=query)

    @property
    def response_cls(self) -> Type[aito_resp.BaseResponse]:
        return aito_resp.TableSchemaResponse

    @classmethod
    def make_request(cls, method: str, endpoint: str, query: Optional[Union[Dict, List]]) -> 'AitoRequest':
        table_name = cls.endpoint_to_table_name(endpoint=endpoint)
        return cls(table_name=table_name, schema=query)


class CreateColumnSchemaRequest(_ColumnSchemaRequest, _CreateSchemaRequest, _SchemaAPIRequest):
    """Request to `Add or replace a column <https://aito.ai/docs/api/#put-api-v1-schema-column>`__"""
    def __init__(self, table_name: str, column_name: str, schema: Union[AitoColumnTypeSchema, Dict]):
        """

        :param table_name: the name of the table containing the column
        :type table_name: str
        :param column_name: the name of the column
        :type column_name: str
        :param schema: the schema of the column
        :type schema: Union[AitoColumnTypeSchema, Dict]
        """
        endpoint = f'{self.endpoint_prefix}/{table_name}/{column_name}'
        query = schema.to_json_serializable() if isinstance(schema, AitoColumnTypeSchema) else schema
        super().__init__(method=self.method, endpoint=endpoint, query=query)

    @property
    def response_cls(self) -> Type[aito_resp.BaseResponse]:
        return aito_resp.ColumnSchemaResponse

    @classmethod
    def make_request(cls, method: str, endpoint: str, query: Optional[Union[Dict, List]]) -> 'AitoRequest':
        table_name, column_name = cls.endpoint_to_table_name_and_column_name(endpoint)
        return cls(table_name=table_name, column_name=column_name, schema=query)


class _DeleteSchemaRequest:
    """Request to delete schema"""
    method = 'DELETE'

    @classmethod
    def check_method(cls, method: str):
        return method == cls.method


class DeleteDatabaseSchemaRequest(_DatabaseSchemaRequest, _DeleteSchemaRequest, _SchemaAPIRequest):
    """Request to `Delete the schema of the database <https://aito.ai/docs/api/#delete-api-v1-schema>`__"""
    endpoint = _SchemaAPIRequest.endpoint_prefix

    def __init__(self):
        super().__init__(method=self.method, endpoint=self.endpoint)

    @property
    def response_cls(self) -> Type[aito_resp.BaseResponse]:
        return aito_resp.BaseResponse

    @classmethod
    def make_request(cls, method: str, endpoint: str, query: Optional[Union[Dict, List]]) -> 'AitoRequest':
        return cls()


class DeleteTableSchemaRequest(_TableSchemaRequest, _DeleteSchemaRequest, _SchemaAPIRequest):
    """Request to `Delete a table <https://aito.ai/docs/api/#delete-api-v1-schema-table>`__"""
    def __init__(self, table_name: str):
        """

        :param table_name: the name of the table
        :type table_name: str
        """
        endpoint = f'{self.endpoint_prefix}/{table_name}'
        super().__init__(method=self.method, endpoint=endpoint)

    @property
    def response_cls(self) -> Type[aito_resp.BaseResponse]:
        return aito_resp.BaseResponse

    @classmethod
    def make_request(cls, method: str, endpoint: str, query: Optional[Union[Dict, List]]) -> 'AitoRequest':
        table_name = cls.endpoint_to_table_name(endpoint=endpoint)
        return cls(table_name=table_name)


class DeleteColumnSchemaRequest(_ColumnSchemaRequest, _DeleteSchemaRequest, _SchemaAPIRequest):
    """Request to `Delete a column <https://aito.ai/docs/api/#delete-api-v1-schema-column>`__"""
    def __init__(self, table_name: str, column_name: str):
        """

        :param table_name: the name of the table containing the column
        :type table_name: str
        :param column_name: the name of the column
        :type column_name: str
        """
        endpoint = f'{self.endpoint_prefix}/{table_name}/{column_name}'
        super().__init__(method=self.method, endpoint=endpoint)

    @property
    def response_cls(self) -> Type[aito_resp.BaseResponse]:
        return aito_resp.BaseResponse

    @classmethod
    def make_request(cls, method: str, endpoint: str, query: Optional[Union[Dict, List]]) -> 'AitoRequest':
        table_name, column_name = cls.endpoint_to_table_name_and_column_name(endpoint)
        return cls(table_name=table_name, column_name=column_name)