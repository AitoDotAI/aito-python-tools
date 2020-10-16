"""Aito `Schema API <https://aito.ai/docs/api/#database-api>`__ Request Class"""

import re
from abc import ABC, abstractmethod
from typing import Optional, Union, Dict, List

from aito.client import responses as aito_responses
from .aito_request import AitoRequest, _PatternEndpoint, _GetRequest, _PutRequest, _DeleteRequest
from aito.schema import AitoDatabaseSchema, AitoTableSchema


class _SchemaAPIRequest(AitoRequest, ABC):
    """Request to manipulate the schema"""
    endpoint_prefix = f'{AitoRequest._api_version_endpoint_prefix}/schema'

    @classmethod
    @abstractmethod
    def _check_method(cls, method: str) -> bool:
        return method in cls._request_methods

    @classmethod
    @abstractmethod
    def _check_endpoint(cls, endpoint: str) -> bool:
        return endpoint.startswith(cls.endpoint_prefix)

    @classmethod
    @abstractmethod
    def make_request(cls, method: str, endpoint: str, query: Optional[Union[Dict, List]]) -> 'AitoRequest':
        for sub_cls in cls.__subclasses__():
            if sub_cls._check_method(method) and sub_cls._check_endpoint(endpoint):
                return sub_cls.make_request(method=method, endpoint=endpoint, query=query)
        raise ValueError(f"invalid {cls.__name__} with '{method}({endpoint})'")


class _DatabaseSchemaRequest:
    """Request to manipulate the database schema"""
    endpoint = _SchemaAPIRequest.endpoint_prefix

    @classmethod
    def _check_endpoint(cls, endpoint: str):
        return endpoint == _SchemaAPIRequest.endpoint_prefix


class _TableSchemaRequest(_PatternEndpoint):
    """Request to manipulate a table schema"""
    @classmethod
    def _endpoint_pattern(cls):
        return re.compile(f'^{_SchemaAPIRequest.endpoint_prefix}/([^/".$\r\n\s]+)$')


class _ColumnSchemaRequest(_PatternEndpoint):
    """Request to manipulate a column schema"""
    @classmethod
    def _endpoint_pattern(cls):
        return re.compile(f'^{_SchemaAPIRequest.endpoint_prefix}/([^/".$\r\n\s]+)/([^/".$\r\n\s]+)$')


class GetDatabaseSchemaRequest(_GetRequest, _DatabaseSchemaRequest, _SchemaAPIRequest):
    """Request to `Get the schema of the database <https://aito.ai/docs/api/#get-api-v1-schema>`__"""
    response_cls = aito_responses.DatabaseSchemaResponse

    def __init__(self):
        super().__init__(method=self.method, endpoint=self.endpoint)

    @classmethod
    def make_request(cls, method: str, endpoint: str, query: Optional[Union[Dict, List]]) -> 'AitoRequest':
        return cls()


class GetTableSchemaRequest(_GetRequest, _TableSchemaRequest, _SchemaAPIRequest):
    """Request to `Get the schema of a table <https://aito.ai/docs/api/#get-api-v1-schema-table>`__"""
    response_cls = aito_responses.TableSchemaResponse

    def __init__(self, table_name: str):
        """

        :param table_name: the name of the table
        :type table_name: str
        """
        endpoint = f'{self.endpoint_prefix}/{table_name}'
        super().__init__(method=self.method, endpoint=endpoint)

    @classmethod
    def make_request(cls, method: str, endpoint: str, query: Optional[Union[Dict, List]]) -> 'AitoRequest':
        groups = cls._endpoint_to_captured_groups(endpoint=endpoint)
        return cls(table_name=groups(1))


class GetColumnSchemaRequest(_GetRequest, _ColumnSchemaRequest, _SchemaAPIRequest):
    """Request to `Get the schema of a column <https://aito.ai/docs/api/#get-api-v1-schema-column>`__"""
    response_cls = aito_responses.ColumnSchemaResponse

    def __init__(self, table_name: str, column_name: str):
        """

        :param table_name: the name of the table containing the column
        :type table_name: str
        :param column_name: the name of the column
        :type column_name: str
        """
        endpoint = f'{self.endpoint_prefix}/{table_name}/{column_name}'
        super().__init__(method=self.method, endpoint=endpoint)

    @classmethod
    def make_request(cls, method: str, endpoint: str, query: Optional[Union[Dict, List]]) -> 'AitoRequest':
        groups = cls._endpoint_to_captured_groups(endpoint=endpoint)
        return cls(table_name=groups(1), column_name=groups(2))


class CreateDatabaseSchemaRequest(_PutRequest, _DatabaseSchemaRequest, _SchemaAPIRequest):
    """Request to `Create the schema of the database <https://aito.ai/docs/api/#put-api-v1-schema>`__"""
    endpoint = _SchemaAPIRequest.endpoint_prefix
    response_cls = aito_responses.DatabaseSchemaResponse

    def __init__(self, schema: Union[AitoDatabaseSchema, Dict]):
        """

        :param schema: Aito database schema
        :type schema: Union[AitoDatabaseSchema, Dict]
        """
        query = schema.to_json_serializable() if isinstance(schema, AitoDatabaseSchema) else schema
        super().__init__(method=self.method, endpoint=self.endpoint, query=query)

    @classmethod
    def make_request(cls, method: str, endpoint: str, query: Optional[Union[Dict, List]]) -> 'AitoRequest':
        return cls(schema=query)


class CreateTableSchemaRequest(_PutRequest, _TableSchemaRequest, _SchemaAPIRequest):
    """Request to `Create a table <https://aito.ai/docs/api/#put-api-v1-schema-table>`__"""
    response_cls = aito_responses.schema_api_response.TableSchemaResponse

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

    @classmethod
    def make_request(cls, method: str, endpoint: str, query: Optional[Union[Dict, List]]) -> 'AitoRequest':
        groups = cls._endpoint_to_captured_groups(endpoint=endpoint)
        return cls(table_name=groups(1), schema=query)


class CreateColumnSchemaRequest(_PutRequest, _ColumnSchemaRequest, _SchemaAPIRequest):
    """Request to `Add or replace a column <https://aito.ai/docs/api/#put-api-v1-schema-column>`__"""
    response_cls = aito_responses.ColumnSchemaResponse

    def __init__(self, table_name: str, column_name: str, schema: Dict):
        """

        :param table_name: the name of the table containing the column
        :type table_name: str
        :param column_name: the name of the column
        :type column_name: str
        :param schema: the schema of the column
        :type schema: Union[AitoColumnTypeSchema, Dict]
        """
        endpoint = f'{self.endpoint_prefix}/{table_name}/{column_name}'
        super().__init__(method=self.method, endpoint=endpoint, query=schema)

    @classmethod
    def make_request(cls, method: str, endpoint: str, query: Optional[Union[Dict, List]]) -> 'AitoRequest':
        groups = cls._endpoint_to_captured_groups(endpoint=endpoint)
        return cls(table_name=groups(1), column_name=groups(2), schema=query)


class DeleteDatabaseSchemaRequest(_DeleteRequest, _DatabaseSchemaRequest, _SchemaAPIRequest):
    """Request to `Delete the schema of the database <https://aito.ai/docs/api/#delete-api-v1-schema>`__"""
    endpoint = _SchemaAPIRequest.endpoint_prefix
    response_cls = aito_responses.BaseResponse

    def __init__(self):
        super().__init__(method=self.method, endpoint=self.endpoint)

    @classmethod
    def make_request(cls, method: str, endpoint: str, query: Optional[Union[Dict, List]]) -> 'AitoRequest':
        return cls()


class DeleteTableSchemaRequest(_DeleteRequest, _TableSchemaRequest, _SchemaAPIRequest):
    """Request to `Delete a table <https://aito.ai/docs/api/#delete-api-v1-schema-table>`__"""
    response_cls = aito_responses.BaseResponse

    def __init__(self, table_name: str):
        """

        :param table_name: the name of the table
        :type table_name: str
        """
        endpoint = f'{self.endpoint_prefix}/{table_name}'
        super().__init__(method=self.method, endpoint=endpoint)

    @classmethod
    def make_request(cls, method: str, endpoint: str, query: Optional[Union[Dict, List]]) -> 'AitoRequest':
        groups = cls._endpoint_to_captured_groups(endpoint=endpoint)
        return cls(table_name=groups(1))


class DeleteColumnSchemaRequest(_DeleteRequest, _ColumnSchemaRequest, _SchemaAPIRequest):
    """Request to `Delete a column <https://aito.ai/docs/api/#delete-api-v1-schema-column>`__"""
    response_cls = aito_responses.BaseResponse

    def __init__(self, table_name: str, column_name: str):
        """

        :param table_name: the name of the table containing the column
        :type table_name: str
        :param column_name: the name of the column
        :type column_name: str
        """
        endpoint = f'{self.endpoint_prefix}/{table_name}/{column_name}'
        super().__init__(method=self.method, endpoint=endpoint)

    @classmethod
    def make_request(cls, method: str, endpoint: str, query: Optional[Union[Dict, List]]) -> 'AitoRequest':
        groups = cls._endpoint_to_captured_groups(endpoint=endpoint)
        return cls(table_name=groups(1), column_name=groups(2))