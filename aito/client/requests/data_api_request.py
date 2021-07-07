"""Aito `Data API <https://aito.ai/docs/api/#database-api>`__ Request Class"""

import re
from abc import ABC, abstractmethod
from typing import Optional, Union, Dict, List

from aito.client import responses as aito_resp
from .aito_request import AitoRequest, _FinalRequest, _PatternEndpoint, _GetRequest, _PostRequest
from aito.schema import AitoSchema


class DataAPIRequest(AitoRequest, ABC):
    """Request to manipulate the schema"""
    endpoint_prefix = f'{AitoRequest._api_version_endpoint_prefix}/data'
    # Partial list of available endpoints.
    _data_api_jobs_methods = ['optimize', 'batch']

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

    @classmethod
    @abstractmethod
    def path_matches(cls, path_to_match):
        return False

class UploadEntriesRequest(_PostRequest, _PatternEndpoint, DataAPIRequest):
    """Request to `Insert entries to a table <https://aito.ai/docs/api/#post-api-v1-data-table>`__"""
    _path_suffix = 'batch'
    response_cls = aito_resp.BaseResponse

    @classmethod
    def _endpoint_pattern(cls):
        return re.compile(f'^{cls.endpoint_prefix}/({AitoSchema.table_name_pattern})/{cls._path_suffix}$')

    def __init__(self, table_name: str, entries: List[Dict]):
        """

        :param table_name: the name of the table to be uploaded
        :type table_name: str
        :param entries: a list of the table entries
        :type entries: List[Dict]
        """
        endpoint = f'{self.endpoint_prefix}/{table_name}/batch'
        super().__init__(method=self.method, endpoint=endpoint, query=entries)

    @classmethod
    def make_request(cls, method: str, endpoint: str, query: Optional[Union[Dict, List]]) -> 'AitoRequest':
        groups = cls._endpoint_to_captured_groups(endpoint=endpoint)
        return cls(table_name=groups(1), entries=query)

    @classmethod
    def path_matches(cls, path_to_match):
        return path_to_match.endswith(f'/{cls._path_suffix}')


class OptimizeTableRequest(_PostRequest, _FinalRequest, DataAPIRequest):
    """Request to `Optimize a table <https://aito.ai/docs/api/#post-api-v1-data-table-optimize>`__"""
    _path_suffix = 'optimize'

    def __init__(self, table_name: str, session_id: str):
        """

        :param table_name: the name of the table to be optimized
        :type table_name: str
        """
        endpoint = f'{self.endpoint_prefix}/{table_name}/optimize'
        super().__init__(method=self.method, endpoint=endpoint)

    @classmethod
    def _endpoint_pattern(cls):
        return re.compile(f'^{cls.endpoint_prefix}/{AitoSchema.table_name_pattern}/{cls._path_suffix}$')

    @classmethod
    def make_request(cls, method: str, endpoint: str, query: Optional[Union[Dict, List]]) -> 'AitoRequest':
        groups = cls._endpoint_to_captured_groups(endpoint=endpoint)
        return cls(table_name=groups(1), session_id=groups(2))

    @classmethod
    def path_matches(cls, path_to_match):
        return path_to_match.endswith(f'/{cls._path_suffix}')

    response_cls = aito_resp.BaseResponse

class DeleteEntriesRequest(_PostRequest, _FinalRequest, DataAPIRequest):
    """Request to `Delete entries of a table <https://aito.ai/docs/api/#post-api-v1-data-delete>`__"""
    _path_suffix = '_delete'

    @classmethod
    def _endpoint_pattern(cls):
        return re.compile(f'^{cls.endpoint_prefix}/{cls._path_suffix}$')

    @classmethod
    def path_matches(cls, path_to_match):
        return path_to_match.endswith(f'/{cls._path_suffix}')


    endpoint = f'{DataAPIRequest.endpoint_prefix}/{_path_suffix}'
    response_cls = aito_resp.BaseResponse


class InitiateFileUploadRequest(_PostRequest, _PatternEndpoint, DataAPIRequest):
    """Request to `Initiate File Upload <https://aito.ai/docs/api/#post-api-v1-data-table-file>`__"""
    _path_suffix = 'file'
    response_cls = aito_resp.BaseResponse

    @classmethod
    def _endpoint_pattern(cls):
        return re.compile(f'^{cls.endpoint_prefix}/({AitoSchema.table_name_pattern})/{cls._path_suffix}$')

    def __init__(self, table_name: str):
        """

        :param table_name: the name of the table to be uploaded
        :type table_name: str
        """
        endpoint = f'{self.endpoint_prefix}/{table_name}/{self._path_suffix}'
        super().__init__(method=self.method, endpoint=endpoint)

    @classmethod
    def path_matches(cls, path_to_match):
        return path_to_match.endswith(f'/{cls._path_suffix}')

    @classmethod
    def make_request(cls, method: str, endpoint: str, query: Optional[Union[Dict, List]]) -> 'AitoRequest':
        groups = cls._endpoint_to_captured_groups(endpoint=endpoint)
        return cls(table_name=groups(1))


class TriggerFileProcessingRequest(_PostRequest, _PatternEndpoint, DataAPIRequest):
    """Request to `Initiate File Upload <https://aito.ai/docs/api/#post-api-v1-data-table-file>`__"""
    response_cls = aito_resp.BaseResponse

    @classmethod
    def _endpoint_pattern(cls):
        return re.compile(f'^{cls.endpoint_prefix}/({AitoSchema.table_name_pattern})/file/({AitoSchema.uuid_pattern})$')

    def __init__(self, table_name: str, session_id: str):
        """

        :param table_name: the name of the table to be uploaded
        :type table_name: str
        :param session_id: The uuid of the file upload session from initiating file upload
        :type session_id: str
        """
        endpoint = f'{self.endpoint_prefix}/{table_name}/file/{session_id}'
        super().__init__(method=self.method, endpoint=endpoint)

    @classmethod
    def make_request(cls, method: str, endpoint: str, query: Optional[Union[Dict, List]]) -> 'AitoRequest':
        groups = cls._endpoint_to_captured_groups(endpoint=endpoint)
        return cls(table_name=groups(1), session_id=groups(2))

    @classmethod
    def path_matches(cls, path_to_match):
        matcher = re.compile(f'file/{AitoSchema.uuid_pattern}$')
        return bool(matcher.match(path_to_match))



class GetFileProcessingRequest(_GetRequest, TriggerFileProcessingRequest, DataAPIRequest):
    """Request to `Initiate File Upload <https://aito.ai/docs/api/#post-api-v1-data-table-file>`__"""
