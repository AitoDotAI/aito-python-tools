"""Aito `Data API <https://aito.ai/docs/api/#database-api>`__ Request Class"""

import re
from abc import ABC, abstractmethod
from typing import Optional, Union, Dict, List

from aito.client import responses as aito_resp
from .aito_request import AitoRequest, _FinalRequest, _PatternEndpoint, _GetRequest, _PostRequest


class DataAPIRequest(AitoRequest, ABC):
    """Request to manipulate the schema"""
    endpoint_prefix = f'{AitoRequest._api_version_endpoint_prefix}/data'

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


class UploadEntriesRequest(_PostRequest, _PatternEndpoint, DataAPIRequest):
    """Request to `Insert entries to a table <https://aito.ai/docs/api/#post-api-v1-data-table>`__"""
    response_cls = aito_resp.BaseResponse

    @classmethod
    def _endpoint_pattern(cls):
        return re.compile(f'^{cls.endpoint_prefix}/([^/".$\r\n\s]+)/batch$')

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


class DeleteEntriesRequest(_PostRequest, _FinalRequest, DataAPIRequest):
    """Request to `Delete entries of a table <https://aito.ai/docs/api/#post-api-v1-data-delete>`__"""
    endpoint = f'{DataAPIRequest.endpoint_prefix}/_delete'
    response_cls = aito_resp.BaseResponse


class InitiateFileUploadRequest(_PostRequest, _PatternEndpoint, DataAPIRequest):
    """Request to `Initiate File Upload <https://aito.ai/docs/api/#post-api-v1-data-table-file>`__"""
    response_cls = aito_resp.BaseResponse

    @classmethod
    def _endpoint_pattern(cls):
        return re.compile(f'^{cls.endpoint_prefix}/([^/".$\r\n\s]+)/file$')

    def __init__(self, table_name: str):
        """

        :param table_name: the name of the table to be uploaded
        :type table_name: str
        """
        endpoint = f'{self.endpoint_prefix}/{table_name}/file'
        super().__init__(method=self.method, endpoint=endpoint)

    @classmethod
    def make_request(cls, method: str, endpoint: str, query: Optional[Union[Dict, List]]) -> 'AitoRequest':
        groups = cls._endpoint_to_captured_groups(endpoint=endpoint)
        return cls(table_name=groups(1))


class TriggerFileProcessingRequest(_PostRequest, _PatternEndpoint, DataAPIRequest):
    """Request to `Initiate File Upload <https://aito.ai/docs/api/#post-api-v1-data-table-file>`__"""
    response_cls = aito_resp.BaseResponse

    @classmethod
    def _endpoint_pattern(cls):
        return re.compile(f'^{cls.endpoint_prefix}/([^/".$\r\n\s]+)/file/(.+)$')

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


class GetFileProcessingRequest(_GetRequest, TriggerFileProcessingRequest, DataAPIRequest):
    """Request to `Initiate File Upload <https://aito.ai/docs/api/#post-api-v1-data-table-file>`__"""
