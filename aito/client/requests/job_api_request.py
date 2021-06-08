"""Aito `Job API <https://aito.ai/docs/api/#post-api-v1-jobs-query>`__ Request Class"""

import re
from abc import abstractmethod
from typing import Optional, Union, Dict, List, Type
import traceback

from aito.client import responses as aito_resp
from .aito_request import AitoRequest, _PatternEndpoint, _GetRequest, _PostRequest
from .query_api_request import QueryAPIRequest
from .data_api_request import DataAPIRequest
from aito.schema import AitoDatabaseSchema, AitoSchema

class _JobAPIRequest(AitoRequest):
    endpoint_prefix = f'{AitoRequest._api_version_endpoint_prefix}/jobs'

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


class CreateJobRequest(_PostRequest, _PatternEndpoint, _JobAPIRequest):
    """Request to `create a job <https://aito.ai/docs/api/#post-api-v1-jobs-query>`__ for an Aito API endpoint
    """
    response_cls = aito_resp.CreateJobResponse

    def __init__(self, endpoint: str, query: Dict):
        """

        :param endpoint: the job endpoint
        :type endpoint: str
        :param query: the query
        :type query: Dict
        """
        super().__init__(method=self.method, endpoint=endpoint, query=query)

    @property
    def path(self):
        return self.endpoint.replace(f'{self.endpoint_prefix}/', '')

    @classmethod
    def _endpoint_pattern(cls):
        query_api_path_regex = '|'.join(QueryAPIRequest._query_api_paths)
        # Include the table name in the regex
        data_api_path_regex = f"data/(_delete|({AitoSchema.table_name_pattern}/({'|'.join(DataAPIRequest._data_api_jobs_methods)})))"
        return re.compile(f"^{cls.endpoint_prefix}/({query_api_path_regex}|({data_api_path_regex}))$")

    @classmethod
    def make_request(cls, method: str, endpoint: str, query: Optional[Union[Dict, List]]) -> 'AitoRequest':
        return cls(endpoint=endpoint, query=query)

    @classmethod
    def from_data_api_request(cls, request_obj: DataAPIRequest) -> 'CreateJobRequest':
        """Create a job from a DataAPI request

        :param request_obj: a :class:`.DataAPIRequest` instance
        :type request_obj: DataAPIRequest
        :return: the corresponding create job request
        :rtype: CreateJobRequest
        """
        endpoint = f'{cls.endpoint_prefix}/{request_obj.path}'
        print(f"\nThe final endpoint for the data-api request is {endpoint}")
        return cls(endpoint=endpoint, query=request_obj.query)

    @classmethod
    def from_query_api_request(cls, request_obj: QueryAPIRequest) -> 'CreateJobRequest':
        """Create a job from a QueryAPI request

        :param request_obj: a :class:`.QueryAPIRequest` instance
        :type request_obj: QueryAPIRequest
        :return: the corresponding create job request
        :rtype: CreateJobRequest
        """
        endpoint = f'{cls.endpoint_prefix}/{request_obj.path}'
        return cls(endpoint=endpoint, query=request_obj.query)

    @property
    def result_response_cls(self) -> Type[aito_resp.BaseResponse]:
        """returns the response class of the job request result"""
        for sub_cls in QueryAPIRequest.__subclasses__():
            if self.path == sub_cls.path:
                return sub_cls.response_cls
        for sub_cls in DataAPIRequest.__subclasses__():
            if sub_cls.path_matches(self.path):
                return sub_cls.response_cls


class GetJobStatusRequest(_GetRequest, _PatternEndpoint, _JobAPIRequest):
    """Request to `create a job <https://aito.ai/docs/api/#post-api-v1-jobs-query>`__ for an Aito API endpoint
    """
    response_cls = aito_resp.GetJobStatusResponse

    def __init__(self, job_id: str):
        """

        :param job_id: the id of the job session
        :type job_id: str
        """
        endpoint = f'{self.endpoint_prefix}/{job_id}'
        super().__init__(method=self.method, endpoint=endpoint)

    @classmethod
    def _endpoint_pattern(cls):
        return re.compile(f'^{cls.endpoint_prefix}/(.+)$')

    @classmethod
    def make_request(cls, method: str, endpoint: str, query: Optional[Union[Dict, List]]) -> 'AitoRequest':
        groups = cls._endpoint_to_captured_groups(endpoint=endpoint)
        return cls(job_id=groups(1))


class GetJobResultRequest(_GetRequest, _PatternEndpoint, _JobAPIRequest):
    """Request to `create a job <https://aito.ai/docs/api/#post-api-v1-jobs-query>`__ for an Aito API endpoint
    """
    response_cls = aito_resp.BaseResponse

    def __init__(self, job_id: str):
        """

        :param job_id: the id of the job session
        :type job_id: str
        """
        endpoint = f'{self.endpoint_prefix}/{job_id}/result'
        super().__init__(method=self.method, endpoint=endpoint)

    @classmethod
    def _endpoint_pattern(cls):
        return re.compile(f'^{cls.endpoint_prefix}/(.+)/result$')

    @classmethod
    def make_request(cls, method: str, endpoint: str, query: Optional[Union[Dict, List]]) -> 'AitoRequest':
        groups = cls._endpoint_to_captured_groups(endpoint=endpoint)
        return cls(job_id=groups(1))
