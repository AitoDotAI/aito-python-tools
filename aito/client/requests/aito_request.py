"""Generic Aito Request Class"""

import logging
from abc import ABC, abstractmethod
from typing import Optional, Union, Dict, List

from aito.client import responses as aito_responses

LOG = logging.getLogger('AitoClientRequest')


class AitoRequest(ABC):
    """The base class of Request"""
    _api_version_endpoint_prefix = '/api/v1'
    _request_methods = ['PUT', 'POST', 'GET', 'DELETE']

    #: the class of the response for this request class
    response_cls = None

    def __init__(self, method: str, endpoint: str, query: Optional[Union[Dict, List]] = None):
        """

        :param method: the method of the request
        :type method: str
        :param endpoint: the endpoint of the request
        :type endpoint: str
        :param query: an Aito query if applicable, optional
        :type query: Optional[Union[Dict, List]]
        """
        if self.response_cls is None:
            raise NotImplementedError("The request 'response_cls' must be implemented")

        method = method.upper()
        if not self._check_method(method=method):
            raise ValueError(f"invalid method '{method}' for {self.__class__.__name__}")
        if not self._check_endpoint(endpoint=endpoint):
            raise ValueError(f"invalid endpoint '{endpoint}' for {self.__class__.__name__}")

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

    @classmethod
    @abstractmethod
    def _check_method(cls, method: str) -> bool:
        """check if the input method is a valid endpoint of the Request class

        :param method: the input method
        :type method: str
        :return: True if the method is valid
        :rtype: bool
        """
        pass

    @classmethod
    @abstractmethod
    def _check_endpoint(cls, endpoint: str) -> bool:
        """check if the input endpoint is a valid endpoint of the Request class

        :param endpoint: the input endpoint
        :type endpoint: str
        :return: True if the endpoint is valid
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
            if sub_cls != BaseRequest and sub_cls._check_method(method) and sub_cls._check_endpoint(endpoint):
                try:
                    instance = sub_cls.make_request(method=method, endpoint=endpoint, query=query)
                    return instance
                except Exception as e:
                    LOG.debug(f"invalid {sub_cls.__name__} with '{method}({endpoint}): {e}'")
        return BaseRequest(method=method, endpoint=endpoint, query=query)


class _FinalRequest(AitoRequest, ABC):
    """Request with fixed method and endpoint"""
    method = None
    endpoint = None

    def __init__(self, query: Optional[Union[Dict, List]] = None):
        if self.method is None:
            raise NotImplementedError(f"The request 'method' must be implemented")
        if self.endpoint is None:
            raise NotImplementedError(f"The request 'endpoint' must be implemented")
        super().__init__(method=self.method, endpoint=self.endpoint, query=query)

    @classmethod
    def _check_method(cls, method: str) -> bool:
        return method == cls.method

    @classmethod
    def _check_endpoint(cls, endpoint: str) -> bool:
        return endpoint == cls.endpoint

    @classmethod
    def make_request(cls, method: str, endpoint: str, query: Optional[Union[Dict, List]]) -> 'AitoRequest':
        return cls(query=query)


class _PatternEndpoint(ABC):
    """Request whose endpoint has a pattern"""
    @classmethod
    @abstractmethod
    def _endpoint_pattern(cls):
        pass

    @classmethod
    def _check_endpoint(cls, endpoint: str) -> bool:
        return cls._endpoint_pattern().search(endpoint) is not None

    @classmethod
    def _endpoint_to_captured_groups(cls, endpoint: str):
        matched = cls._endpoint_pattern().search(endpoint)
        if matched is None:
            raise ValueError(f"invalid {cls.__name__} endpoint: '{endpoint}'")
        return matched.group


class _GetRequest:
    method = 'GET'

    @classmethod
    def _check_method(cls, method: str):
        return method == cls.method


class _PostRequest:
    method = 'POST'

    @classmethod
    def _check_method(cls, method: str):
        return method == cls.method


class _PutRequest:
    method = 'PUT'

    @classmethod
    def _check_method(cls, method: str):
        return method == cls.method


class _DeleteRequest:
    method = 'DELETE'

    @classmethod
    def _check_method(cls, method: str):
        return method == cls.method


class BaseRequest(AitoRequest):
    """Base request to the Aito instance"""
    #: the class of the response for this request class
    response_cls = aito_responses.BaseResponse

    @classmethod
    def _check_endpoint(cls, endpoint: str) -> bool:
        """check if the input endpoint is a valid Aito endpoint
        """
        if not endpoint.startswith('/'):
            return False
        if not endpoint.startswith(cls._api_version_endpoint_prefix) and endpoint != GetVersionRequest.endpoint:
            return False
        return True

    @classmethod
    def _check_method(cls, method: str) -> bool:
        """returns True if the input request method is valid"""
        return method in cls._request_methods

    @classmethod
    def make_request(cls, method: str, endpoint: str, query: Optional[Union[Dict, List]]) -> 'AitoRequest':
        return cls(method=method, endpoint=endpoint, query=query)


class GetVersionRequest(_GetRequest, _FinalRequest):
    """Request to get the Aito instance version"""
    endpoint = '/version'
    response_cls = aito_responses.GetVersionResponse

    def __init__(self):
        super().__init__(query=None)