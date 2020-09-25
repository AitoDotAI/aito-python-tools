"""A versatile client that makes requests to an Aito Database Instance

"""

import asyncio
import logging
import warnings
from typing import Dict, List, Union, Tuple, Optional

import requests as requestslib
from aiohttp import ClientSession, ClientResponseError

from .client_request import BaseRequest, SearchRequest, PredictRequest, RecommendRequest, EvaluateRequest, \
    SimilarityRequest, MatchRequest, RelateRequest, GenericQueryRequest
from .client_response import BaseResponse, SearchResponse, PredictResponse, RecommendResponse, EvaluateResponse, \
    SimilarityResponse, MatchResponse, RelateResponse, HitsResponse
from aito.exceptions import BaseError
LOG = logging.getLogger('AitoClient')


class Error(BaseError):
    """An error occurred when using the client

    """
    def __init__(self, message: str):
        super().__init__(message, LOG)


class RequestError(Error):
    """An error occurred when sending a request to the Aito instance

    """
    def __init__(self, request_obj: BaseRequest, error: Exception):
        """

        :param request_obj: the request object
        :type request_obj: BaseRequest
        :param error: the error
        :type error: Exception
        """
        self.request_obj = request_obj
        self.error = error
        if isinstance(error, requestslib.HTTPError):
            resp = error.response.json()
            error_msg = resp['message'] if 'message' in resp else resp
        elif isinstance(error, ClientResponseError):
            error_msg = error.message
        else:
            error_msg = str(error)
        super().__init__(f'failed to {request_obj}: {error_msg}')


class AitoClient:
    """A versatile client that connects to the Aito Database Instance

    """
    _request_response_map = {
        SearchRequest: SearchResponse,
        PredictRequest: PredictResponse,
        RecommendRequest: RecommendResponse,
        EvaluateRequest: EvaluateResponse,
        SimilarityRequest: SimilarityResponse,
        MatchRequest: MatchResponse,
        RelateRequest: RelateResponse,
        GenericQueryRequest: HitsResponse
    }

    def __init__(
            self,
            instance_url: str,
            api_key: str,
            check_credentials: bool = True,
            raise_for_status: bool = True
    ):
        """

        :param instance_url: Aito instance url
        :type instance_url: str
        :param api_key: Aito instance API key
        :type api_key: str
        :param check_credentials: check the given credentials by requesting the Aito instance version, defaults to True
        :type check_credentials: bool
        :param raise_for_status: automatically raise RequestError for each failed response, defaults to True
        :type raise_for_status: bool
        :raises BaseError: an error occurred during the creation of AitoClient

        >>> aito_client = AitoClient(your_instance_url, your_api_key) # doctest: +SKIP
        >>> # Change the API key to READ-WRITE or READ-ONLY
        >>> aito_client.api_key = new_api_key # doctest: +SKIP
        """
        self.instance_url = instance_url.strip("/")
        self.api_key = api_key
        self.raise_for_status = raise_for_status
        if check_credentials:
            try:
                self.request(BaseRequest('GET', '/version'))
            except Exception:
                raise Error('failed to instantiate Aito Client, please check your credentials')

    @property
    def headers(self):
        """ the headers that will be used to send a request to the Aito instance

        :rtype: Dict
        """
        return {'Content-Type': 'application/json', 'x-api-key': self.api_key}

    # noinspection PyProtectedMember
    def _get_response(self, request: BaseRequest, json_response: Dict):
        """return the appropriate response object"""
        for req_type, resp_type in self._request_response_map.items():
            if request._is_same_type(req_type({})):
                return resp_type(json_response)
        return BaseResponse(json_response)

    def request(
            self, request_obj: BaseRequest, raise_for_status: Optional[bool] = None
    ) -> Union[BaseResponse, RequestError]:
        """make a request to an Aito API endpoint
        The client returns a JSON response if the request succeed and a :class:`.RequestError` if the request fails

        :param request_obj: request object
        :type request_obj: BaseRequest
        :param raise_for_status: raise :class:`.RequestError` if the request fails.
        If set to None, value from Client will be used. Defaults to True
        :type raise_for_status: bool
        :raises RequestError: an error occurred during the execution of the request and raise_for_status
        :return: request JSON content or :class:`.RequestError` if an error occurred and not raise_for_status
        :rtype: Union[BaseResponse, RequestError]

        Simple request to get the schema of a table:

        >>> res = client.request(BaseRequest(method="GET", endpoint="/api/v1/schema/impressions"))
        >>> res.to_json_string(indent=2, sort_keys=True)
        {
          "columns": {
            "product": {
              "link": "products.id",
              "nullable": false,
              "type": "String"
            },
            "purchase": {
              "nullable": false,
              "type": "Boolean"
            },
            "session": {
              "link": "sessions.id",
              "nullable": false,
              "type": "String"
            }
          },
          "type": "table"
        }

         Sends a `PREDICT <https://aito.ai/docs/api/#post-api-v1-predict>`__ query:

         >>> res = client.request(PredictRequest(
         ...    query={
         ...        "from": "impressions",
         ...        "where": { "session": "veronica" },
         ...        "predict": "product.name",
         ...        "limit": 1
         ...    }
         ... )) # doctest: +NORMALIZE_WHITESPACE
         >>> print(res.predictions)
         >>> [{"$p": 0.07285448674038553, "field": "product.name", "feature": "pirkka"}]

         Returns an error when make a request to an incorrect path:

         >>> client.request(BaseRequest(method="GET", endpoint="/api/v1/incorrect-path")) # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
         Traceback (most recent call last):
            ...
         aito.client.RequestError: failed to GET(/api/v1/incorrect-path): None: The path you requested [/incorrect-path] does not exist
         """
        try:
            resp = requestslib.request(
                method=request_obj.method,
                url=self.instance_url + request_obj.endpoint,
                headers=self.headers,
                json=request_obj.query
            )
            resp.raise_for_status()
            json_resp = resp.json()
        except Exception as e:
            req_err = RequestError(request_obj, e)
            _raise = raise_for_status if raise_for_status is not None else self.raise_for_status
            if _raise:
                raise req_err
            else:
                return req_err
        return self._get_response(request_obj, json_resp)

    async def async_request(
            self, session: ClientSession, request_obj: BaseRequest, raise_for_status: Optional[bool] = None
    ) -> Union[BaseResponse, RequestError]:
        """execute a request asynchronously using aiohttp ClientSession

        :param session: aiohttp ClientSession for making request
        :type session: ClientSession
        :param request_obj: the request object
        :type request_obj: BaseRequest
        :param raise_for_status: raise :class:`.RequestError` if the request fails.
        If set to None, value from Client will be used. Defaults to True
        :type raise_for_status: bool
        :raises RequestError: an error occurred during the execution of the request and raise_for_status
        """
        LOG.debug(f'async {request_obj}')
        try:
            async with session.request(
                    method=request_obj.method,
                    url=self.instance_url + request_obj.endpoint,
                    json=request_obj.query,
                    headers=self.headers,
                    raise_for_status=True
            ) as resp:
                return self._get_response(request_obj, await resp.json())
        except Exception as e:
            req_err = RequestError(request_obj, e)
            _raise = raise_for_status if raise_for_status is not None else self.raise_for_status
            if _raise:
                raise req_err
            else:
                return req_err

    async def bounded_async_request(self, semaphore: asyncio.Semaphore, **kwargs) -> Union[BaseResponse, RequestError]:
        """bounded concurrent requests with asyncio semaphore

        :param semaphore: asyncio Semaphore
        :type semaphore: asyncio.Semaphore
        :param kwargs: :func:`.async_request` keyword arguments
        :return: tuple of request status code and request json content
        :rtype: Tuple[int, Union[Dict, List]]
        """
        async with semaphore:
            return await self.async_request(**kwargs)

    def async_requests(
            self,
            methods: List[str],
            endpoints: List[str],
            queries: List[Union[List, Dict]],
            batch_size: int = 10
    ) -> List[BaseResponse]:
        """
        .. deprecated:: 0.4.0

        Use :func:`batch_requests` instead
        """
        warnings.warn(
            'The AitoClient.async_requests function is deprecated and will be removed '
            'in a future version. Use AitoClient.batch_requests instead',
            category=FutureWarning
        )
        requests = [BaseRequest(method, endpoints[idx], queries[idx]) for idx, method in enumerate(methods)]
        return self.batch_requests(requests=requests, max_concurrent_requests=batch_size)

    def batch_requests(
            self,
            requests: List[BaseRequest],
            max_concurrent_requests: int = 10
    ) -> List[BaseResponse]:
        """execute a batch of requests asynchronously

        This method is useful when sending a batch of requests, for example, when sending a batch of predict requests.

        :param requests: list of request objects
        :type requests: List[BaseRequest]
        :param max_concurrent_requests: the number of queries to be sent per batch
        :type max_concurrent_requests: int
        :return: list of request response or exception if a request did not succeed
        :rtype: List[Dict]

        Find products that multiple users would most likely buy

        >>> users = ['veronica', 'larry', 'alice']
        >>> responses = client.batch_requests([
        ...     MatchRequest(
        ...         query = {
        ...             'from': 'impressions',
        ...             'where': { 'session.user': usr },
        ...             'match': 'product'
        ...         }
        ...     )
        ...     for usr in users
        ... ])
        >>> # Print top product for each customer
        >>> for idx, usr in enumerate(users):
        ...     print(f"{usr}: {responses[idx].top_match}") # doctest: +NORMALIZE_WHITESPACE +REPORT_UDIFF
        veronica: {'$p': 0.14496525949529243, 'category': '100', 'id': '6410405060457', 'name': 'Pirkka bio cherry tomatoes 250g international 1st class', 'price': 1.29, 'tags': 'fresh vegetable pirkka tomato'}
        larry: {'$p': 0.2348757987154449, 'category': '104', 'id': '6410405216120', 'name': 'Pirkka lactose-free semi-skimmed milk drink 1l', 'price': 1.25, 'tags': 'lactose-free drink pirkka'}
        alice: {'$p': 0.11144746333281082, 'category': '104', 'id': '6408430000258', 'name': 'Valio eila™ Lactose-free semi-skimmed milk drink 1l', 'price': 1.95, 'tags': 'lactose-free drink'}

        """
        async def run():
            async with ClientSession() as session:
                tasks = [
                    self.bounded_async_request(semaphore, session=session, request_obj=req, raise_for_status=False)
                    for req in requests
                ]
                return await asyncio.gather(*tasks)

        semaphore = asyncio.Semaphore(max_concurrent_requests)
        loop = asyncio.get_event_loop()
        responses = loop.run_until_complete(run())
        return responses
