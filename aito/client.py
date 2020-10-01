"""A versatile client that makes requests to an Aito Database Instance

"""

import asyncio
import logging
import warnings
from typing import Dict, List, Union, Tuple, Optional

import requests as requestslib
from aiohttp import ClientSession, ClientResponseError

from .client_request import BaseRequest, SearchRequest, PredictRequest, RecommendRequest, EvaluateRequest, \
    SimilarityRequest, MatchRequest, RelateRequest, GenericQueryRequest, AitoRequest
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
    def __init__(self, request_obj: AitoRequest, error: Exception):
        """

        :param request_obj: the request object
        :type request_obj: AitoRequest
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
                self.request(method='GET', endpoint='/version')
            except Exception:
                raise Error('failed to instantiate Aito Client, please check your credentials')

    @property
    def headers(self):
        """ the headers that will be used to send a request to the Aito instance

        :rtype: Dict
        """
        return {'Content-Type': 'application/json', 'x-api-key': self.api_key}

    def request(
            self, *,
            method: Optional[str] = None,
            endpoint: Optional[str] = None,
            query: Optional[Union[Dict, List]] = None,
            request_obj: Optional[AitoRequest] = None,
            raise_for_status: Optional[bool] = None
    ) -> Union[BaseResponse, RequestError]:
        # noinspection LongLine
        """make a request to an Aito API endpoint
        The client returns a JSON response if the request succeed and a :class:`.RequestError` if the request fails

        :param method: method for the new :class:`.AitoRequest` object
        :type method: str
        :param endpoint: endpoint for the new :class:`.AitoRequest` object
        :type endpoint: str
        :param query: an Aito query if applicable, defaults to None
        :type query: Optional[Union[Dict, List]]
        :param request_obj: use an :class:`.AitoRequest` object
        :type request_obj: AitoRequest
        :param raise_for_status: raise :class:`.RequestError` if the request fails instead of returning the error
            If set to None, value from Client will be used. Defaults to True
        :type raise_for_status: bool
        :raises RequestError: an error occurred during the execution of the request and raise_for_status
        :return: request JSON content or :class:`.RequestError` if an error occurred and not raise_for_status
        :rtype: Union[BaseResponse, RequestError]

        Simple request to get the schema of a table:

        >>> res = client.request(method="GET", endpoint="/api/v1/schema/impressions")
        >>> print(res.to_json_string(indent=2, sort_keys=True))
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

         >>> res = client.request(request_obj=PredictRequest(
         ...    query={
         ...        "from": "impressions",
         ...        "where": { "session": "veronica" },
         ...        "predict": "product.name"
         ...    }
         ... )) # doctest: +NORMALIZE_WHITESPACE
         >>> print(res.top_prediction) # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
         {"$p": ..., "field": ..., "feature": ...}

         Returns an error when make a request to an incorrect path:

         >>> client.request(method="GET", endpoint="/api/v1/incorrect-path") # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
         Traceback (most recent call last):
            ...
         aito.client.RequestError: failed to GET(/api/v1/incorrect-path): None: The path you requested [/incorrect-path] does not exist
         """
        if request_obj is None:
            if method is not None and endpoint is not None:
                request_obj = AitoRequest.make_request(method, endpoint, query)
            else:
                raise TypeError("'request() requires either 'request_obj' or 'method' and 'endpoint'")
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
        return request_obj.response_cls(json_resp)

    async def async_request(
            self, session: ClientSession, request_obj: AitoRequest, raise_for_status: Optional[bool] = None
    ) -> Union[BaseResponse, RequestError]:
        """execute a request asynchronously using aiohttp ClientSession

        :param session: aiohttp ClientSession for making request
        :type session: ClientSession
        :param request_obj: the request object
        :type request_obj: AitoRequest
        :param raise_for_status: raise :class:`.RequestError` if the request fails instead of returning the error
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
                return request_obj.response_cls(await resp.json())
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
            requests: List[AitoRequest],
            max_concurrent_requests: int = 10
    ) -> List[BaseResponse]:
        """execute a batch of requests asynchronously

        This method is useful when sending a batch of requests, for example, when sending a batch of predict requests.

        :param requests: list of request objects
        :type requests: List[AitoRequest]
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
        ...     print(f"{usr}: {responses[idx].top_match}") # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
        veronica: {"$p": ..., "category": ..., "id": ..., "name": ..., "price": ..., "tags": ...}
        larry: {"$p": ..., "category": ..., "id": ..., "name": ..., "price": ..., "tags": ...}
        alice: {"$p": ..., "category": ..., "id": ..., "name": ..., "price": ..., "tags": ...}

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

    def search(self, query: Dict, raise_for_status: Optional[bool] = None) -> Union[SearchResponse, RequestError]:
        """send a query to the `Search API <https://aito.ai/docs/api/#post-api-v1-search>`__

        :param query: the search query
        :type query: Dict
        :param raise_for_status: raise :class:`.RequestError` if the request fails instead of returning the error
            If set to None, value from Client will be used. Defaults to None
        :type raise_for_status: Optional[bool]
        :return: request JSON content or :class:`.RequestError` if an error occurred and not raise_for_status
        :rtype: Union[SearchResponse, RequestError]
        """
        req = SearchRequest(query)
        return self.request(request_obj=req, raise_for_status=raise_for_status)

    def predict(self, query: Dict, raise_for_status: Optional[bool] = None) -> Union[PredictResponse, RequestError]:
        """send a query to the `Predict API <https://aito.ai/docs/api/#post-api-v1-predict>`__

        :param query: the predict query
        :type query: Dict
        :param raise_for_status: raise :class:`.RequestError` if the request fails instead of returning the error
            If set to None, value from Client will be used. Defaults to None
        :type raise_for_status: Optional[bool]
        :return: request JSON content or :class:`.RequestError` if an error occurred and not raise_for_status
        :rtype: Union[PredictResponse, RequestError]
        """
        req = PredictRequest(query)
        return self.request(request_obj=req, raise_for_status=raise_for_status)

    def recommend(self, query: Dict, raise_for_status: Optional[bool] = None) -> Union[RecommendResponse, RequestError]:
        """send a query to the `Recommend API <https://aito.ai/docs/api/#post-api-v1-recommend>`__

        :param query: the recommend query
        :type query: Dict
        :param raise_for_status: raise :class:`.RequestError` if the request fails instead of returning the error
            If set to None, value from Client will be used. Defaults to None
        :type raise_for_status: Optional[bool]
        :return: request JSON content or :class:`.RequestError` if an error occurred and not raise_for_status
        :rtype: Union[RecommendResponse, RequestError]
        """
        req = RecommendRequest(query)
        return self.request(request_obj=req, raise_for_status=raise_for_status)

    def evaluate(self, query: Dict, raise_for_status: Optional[bool] = None) -> Union[EvaluateResponse, RequestError]:
        """send a query to the `Evaluate API <https://aito.ai/docs/api/#post-api-v1-evaluate>`__

        :param query: the evaluate query
        :type query: Dict
        :param raise_for_status: raise :class:`.RequestError` if the request fails instead of returning the error
            If set to None, value from Client will be used. Defaults to None
        :type raise_for_status: Optional[bool]
        :return: request JSON content or :class:`.RequestError` if an error occurred and not raise_for_status
        :rtype: Union[EvaluateResponse, RequestError]
        """
        req = EvaluateRequest(query)
        return self.request(request_obj=req, raise_for_status=raise_for_status)

    def similarity(
            self, query: Dict, raise_for_status: Optional[bool] = None
    ) -> Union[SimilarityResponse, RequestError]:
        """send a query to the `Similarity API <https://aito.ai/docs/api/#post-api-v1-similarity>`__

        :param query: the similarity query
        :type query: Dict
        :param raise_for_status: raise :class:`.RequestError` if the request fails instead of returning the error
            If set to None, value from Client will be used. Defaults to None
        :type raise_for_status: Optional[bool]
        :return: request JSON content or :class:`.RequestError` if an error occurred and not raise_for_status
        :rtype: Union[SimilarityResponse, RequestError]
        """
        req = SimilarityRequest(query)
        return self.request(request_obj=req, raise_for_status=raise_for_status)

    def match(self, query: Dict, raise_for_status: Optional[bool] = None) -> Union[MatchResponse, RequestError]:
        """send a query to the `Match API <https://aito.ai/docs/api/#post-api-v1-match>`__

        :param query: the match query
        :type query: Dict
        :param raise_for_status: raise :class:`.RequestError` if the request fails instead of returning the error
            If set to None, value from Client will be used. Defaults to None
        :type raise_for_status: Optional[bool]
        :return: request JSON content or :class:`.RequestError` if an error occurred and not raise_for_status
        :rtype: Union[MatchResponse, RequestError]
        """
        req = MatchRequest(query)
        return self.request(request_obj=req, raise_for_status=raise_for_status)

    def relate(self, query: Dict, raise_for_status: Optional[bool] = None) -> Union[RelateResponse, RequestError]:
        """send a query to the `Relate API <https://aito.ai/docs/api/#post-api-v1-relate>`__

        :param query: the relate query
        :type query: Dict
        :param raise_for_status: raise :class:`.RequestError` if the request fails instead of returning the error
            If set to None, value from Client will be used. Defaults to None
        :type raise_for_status: Optional[bool]
        :return: request JSON content or :class:`.RequestError` if an error occurred and not raise_for_status
        :rtype: Union[RelateResponse, RequestError]
        """
        req = RelateRequest(query)
        return self.request(request_obj=req, raise_for_status=raise_for_status)

    def query(self, query: Dict, raise_for_status: Optional[bool] = None) -> Union[HitsResponse, RequestError]:
        """send a query to the `Generic Query API <https://aito.ai/docs/api/#post-api-v1-qyert>`__

        :param query: the query
        :type query: Dict
        :param raise_for_status: raise :class:`.RequestError` if the request fails instead of returning the error
            If set to None, value from Client will be used. Defaults to None
        :type raise_for_status: Optional[bool]
        :return: request JSON content or :class:`.RequestError` if an error occurred and not raise_for_status
        :rtype: Union[HitsResponse, RequestError]
        """
        req = GenericQueryRequest(query)
        return self.request(request_obj=req, raise_for_status=raise_for_status)
