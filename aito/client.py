"""A versatile client that connects to an Aito Database Instance

"""

import asyncio
import logging
import warnings
from typing import Dict, List, Union, Tuple, Optional

import requests as requestslib
from aiohttp import ClientSession, ClientResponseError

from .client_request import BaseRequest

LOG = logging.getLogger('AitoClient')


class BaseError(Exception):
    """An error occurred when using the client. Log the error message and the stack trace

    """
    def __init__(self, message: str):
        """

        :param message: the error message
        :type message: str
        """
        LOG.error(message)
        LOG.debug(message, stack_info=True)
        self.message = message

    def __str__(self):
        return self.message


class RequestError(BaseError):
    """An error occurred when sending a request to the Aito instance

    """
    def __init__(self, request: BaseRequest, error: Exception):
        """

        :param request: the request object
        :type request: BaseRequest
        :param error: the error
        :type error: Exception
        """
        if isinstance(error, requestslib.HTTPError):
            resp = error.response.json()
            error_msg = resp['message'] if 'message' in resp else resp
        elif isinstance(error, ClientResponseError):
            error_msg = error.message
        else:
            error_msg = str(error)
        super().__init__(f'failed to {request}: {error_msg}')


class AitoClient:
    """A versatile client that connects to the Aito Database Instance

    """
    def __init__(self, instance_url: str, api_key: str, check_credentials: Optional[bool] = True):
        """

        :param instance_url: Aito instance url
        :type instance_url: str
        :param api_key: Aito instance API key
        :type api_key: str
        :param check_credentials: check the given credentials by requesting the Aito instance version, defaults to True
        :type check_credentials: Optional[bool]
        :raises BaseError: an error occurred during the creation of AitoClient

        >>> aito_client = AitoClient(your_instance_url, your_api_key) # doctest: +SKIP
        >>> # Change the API key to READ-WRITE or READ-ONLY
        >>> aito_client.api_key = new_api_key # doctest: +SKIP
        """
        self.instance_url = instance_url.strip("/")
        self.api_key = api_key
        if check_credentials:
            try:
                self.request(BaseRequest('GET', '/version'))
            except Exception:
                raise BaseError('failed to instantiate Aito Client, please check your credentials')

    @property
    def headers(self):
        """

        :return: the headers that will be used to send a request to the Aito instance
        :rtype: Dict
        """
        return {'Content-Type': 'application/json', 'x-api-key': self.api_key}

    def request(self, request: BaseRequest) -> Dict:
        """make a request to an Aito API endpoint
        The client returns a JSON response if the request succeed and a :class:`.RequestError` if the request failed

        :param request: request object
        :type request: BaseRequest
        :raises RequestError: an error occurred during the execution of the request
        :return: request JSON content
        :rtype: Dict

        Simple request to get the schema of a table:

        >>> res = client.request(BaseRequest(method="GET", endpoint="/api/v1/schema/impressions"))
        >>> pprint(res) # doctest: +NORMALIZE_WHITESPACE
         {'columns': {'product': {'link': 'products.id',
                                 'nullable': False,
                                 'type': 'String'},
                     'purchase': {'nullable': False, 'type': 'Boolean'},
                     'session': {'link': 'sessions.id',
                                 'nullable': False,
                                 'type': 'String'}},
         'type': 'table'}

         Sends a `PREDICT <https://aito.ai/docs/api/#post-api-v1-predict>`__ query:

         >>> client.request(
         ...    BaseRequest(
         ...        method="POST",
         ...        endpoint="/api/v1/_predict",
         ...        query={
         ...            "from": "impressions",
         ...            "where": { "session": "veronica" },
         ...            "predict": "product.name",
         ...            "limit": 1
         ...        }
         ...    )
         ... ) # doctest: +NORMALIZE_WHITESPACE
         {'offset': 0, 'total': 142, 'hits': [{'$p': 0.07285448674038553, 'field': 'product.name', 'feature': 'pirkka'}]}

         Returns an error when make a request to an incorrect path:

         >>> client.request(BaseRequest(method="GET", endpoint="/api/v1/incorrect-path")) # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
         Traceback (most recent call last):
            ...
         aito.client.RequestError: failed to GET(/api/v1/incorrect-path): None: The path you requested [/incorrect-path] does not exist
         """
        try:
            resp = requestslib.request(
                method=request.method,
                url=self.instance_url + request.endpoint,
                headers=self.headers,
                json=request.query
            )
            resp.raise_for_status()
            json_resp = resp.json()
        except Exception as e:
            raise RequestError(request, e)
        return json_resp

    async def async_request(
            self, session: ClientSession, request: BaseRequest
    ) -> Tuple[int, Union[Dict, List]]:
        """execute a request asynchronously using aiohttp ClientSession

        :param session: aiohttp ClientSession for making request
        :type session: ClientSession
        :param request: the request object
        :type request: BaseRequest
        """
        LOG.debug(f'async {request}')
        async with session.request(
                method=request.method,
                url=self.instance_url + request.endpoint,
                json=request.query,
                headers=self.headers) as resp:
            return resp.status, await resp.json()

    async def bounded_async_request(self, semaphore: asyncio.Semaphore, *args) -> Tuple[int, Union[Dict, List]]:
        """bounded concurrent requests with asyncio semaphore

        :param semaphore: asyncio Semaphore
        :type semaphore: asyncio.Semaphore
        :param args: :func:`.async_request` arguments
        :return: tuple of request status code and request json content
        :rtype: Tuple[int, Union[Dict, List]]
        """
        async with semaphore:
            return await self.async_request(*args)

    def async_requests(
            self,
            methods: List[str],
            endpoints: List[str],
            queries: List[Union[List, Dict]],
            batch_size: int = 10
    ) -> List[Dict]:
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
    ) -> List[Dict]:
        """execute a batch of requests asynchronously

        This method is useful when sending a batch of requests, for example, when sending a batch of predict requests.

        :param requests: list of request object
        :type requests: List[BaseRequest]
        :param max_concurrent_requests: the number of queries to be sent per batch
        :type max_concurrent_requests: int
        :return: list of request response or exception if a request did not succeed
        :rtype: List[Dict]

        Find products that multiple users would most likely buy

        >>> users = ['veronica', 'larry', 'alice']
        >>> responses = client.batch_requests([
        ...     BaseRequest(
        ...         method='POST',
        ...         endpoint='/api/v1/_match',
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
        ...     print(f"{usr}: {responses[idx]['hits'][0]}") # doctest: +NORMALIZE_WHITESPACE +REPORT_UDIFF
        veronica: {'$p': 0.14496525949529243, 'category': '100', 'id': '6410405060457', 'name': 'Pirkka bio cherry tomatoes 250g international 1st class', 'price': 1.29, 'tags': 'fresh vegetable pirkka tomato'}
        larry: {'$p': 0.2348757987154449, 'category': '104', 'id': '6410405216120', 'name': 'Pirkka lactose-free semi-skimmed milk drink 1l', 'price': 1.25, 'tags': 'lactose-free drink pirkka'}
        alice: {'$p': 0.11144746333281082, 'category': '104', 'id': '6408430000258', 'name': 'Valio eila™ Lactose-free semi-skimmed milk drink 1l', 'price': 1.95, 'tags': 'lactose-free drink'}

        """
        async def run():
            async with ClientSession() as session:
                tasks = [self.bounded_async_request(semaphore, session, req) for req in requests]
                return await asyncio.gather(*tasks, return_exceptions=True)

        semaphore = asyncio.Semaphore(max_concurrent_requests)
        loop = asyncio.get_event_loop()
        responses = loop.run_until_complete(run())
        for idx, resp in enumerate(responses):
            req = requests[idx]
            if isinstance(resp, Exception):
                responses[idx] = RequestError(req, resp)
            # manually handling raise for status since aiohttp doesnt return response content with raise for status
            elif resp[0] >= 400:
                responses[idx] = RequestError(req, resp[1])
            else:
                responses[idx] = resp[1]
        return responses
