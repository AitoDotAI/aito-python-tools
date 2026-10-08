"""A versatile client that makes requests to an Aito Database Instance

`aiohttp` is imported inside the functions that use it rather than at module
scope. It is needed only for the asynchronous path, and this module is
reachable from ``import aito.v2`` -- a synchronous client that does not
use it.
"""

import asyncio
import logging
import time
import warnings
from typing import Dict, List, Union, Tuple, Optional, TYPE_CHECKING

import requests as requestslib

if TYPE_CHECKING:  # pragma: no cover - import-time typing only
    from aiohttp import ClientSession

from aito import _write_contention as contention
from aito.exceptions import BaseError
from aito.utils._generic_utils import instance_url_problem
from .requests import AitoRequest, BaseRequest, GetVersionRequest
from .responses import BaseResponse

LOG = logging.getLogger('AitoClient')


class Error(BaseError):
    """An error occurred when using the client

    """
    def __init__(self, message: str):
        super().__init__(message, LOG)


def _is_aiohttp_response_error(error: Exception) -> bool:
    """whether `error` is aiohttp's ClientResponseError, importing aiohttp lazily

    aiohttp is only needed for the async path. Importing it at module scope made
    it load for every caller of this package -- including
    ``import aito.v2``, which is synchronous and does not use it. If
    aiohttp is not installed at all, an error plainly cannot be one of its.
    """
    try:
        from aiohttp import ClientResponseError
    except ImportError:  # pragma: no cover - aiohttp is a declared dependency
        return False
    return isinstance(error, ClientResponseError)


class RequestError(Error):
    """An error occurred when sending a request to the Aito instance

    """
    def __init__(self, request_obj: AitoRequest, error: Exception, retries: int = 0):
        """

        :param request_obj: the request object
        :type request_obj: AitoRequest
        :param error: the error
        :type error: Exception
        :param retries: how many times the request was repeated before this error (a 409
            ``write.contention`` is retried), defaults to 0
        :type retries: int
        """
        self.request_obj = request_obj
        self.error = error
        if isinstance(error, requestslib.HTTPError):
            try:
                resp = error.response.json()
                error_msg = resp['message'] if 'message' in resp else resp
            except (ValueError, KeyError):
                # Response is not valid JSON or doesn't have expected structure
                error_msg = error.response.text or str(error)
        elif _is_aiohttp_response_error(error):
            error_msg = error.message
        else:
            error_msg = str(error)
        self.retries = retries
        suffix = f' (retried {retries} times)' if retries else ''
        super().__init__(f'failed to {request_obj}: {error_msg}{suffix}')


class AitoClient:
    """A versatile client that connects to the Aito Database Instance

    """

    # Pattern to detect multitenant URLs: /db/{database_name}
    _MULTITENANT_PATH_PREFIX = '/db/'

    @property
    def is_multitenant(self) -> bool:
        """Check if the client is connected to a multitenant instance.

        Multitenant URLs have the format: https://shared.aito.ai/db/{database_name}

        :return: True if connected to a multitenant instance
        :rtype: bool
        """
        from urllib.parse import urlparse
        parsed = urlparse(self.instance_url)
        return self._MULTITENANT_PATH_PREFIX in parsed.path

    def __init__(
            self,
            instance_url: str,
            api_key: str,
            check_credentials: bool = True,
            raise_for_status: bool = True,
            write_contention_retries: int = contention.DEFAULT_RETRIES
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
        :param write_contention_retries: how many times to repeat a request the server answered with 409
            ``write.contention``: a write that lost to concurrent writes on the same table and committed
            nothing, so repeating it is safe. Each retry waits the server's ``Retry-After`` plus exponential
            jitter (at most 8 s on top). 0 turns it off. Other 409s are never retried. Defaults to 3
        :type write_contention_retries: int
        :raises BaseError: an error occurred during the creation of AitoClient
        :raises ValueError: write_contention_retries is negative

        >>> aito_client = AitoClient(your_instance_url, your_api_key) # doctest: +SKIP
        >>> # Change the API key to READ-WRITE or READ-ONLY
        >>> aito_client.api_key = new_api_key # doctest: +SKIP
        """
        # Before the credential check, whose catch-all would hide the fix
        url_problem = instance_url_problem(instance_url)
        if url_problem:
            raise Error(url_problem)
        self.instance_url = instance_url.strip("/")
        self.api_key = api_key
        self.raise_for_status = raise_for_status
        self.write_contention_retries = contention.check_retries(write_contention_retries)
        self.instance_version = None
        if check_credentials:
            try:
                version_resp = self._request_version()
                self.instance_version = version_resp.version
                # Also verify API key is valid by making an authenticated request
                self._verify_api_key()
            except Exception:
                raise Error('failed to instantiate Aito Client, please check your credentials')

    @property
    def _base_url(self) -> str:
        """Extract the base URL for endpoints that don't include the database path.

        For multitenant URLs like 'https://shared.aito.ai/db/my-database',
        returns 'https://shared.aito.ai'.
        For regular URLs, returns the instance_url unchanged.
        """
        from urllib.parse import urlparse, urlunparse
        parsed = urlparse(self.instance_url)
        path = parsed.path
        if self._MULTITENANT_PATH_PREFIX in path:
            # Strip /db/{database_name} from the path
            db_index = path.find(self._MULTITENANT_PATH_PREFIX)
            base_path = path[:db_index]
            return urlunparse(parsed._replace(path=base_path))
        return self.instance_url

    def _request_version(self):
        """Request the Aito instance version.

        For multitenant deployments, the /version endpoint is at the base URL,
        not under the database path.
        """
        version_url = self._base_url + GetVersionRequest.endpoint
        try:
            resp = requestslib.request(
                method=GetVersionRequest.method,
                url=version_url,
                headers=self.headers,
                json=None
            )
            resp.raise_for_status()
            return GetVersionRequest.response_cls(resp.json())
        except Exception as e:
            raise RequestError(GetVersionRequest(), e)

    def _verify_api_key(self):
        """Verify the API key is valid by making an authenticated request.

        The /version endpoint doesn't require authentication, so we need to
        make a separate request to an authenticated endpoint to verify credentials.
        """
        schema_url = self.instance_url + '/api/v1/schema'
        resp = requestslib.request(
            method='GET',
            url=schema_url,
            headers=self.headers,
            json=None
        )
        resp.raise_for_status()

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
        :type raise_for_status: Optional[bool]
        :raises RequestError: an error occurred during the execution of the request and raise_for_status
        :return: a response object or :class:`.RequestError` if an error occurred and not raise_for_status
        :rtype: Union[BaseResponse, RequestError]

        Simple request to get the schema of a table:

        >>> res = client.request(method="GET", endpoint="/api/v1/schema/impressions")
        >>> print(res.to_json_string(indent=2, sort_keys=True))
        {
          "columns": {
            "context": {
              "link": "contexts.id",
              "nullable": false,
              "type": "String"
            },
            "product": {
              "link": "products.id",
              "nullable": false,
              "type": "String"
            },
            "purchase": {
              "nullable": false,
              "type": "Boolean"
            }
          },
          "type": "table"
        }

         Sends a `PREDICT <https://aito.ai/docs/api/#post-api-v1-predict>`__ query:

         >>> from aito.v1 import PredictRequest
         >>> res = client.request(request_obj=PredictRequest(
         ...    query={
         ...        "from": "impressions",
         ...        "where": { "context": "veronica" },
         ...        "predict": "product.name"
         ...    }
         ... )) # doctest: +NORMALIZE_WHITESPACE
         >>> print(res.top_prediction) # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
         {"$p": ..., "$value": ...}

         Returns an error when make a request to an incorrect path:

         >>> client.request(method="GET", endpoint="api/v1/incorrect-path") # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
         Traceback (most recent call last):
            ...
         ValueError: invalid endpoint 'api/v1/incorrect-path' for BaseRequest
         """
        if request_obj is None:
            if method is not None and endpoint is not None:
                request_obj = AitoRequest.make_request(method, endpoint, query)
            else:
                raise TypeError("'request() requires either 'request_obj' or 'method' and 'endpoint'")
        retries = 0
        while True:
            try:
                resp = requestslib.request(
                    method=request_obj.method,
                    url=self.instance_url + request_obj.endpoint,
                    headers=self.headers,
                    json=request_obj.query
                )
                # the body is only parsed here for a 409, so a normal response is parsed once
                if resp.status_code == 409 and self._is_write_contention(
                        resp.status_code, _json_or_none(resp), retries):
                    delay = contention.delay(resp.headers, retries)
                    retries += 1
                    LOG.info(f'{request_obj}: write.contention, retry {retries} of '
                             f'{self.write_contention_retries} in {delay:.1f}s')
                    time.sleep(delay)
                    continue
                resp.raise_for_status()
                json_resp = resp.json()
            except Exception as e:
                req_err = RequestError(request_obj, e, retries)
                _raise = raise_for_status if raise_for_status is not None else self.raise_for_status
                if _raise:
                    raise req_err
                else:
                    return req_err
            return request_obj.response_cls(json_resp)

    def _is_write_contention(self, status: int, body, retries: int) -> bool:
        """a 409 whose code is write.contention, with retries left (aito-core #1619)

        Keyed on the code: the server's other 409s (an env being migrated, an old binary
        format that needs a repair, a release or branch conflict) are other conditions.
        """
        return (status == 409 and retries < self.write_contention_retries
                and contention.code_of(body) == contention.WRITE_CONTENTION)

    async def async_request(
            self, session: 'ClientSession', *,
            method: Optional[str] = None,
            endpoint: Optional[str] = None,
            query: Optional[Union[Dict, List]] = None,
            request_obj: Optional[AitoRequest] = None,
            raise_for_status: Optional[bool] = None
    ) -> Union[BaseResponse, RequestError]:
        """execute a request asynchronously using aiohttp ClientSession

        :param session: aiohttp ClientSession for making request
        :type session: ClientSession
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
        :type raise_for_status: Optional[bool]
        :raises RequestError: an error occurred during the execution of the request and raise_for_status
        """
        LOG.debug(f'async {request_obj}')
        if request_obj is None:
            if method is not None and endpoint is not None:
                request_obj = AitoRequest.make_request(method, endpoint, query)
            else:
                raise TypeError("request() requires either 'request_obj' or 'method' and 'endpoint'")
        retries = 0
        while True:
            delay = None
            try:
                async with session.request(
                        method=request_obj.method,
                        url=self.instance_url + request_obj.endpoint,
                        json=request_obj.query,
                        headers=self.headers,
                        raise_for_status=False
                ) as resp:
                    if resp.status == 409:
                        try:
                            body = await resp.json(content_type=None)
                        except Exception:
                            body = None
                        if self._is_write_contention(resp.status, body, retries):
                            delay = contention.delay(resp.headers, retries)
                    if delay is None:
                        resp.raise_for_status()
                        return request_obj.response_cls(await resp.json())
            except Exception as e:
                req_err = RequestError(request_obj, e, retries)
                _raise = raise_for_status if raise_for_status is not None else self.raise_for_status
                if _raise:
                    raise req_err
                else:
                    return req_err
            retries += 1
            LOG.info(f'async {request_obj}: write.contention, retry {retries} of '
                     f'{self.write_contention_retries} in {delay:.1f}s')
            await asyncio.sleep(delay)

    async def bounded_async_request(
            self, semaphore: asyncio.Semaphore, *args, **kwargs
    ) -> Union[BaseResponse, RequestError]:
        """bounded concurrent requests with asyncio semaphore

        :param semaphore: asyncio Semaphore
        :type semaphore: asyncio.Semaphore
        :param args: :func:`.async_request` positional arguments
        :param kwargs: :func:`.async_request` keyword arguments
        :return: tuple of request status code and request json content
        :rtype: Tuple[int, Union[Dict, List]]
        """
        async with semaphore:
            return await self.async_request(*args, **kwargs)

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
    ) -> List[Union[BaseResponse, RequestError]]:
        """execute a batch of requests asynchronously

        This method is useful when sending a batch of requests, for example, when sending a batch of predict requests.

        :param requests: list of request objects
        :type requests: List[AitoRequest]
        :param max_concurrent_requests: the number of queries to be sent per batch
        :type max_concurrent_requests: int
        :return: list of request response or exception if a request did not succeed
        :rtype: List[Union[BaseResponse, RequestError]]

        Find products that multiple users would most likely buy

        >>> from aito.v1 import MatchRequest
        >>> users = ['veronica', 'larry', 'alice']
        >>> responses = client.batch_requests([
        ...     MatchRequest(
        ...         query = {
        ...             'from': 'impressions',
        ...             'where': { 'context.user': usr },
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
            from aiohttp import ClientSession  # deferred: see the module docstring
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


def _json_or_none(resp) -> Optional[Union[Dict, List]]:
    """the parsed JSON body, or None when there is none"""
    try:
        return resp.json()
    except ValueError:
        return None
