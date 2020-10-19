import asyncio
import os

from aiohttp import ClientSession
from parameterized import parameterized

from aito.client import AitoClient, Error, RequestError
from aito.client.requests import BaseRequest, GenericQueryRequest
from aito.client.responses import BaseResponse, HitsResponse
from tests.cases import CompareTestCase
from tests.sdk.contexts import default_client, grocery_demo_client, endpoint_methods_test_context


class TestAitoClient(CompareTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.client = default_client()

    def test_erroneous_instance_url(self):
        with self.assertRaises(Error):
            AitoClient("dont_stop_me_now", "key")

    def test_erroneous_api_key(self):
        with self.assertRaises(Error):
            AitoClient(os.environ['AITO_INSTANCE_URL'], "under_pressure")

    def test_request_with_request_obj_kwarg(self):
        self.client.request(request_obj=BaseRequest('GET', '/api/v1/schema'))

    def test_request_with_method_and_endpoint_kwarg(self):
        self.client.request(method='GET', endpoint='/api/v1/schema')

    def test_erroneous_query(self):
        with self.assertRaises(RequestError):
            self.client.request(request_obj=BaseRequest('POST', '/api/v1/_query', {"from": "catch_me_if_you_can"}))


class TestAitoClientGroceryCase(CompareTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.client = grocery_demo_client()
        cls.loop = asyncio.new_event_loop()

    @parameterized.expand(endpoint_methods_test_context)
    def test_request(self, endpoint, request_cls, query, response_cls):
        async def test_async_request():
            async with ClientSession() as session:
                a_resp = await self.client.async_request(session, request_obj=request_cls(query))
                self.assertTrue(isinstance(a_resp, response_cls))

        self.logger.debug('test request method')
        req = request_cls(query)
        resp = self.client.request(request_obj=req)
        self.assertTrue(isinstance(resp, response_cls))

        self.logger.debug('test async request')
        self.loop.run_until_complete(test_async_request())

    @parameterized.expand([
        ('default_max_concurrent', 10),
        ('limited_max_concurrent', 1)
    ])
    def test_batch_queries(self, _, max_concurrent):
        queries = [{'from': 'users', 'limit': 1}] * 3
        responses = self.client.batch_requests(
            [BaseRequest('POST', '/api/v1/_query', query) for query in queries],
            max_concurrent_requests=max_concurrent
        )
        self.assertTrue(all([isinstance(res, BaseResponse) for res in responses]))
        self.assertEqual(
            [res.json for res in responses],
            [{'offset': 0, 'total': 3, 'hits': [{'username': 'veronica'}]}] * 3
        )

    def test_batch_queries_erroneous_query(self):
        queries = [{'from': 'users', 'limit': 1}, {'from': 'bohemian'}]
        responses = self.client.batch_requests([
            GenericQueryRequest(query)
            for query in queries
        ])
        self.assertTrue(isinstance(responses[0], HitsResponse))
        self.assertEqual(
            responses[0].json,
            {'offset': 0, 'total': 3, 'hits': [{'username': 'veronica'}]}
        )
        self.assertTrue(isinstance(responses[1], RequestError))
