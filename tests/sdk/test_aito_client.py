import json
import os
from uuid import uuid4

from parameterized import parameterized

from aito.client_request import BaseRequest
from aito.client import AitoClient, Error, RequestError
from tests.cases import CompareTestCase


class TestAitoClient(CompareTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env_var = os.environ
        cls.client = AitoClient(env_var['AITO_INSTANCE_URL'], env_var['AITO_API_KEY'])

    def test_erroneous_instance_url(self):
        with self.assertRaises(Error):
            AitoClient("dont_stop_me_now", "key")

    def test_erroneous_api_key(self):
        with self.assertRaises(Error):
            AitoClient(os.environ['AITO_INSTANCE_URL'], "under_pressure")

    def test_erroneous_query(self):
        with self.assertRaises(RequestError):
            self.client.request(BaseRequest('POST', '/api/v1/_query', {"from": "catch_me_if_you_can"}))


class TestAitoClientGroceryCase(CompareTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env_var = os.environ
        cls.client = AitoClient(env_var['AITO_GROCERY_DEMO_INSTANCE_URL'], env_var['AITO_GROCERY_DEMO_API_KEY'])

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
        self.assertTrue(all([isinstance(res, HitsResponse) for res in responses]))
        self.assertEqual(
            [res.response for res in responses],
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
            responses[0].response,
            {'offset': 0, 'total': 3, 'hits': [{'username': 'veronica'}]}
        )
        self.assertTrue(isinstance(responses[1], RequestError))
