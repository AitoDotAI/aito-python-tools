import asyncio
import os

from aiohttp import ClientSession
from parameterized import parameterized

from aito.client import AitoClient, Error, RequestError
from aito.client_request import BaseRequest, SearchRequest, PredictRequest, RecommendRequest, EvaluateRequest, \
    SimilarityRequest, MatchRequest, RelateRequest, GenericQueryRequest
from aito.client_response import SearchResponse, PredictResponse, RecommendResponse, EvaluateResponse, \
    SimilarityResponse, MatchResponse, RelateResponse, HitsResponse, BaseResponse
from tests.cases import CompareTestCase
from tests.sdk.contexts import default_client, grocery_demo_client


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

    @parameterized.expand([
        ('search', SearchRequest, {"from": "users"}, SearchResponse),
        (
                'predict',
                PredictRequest,
                {"from": "products", "where": {"name": "Pirkka banana"}, "predict": "tags"},
                PredictResponse
        ),
        (
                'recommend',
                RecommendRequest,
                {"from": "impressions", "recommend": "product", "goal": {"session.user": "veronica"}},
                RecommendResponse
        ),
        (
                'evaluate',
                EvaluateRequest,
                {
                    "test": {"$index": {"$mod": [10, 0]}},
                    "evaluate": {
                        "from": "products",
                        "where": {"name": {"$get": "name"}},
                        "match": "tags"
                    }
                },
                EvaluateResponse
        ),
        (
                'similarity',
                SimilarityRequest,
                {"from": "products", "similarity": {"name": "rye bread"}},
                SimilarityResponse
        ),
        (
                'match',
                MatchRequest,
                {"from": "impressions", "where": {"session.user": "veronica"}, "match": "product"},
                MatchResponse
        ),
        ('relate', RelateRequest, {"from": "products", "where": {"$exists": "name"}, "relate": "tags"}, RelateResponse),
        (
                'query',
                GenericQueryRequest,
                {"from": "products", "where": {"name": "Pirkka banana"}, "get": "tags", "orderBy": "$p"},
                HitsResponse
        ),
    ])
    def test_request(self, endpoint, request_cls, query, response_cls):
        async def test_async_request():
            async with ClientSession() as session:
                a_resp = await self.client.async_request(session, request_cls(query))
                self.assertTrue(isinstance(a_resp, response_cls))

        self.logger.debug('test request method')
        req = request_cls(query)
        resp = self.client.request(request_obj=req)
        self.assertTrue(isinstance(resp, response_cls))

        self.logger.debug('test endpoint method')
        method = self.client.__getattribute__(endpoint)
        resp = method(query)
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
