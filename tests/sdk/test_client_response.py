import requests
from parameterized import parameterized, parameterized_class

import aito.client.requests as aito_requests
import aito.schema as aito_schema
from aito.client import AitoClient
from tests.cases import CompareTestCase
from tests.sdk.contexts import grocery_demo_client


def get_requests_resp_and_aito_resp(aito_client: AitoClient, request_obj: aito_requests.AitoRequest):
    """returns the json content from requests lib response and aito response for comparison"""
    raw_resp_obj = requests.request(
        method=request_obj.method,
        url=aito_client.instance_url + request_obj.endpoint,
        headers=aito_client.headers,
        json=request_obj.query
    )
    raw_resp_json = raw_resp_obj.json()

    aito_resp = aito_client.request(request_obj=request_obj)
    return raw_resp_json, aito_resp


class TestBaseHitsResponse(CompareTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.client = grocery_demo_client()
        cls.request_obj = aito_requests.GenericQueryRequest(query={'from': 'users', 'limit': 3})
        cls.raw_resp_json, cls.aito_resp = get_requests_resp_and_aito_resp(cls.client, cls.request_obj)

    def test_attributes(self):
        for attr in ['offset', 'total']:
            self.assertEqual(getattr(self.aito_resp, attr), self.raw_resp_json[attr])
        self.assertTrue(hasattr(self.aito_resp, 'hits'))
        for idx, hit in enumerate(self.aito_resp.hits):
            self.assertEqual(hit.json, self.raw_resp_json['hits'][idx])
        self.assertTrue(hasattr(self.aito_resp, 'first_hit'))
        self.assertEqual(self.aito_resp.first_hit.json, self.raw_resp_json['hits'][0])

    def test_get_field(self):
        self.assertIn('offset', self.aito_resp)
        with self.assertRaises(KeyError):
            _ = self.aito_resp['some_field']

    def test_iter_fields(self):
        aito_res_fields = [field for field in self.aito_resp]
        json_res_fields = list(self.raw_resp_json.keys())
        self.assertCountEqual(aito_res_fields, json_res_fields)


@parameterized_class(("test_name", "request_obj", "score_field"), [
    ("predict", aito_requests.PredictRequest({"from": "products", "predict": "tags", "limit": 3}), "$p"),
    ("recommend", aito_requests.RecommendRequest(
        {"from": "impressions", "recommend": "product", "goal": {"session.user": "veronica"}, "limit": 3}
    ), "$p" ),
    ("match", aito_requests.MatchRequest(
        {"from": "impressions", "where": {"session.user": "veronica"}, "match": "product", "limit": 3}
    ), "$p"),
    ("similarity", aito_requests.SimilarityRequest({"from": "products", "similarity": {"name": "rye bread"}}), "$score")
])
class TestScoredHitsResponse(CompareTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.client = grocery_demo_client()

    def test_hit_class(self):
        raw_resp_json, aito_resp = get_requests_resp_and_aito_resp(self.client, self.request_obj)
        self.assertTrue(hasattr(aito_resp, 'first_hit'))
        self.assertEqual(aito_resp.first_hit.score, raw_resp_json['hits'][0][self.score_field])
        with self.assertRaises(KeyError):
            _ = aito_resp.first_hit.explanation

    def test_hit_with_explanation(self):
        self.request_obj.query = {**self.request_obj.query, 'select': ['$why']}
        raw_resp_json, aito_resp = get_requests_resp_and_aito_resp(self.client, self.request_obj)
        self.assertEqual(aito_resp.first_hit.explanation, raw_resp_json['hits'][0]['$why'])


class TestRelateResponse(CompareTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.client = grocery_demo_client()

    def test_relate_response(self):
        raw_resp_json, aito_resp = get_requests_resp_and_aito_resp(
            self.client,
            aito_requests.RelateRequest({"from": "products", "where": {"$exists": "name"}, "relate": "tags", "limit": 2})
        )

        self.assertEqual(aito_resp.relations[0].json, raw_resp_json['hits'][0])
        self.assertEqual(aito_resp.relations[0].frequencies, raw_resp_json['hits'][0]['fs'])
        self.assertEqual(aito_resp.relations[0].probabilities, raw_resp_json['hits'][0]['ps'])


class TestEvaluateResponse(CompareTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.client = grocery_demo_client()

    def test_relate_response(self):
        raw_resp_json, aito_resp = get_requests_resp_and_aito_resp(
            self.client,
            aito_requests.EvaluateRequest({
                "test": {"$index": {"$mod": [10, 0]}},
                "evaluate": {
                    "from": "products",
                    "where": {"name": {"$get": "name"}},
                    "match": "tags"
                }
            })
        )

        self.assertEqual(aito_resp.accuracy, raw_resp_json['accuracy'])
        self.assertEqual(aito_resp.test_sample_count, raw_resp_json['testSamples'])
        self.assertEqual(aito_resp.train_sample_count, raw_resp_json['trainSamples'])


class TestGetSchemaResponse(CompareTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.client = grocery_demo_client()

    @parameterized.expand([
        ('get_database_schema', aito_requests.GetDatabaseSchemaRequest(), aito_schema.AitoDatabaseSchema),
        ('get_table_schema', aito_requests.GetTableSchemaRequest(table_name='products'), aito_schema.AitoTableSchema),
        (
                'get_column_schema',
                aito_requests.GetColumnSchemaRequest(table_name='products', column_name='name'),
                aito_schema.AitoColumnTypeSchema
        )
    ])
    def test_get_schema_response(self, _, request_instance, schema_cls):
        raw_resp_json, aito_resp = get_requests_resp_and_aito_resp(self.client, request_instance)
        self.assertEqual(aito_resp.schema, schema_cls.from_deserialized_object(raw_resp_json))
