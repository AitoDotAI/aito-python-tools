import requests
from parameterized import parameterized_class

from tests.cases import CompareTestCase
from tests.sdk.contexts import grocery_demo_client


def get_requests_resp_and_aito_resp(aito_client, endpoint, query):
    """returns the json content from requests lib response and aito response for comparison"""
    raw_resp_obj = requests.post(
        url=f'{aito_client.instance_url}/api/v1/_{endpoint}',
        headers=aito_client.headers,
        json=query
    )
    raw_resp_json = raw_resp_obj.json()

    aito_method = getattr(aito_client, endpoint)
    aito_resp = aito_method(query)
    return raw_resp_json, aito_resp


class TestBaseHitsResponse(CompareTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.client = grocery_demo_client()
        cls.endpoint = 'query'
        cls.query = {'from': 'users', 'limit': 3}
        cls.raw_resp_json, cls.aito_resp = get_requests_resp_and_aito_resp(cls.client, cls.endpoint, cls.query)

    def test_attributes(self):
        for attr in ['offset', 'total']:
            self.assertEqual(getattr(self.aito_resp, attr), self.raw_resp_json[attr])
        for idx, hit in enumerate(self.aito_resp.hits):
            self.assertEqual(hit.json, self.raw_resp_json['hits'][idx])
        self.assertEqual(self.aito_resp.first_hit.json, self.raw_resp_json['hits'][0])

    def test_get_field(self):
        self.assertIn('offset', self.aito_resp)
        with self.assertRaises(KeyError):
            _ = self.aito_resp['some_field']

    def test_iter_fields(self):
        aito_res_fields = [field for field in self.aito_resp]
        json_res_fields = list(self.raw_resp_json.keys())
        self.assertCountEqual(aito_res_fields, json_res_fields)


@parameterized_class(("endpoint", "query", "score_field"), [
    ('predict', {"from": "products", "predict": "tags", "limit": 3}, "$p"),
    (
        'recommend',
        {"from": "impressions", "recommend": "product", "goal": {"session.user": "veronica"}, "limit": 3},
        "$p"
    ),
    ('match', {"from": "impressions", "where": {"session.user": "veronica"}, "match": "product", "limit": 3}, "$p"),
    ('similarity', {"from": "products", "similarity": {"name": "rye bread"}}, "$score")
])
class TestScoredHitsResponse(CompareTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.client = grocery_demo_client()

    def test_hit_class(self):
        raw_resp_json, aito_resp = get_requests_resp_and_aito_resp(self.client, self.endpoint, self.query)
        self.assertEqual(aito_resp.first_hit.score, raw_resp_json['hits'][0][self.score_field])
        with self.assertRaises(KeyError):
            _ = aito_resp.first_hit.explanation

    def test_hit_with_explanation(self):
        query_with_explanation = self.query
        query_with_explanation["select"] = ["$why"]
        raw_resp_json, aito_resp = get_requests_resp_and_aito_resp(self.client, self.endpoint, query_with_explanation)

        self.assertEqual(aito_resp.first_hit.explanation, raw_resp_json['hits'][0]['$why'])


class TestRelateResponse(CompareTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.client = grocery_demo_client()

    def test_relate_response(self):
        relate_query = {"from": "products", "where": {"$exists": "name"}, "relate": "tags", "limit": 2}
        raw_resp_json, aito_resp = get_requests_resp_and_aito_resp(self.client, 'relate', relate_query)

        self.assertEqual(aito_resp.relations[0].json, raw_resp_json['hits'][0])
        self.assertEqual(aito_resp.relations[0].frequencies, raw_resp_json['hits'][0]['fs'])
        self.assertEqual(aito_resp.relations[0].probabilities, raw_resp_json['hits'][0]['ps'])


class TestEvaluateResponse(CompareTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.client = grocery_demo_client()

    def test_relate_response(self):
        evaluate_query = {
            "test": {"$index": {"$mod": [10, 0]}},
            "evaluate": {
                "from": "products",
                "where": {"name": {"$get": "name"}},
                "match": "tags"
            }
        }
        raw_resp_json, aito_resp = get_requests_resp_and_aito_resp(self.client, 'evaluate', evaluate_query)

        self.assertEqual(aito_resp.accuracy, raw_resp_json['accuracy'])
        self.assertEqual(aito_resp.test_sample_count, raw_resp_json['testSamples'])
        self.assertEqual(aito_resp.train_sample_count, raw_resp_json['trainSamples'])
