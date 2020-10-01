from tests.cases import CompareTestCase
from aito.client_request import BaseRequest, PredictRequest, SearchRequest, RecommendRequest, EvaluateRequest, \
    SimilarityRequest, RelateRequest, GenericQueryRequest, AitoRequest
from parameterized import parameterized


class TestClientRequest(CompareTestCase):
    @parameterized.expand([
        ('search', 'POST', '/api/v1/_search', {}, SearchRequest, None),
        ('predict', 'POST', '/api/v1/_predict', {}, PredictRequest, None),
        ('recommend', 'POST', '/api/v1/_recommend', {}, RecommendRequest, None),
        ('evaluate', 'POST', '/api/v1/_evaluate', {}, EvaluateRequest, None),
        ('similarity', 'POST', '/api/v1/_similarity', {}, SimilarityRequest, None),
        ('relate', 'POST', '/api/v1/_relate', {}, RelateRequest, None),
        ('query', 'POST', '/api/v1/_query', {}, GenericQueryRequest, None),
        ('schema', 'POST', '/api/v1/schema', {}, BaseRequest, None),
        ('erroneous_method', 'PATCH', '/api/v1/schema', {}, None, ValueError),
        ('erroneous_endpoint', 'GET', 'api/v1/schema', {}, None, ValueError)
    ])
    def test_make_request(self, _, method, endpoint, query, request_cls, error):
        if error:
            with self.assertRaises(error):
                AitoRequest.make_request(method=method, endpoint=endpoint, query=query)
        else:
            req = AitoRequest.make_request(method=method, endpoint=endpoint, query=query)
            self.assertTrue(isinstance(req, request_cls))

    def test_base_request_erroneous_method(self):
        with self.assertRaises(ValueError):
            BaseRequest('PATCH', '/api/v1/schema')

    def test_base_request_erroneous_endpoint(self):
        with self.assertRaises(ValueError):
            BaseRequest('GET', 'api/v1/schema')
