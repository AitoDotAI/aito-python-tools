from tests.cases import CompareTestCase
from aito.client_request import BaseRequest


class TestAitoClient(CompareTestCase):
    def test_erroneous_method(self):
        with self.assertRaises(ValueError):
            BaseRequest('PATCH', '/api/v1/schema')

    def test_erroneous_endpoint(self):
        with self.assertRaises(ValueError):
            BaseRequest('GET', 'api/v1/schema')
