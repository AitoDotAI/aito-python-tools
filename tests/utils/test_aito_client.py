import os

from aito.utils.aito_client import AitoClient, AitoClientError, AitoClientRequestError
from tests.cases import TestCaseCompare


class TestAitoClient(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass(test_path='utils/aito_client')
        env_var = os.environ
        cls.client = AitoClient(env_var['AITO_INSTANCE_URL'], env_var['AITO_API_KEY'])

    def test_no_instance_url(self):
        with self.assertRaises(AitoClientError):
            AitoClient("", "key")

    def test_error_instance_name(self):
        with self.assertRaises(AitoClientError):
            AitoClient("1broccoli", "key")

    def test_missing_both_keys(self):
        with self.assertRaises(AitoClientError):
            AitoClient("broccoli", "")

    def test_error_endpoint(self):
        with self.assertRaises(AitoClientRequestError):
            self.client.request('GET', 'api/v1/schema')

    def test_error_query(self):
        with self.assertRaises(AitoClientRequestError):
            self.client.request('POST', '/api/v1/_query', {"from": "catch_me_if_you_can"})