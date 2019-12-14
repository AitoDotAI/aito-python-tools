from tests.test_case import TestCaseCompare
from aito.utils.aito_client import AitoClient, ClientError
import os
import json


class TestAitoClient(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass(test_path='utils/aito_client')
        cls.input_folder = cls.input_folder.parent.parent / 'sample_invoice'
        env_var = os.environ
        cls.client = AitoClient(env_var['AITO_INSTANCE_NAME'], env_var['API_KEY'])

        with (cls.input_folder / 'invoice_aito_schema.json').open() as f:
            cls.sample_schema = json.load(f)

    def test_no_instance_url(self):
        with self.assertRaises(ClientError):
            AitoClient("", "key")

    def test_error_instance_name(self):
        with self.assertRaises(ClientError):
            AitoClient("1broccoli", "key")

    def test_missing_both_keys(self):
        with self.assertRaises(ClientError):
            AitoClient("broccoli", "")

    def test_error_endpoint(self):
        with self.assertRaises(ClientError):
            self.client.request('GET', 'api/v1/schema')

    def test_error_query(self):
        with self.assertRaises(ClientError):
            self.client.request('POST', '/api/v1/_query', {"from": "catch_me_if_you_can"})