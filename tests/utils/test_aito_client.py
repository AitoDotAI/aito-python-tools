from tests.test_case import TestCaseCompare
from aito.utils.aito_client import AitoClient, ClientError
import os
import json


class TestAitoClient(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass(test_path='utils/aito_client')
        cls.input_folder = cls.input_folder.parent.parent / 'schema'
        env_var = os.environ
        cls.client = AitoClient(env_var['AITO_INSTANCE_URL'], env_var['AITO_RW_KEY'], env_var['AITO_RO_KEY'])

        with (cls.input_folder / 'sample_schema.json').open() as f:
            cls.sample_schema = json.load(f)

    def test_no_instance_url(self):
        with self.assertRaises(ClientError):
            AitoClient("", "rwkey", "rokey")

    def test_error_instance_url(self):
        with self.assertRaises(ClientError):
            AitoClient("https://broccoli.api.aito", "rwkey", "rokey")

    def test_missing_both_keys(self):
        with self.assertRaises(ClientError):
            AitoClient("https://broccoli.api.aito.ai", "", "")

    def test_error_endpoint(self):
        with self.assertRaises(ClientError):
            self.client.request('GET', 'api/v1/schema')

    def test_error_query(self):
        with self.assertRaises(ClientError):
            self.client.request('POST', '/api/v1/_query', {"from": "catch_me_if_you_can"})