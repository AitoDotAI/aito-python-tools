import json
import os
from uuid import uuid4

from aito.utils.aito_client import AitoClient, BaseError, RequestError
from aito.utils.file_utils import read_ndjson_gz_file
from tests.cases import TestCaseCompare


class TestAitoClient(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass(test_path='utils/aito_client')
        env_var = os.environ
        cls.client = AitoClient(env_var['AITO_INSTANCE_URL'], env_var['AITO_API_KEY'])
        cls.default_table_name = f"invoice_{uuid4()}"
        cls.input_folder = cls.input_folder.parent.parent / 'sample_invoice'
        with (cls.input_folder / 'invoice_aito_schema.json').open() as f:
            cls.default_schema = json.load(f)

    def test_error_instance_url(self):
        with self.assertRaises(BaseError):
            AitoClient("dont_stop_me_now", "key")

    def test_error_api_key_keys(self):
        with self.assertRaises(BaseError):
            AitoClient(os.environ['AITO_INSTANCE_URL'], "under_pressure")

    def test_error_endpoint(self):
        with self.assertRaises(BaseError):
            self.client.request('GET', 'api/v1/schema')

    def test_error_query(self):
        with self.assertRaises(RequestError):
            self.client.request('POST', '/api/v1/_query', {"from": "catch_me_if_you_can"})

    def delete_table_step(self):
        self.client.delete_table(self.default_table_name)

    def create_table_step(self):
        self.client.put_table_schema(self.default_table_name, self.default_schema)

    def get_table_schema_step(self):
        tbl_schema = self.client.get_table_schema(self.default_table_name)
        self.assertDictEqual(tbl_schema, self.default_schema)

    def upload_by_batch_step(self):
        entries = [{'id': idx, 'name': 'some_name', 'amount': idx} for idx in range(4)]
        self.client.populate_table_entries_by_batches(self.default_table_name, entries, 2, False)

    def query_table_entries_step(self):
        entries = self.client.query_table_entries(self.default_table_name, 2, 2)['hits']
        self.assertEqual(entries, [
            {'id': 2, 'name': 'some_name', 'amount': 2},
            {'id': 3, 'name': 'some_name', 'amount': 3}
        ])

    def async_query_step(self):
        queries = [{'from': self.default_table_name, 'offset': 1, 'limit': 1}] * 2
        responses = self.client.async_requests(['POST'] * 2, ['/api/v1/_query'] * 2, queries)
        self.assertEqual(
            responses,
            [{'offset': 1, 'total': 4, 'hits': [{'id': 1, 'name': 'some_name', 'amount': 1}]}] * 2
        )

    def bounded_async_query_step(self):
        queries = [{'from': self.default_table_name, 'offset': 1, 'limit': 1}] * 2
        responses = self.client.async_requests(['POST'] * 2, ['/api/v1/_query'] * 2, queries, 1)
        self.assertEqual(
            responses,
            [{'offset': 1, 'total': 4, 'hits': [{'id': 1, 'name': 'some_name', 'amount': 1}]}] * 2
        )

    def async_error_query_step(self):
        queries = [{'from': self.default_table_name, 'offset': 0, 'limit': 1}, {'from': 'bohemian'}]
        responses = self.client.async_requests(['POST'] * 2, ['/api/v1/_query'] * 2, queries)
        self.assertEqual(
            responses[0],
            {'offset': 0, 'total': 4, 'hits': [{'id': 0, 'name': 'some_name', 'amount': 0}]}
        )
        self.assertTrue(isinstance(responses[1], RequestError))

    def job_query_step(self):
        resp = self.client.job_request(
            '/api/v1/jobs/_query', {'from': self.default_table_name, 'offset': 0, 'limit': 1})
        self.assertEqual(
            resp,
            {'offset': 0, 'total': 4, 'hits': [{'id': 0, 'name': 'some_name', 'amount': 0}]}
        )

    def optimize_step(self):
        entries = [{'id': idx, 'name': 'some_name', 'amount': idx} for idx in range(4, 8)]
        self.client.populate_table_entries_by_batches(self.default_table_name, entries, 1, False)
        self.client.optimize_table(self.default_table_name)

    def get_all_table_entries_step(self):
        entries = self.client.query_table_all_entries(self.default_table_name)
        self.assertEqual(entries, [{'id': idx, 'name': 'some_name', 'amount': idx} for idx in range(0, 8)])

    def download_table_step(self):
        self.client.download_table(self.default_table_name, self.output_folder)
        import ndjson
        with (self.output_folder / f'{self.default_table_name}.ndjson').open() as f:
            entries = ndjson.load(f)
        self.assertEqual(entries,  [{'id': idx, 'name': 'some_name', 'amount': idx} for idx in range(0, 8)])

    def download_table_gzipped_step(self):
        self.client.download_table(self.default_table_name, self.output_folder, file_name='invoices', gzip_output=True)
        entries = read_ndjson_gz_file(self.output_folder / 'invoices.ndjson.gz')
        self.assertEqual(entries,  [{'id': idx, 'name': 'some_name', 'amount': idx} for idx in range(0, 8)])

    def test_functions(self):
        self.create_table_step()
        self.upload_by_batch_step()
        self.query_table_entries_step()
        self.async_query_step()
        self.bounded_async_query_step()
        self.async_error_query_step()
        self.job_query_step()
        self.optimize_step()
        self.get_all_table_entries_step()
        self.download_table_step()
        self.download_table_gzipped_step()
        self.delete_table_step()

