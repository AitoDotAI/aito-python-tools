import json
import os
from uuid import uuid4

from aito.sdk.aito_client import AitoClient, BaseError, RequestError
from aito.common.file_utils import read_ndjson_gz_file
from aito.sdk.aito_schema import AitoDatabaseSchema
from tests.cases import CompareTestCase
from parameterized import parameterized


class TestAitoClient(CompareTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env_var = os.environ
        cls.client = AitoClient(env_var['AITO_INSTANCE_URL'], env_var['AITO_API_KEY'])
        cls.default_table_name = f"invoice_{str(uuid4()).replace('-', '_')}"
        cls.input_folder = cls.input_folder.parent.parent / 'sample_invoice'
        with (cls.input_folder / 'invoice_aito_schema.json').open() as f:
            cls.default_schema = json.load(f)

    def tearDown(self):
        super().tearDown()
        self.client.delete_table(self.default_table_name)

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
        self.assertEqual(self.client.check_table_exists(self.default_table_name), False)

    def create_table_step(self):
        self.client.create_table(self.default_table_name, self.default_schema)
        self.assertEqual(self.client.check_table_exists(self.default_table_name), True)

    def get_table_schema_step(self):
        tbl_schema = self.client.get_table_schema(self.default_table_name)
        self.assertDictEqual(tbl_schema.to_json_serializable(), self.default_schema)

    def upload_by_batch_step(self, start, end):
        entries = [{'id': idx, 'name': 'some_name', 'amount': idx} for idx in range(start, end)]
        self.client.upload_entries(
            table_name=self.default_table_name,
            entries=entries,
            batch_size=2,
            optimize_on_finished=False)

    @staticmethod
    def generator_for_test(start, end):
        for idx in range(start, end):
            entry = {'id': idx, 'name': 'some_name', 'amount': idx}
            yield entry

    def upload_by_batch_step_generator(self, start, end):
        entries = self.generator_for_test(start, end)
        self.client.upload_entries(
            table_name=self.default_table_name,
            entries=entries,
            batch_size=2,
            optimize_on_finished=False)

    def query_table_entries_step(self):
        entries = self.client.query_entries(self.default_table_name, 2, 2)
        self.assertEqual(entries, [
            {'id': 2, 'name': 'some_name', 'amount': 2},
            {'id': 3, 'name': 'some_name', 'amount': 3}
        ])

    def query_table_all_entries_step(self, expected_result):
        entries = self.client.query_entries(self.default_table_name)
        self.assertEqual(len(entries), expected_result)

    def async_query_step(self):
        queries = [{'from': self.default_table_name, 'offset': 1, 'limit': 1}] * 2
        responses = self.client.async_requests(['POST'] * 2, ['/api/v1/_query'] * 2, queries)
        self.assertEqual(
            responses,
            [{'offset': 1, 'total': 8, 'hits': [{'id': 1, 'name': 'some_name', 'amount': 1}]}] * 2
        )

    def bounded_async_query_step(self):
        queries = [{'from': self.default_table_name, 'offset': 1, 'limit': 1}] * 2
        responses = self.client.async_requests(['POST'] * 2, ['/api/v1/_query'] * 2, queries, 1)
        self.assertEqual(
            responses,
            [{'offset': 1, 'total': 8, 'hits': [{'id': 1, 'name': 'some_name', 'amount': 1}]}] * 2
        )

    def async_error_query_step(self):
        queries = [{'from': self.default_table_name, 'offset': 0, 'limit': 1}, {'from': 'bohemian'}]
        responses = self.client.async_requests(['POST'] * 2, ['/api/v1/_query'] * 2, queries)
        self.assertEqual(
            responses[0],
            {'offset': 0, 'total': 8, 'hits': [{'id': 0, 'name': 'some_name', 'amount': 0}]}
        )
        self.assertTrue(isinstance(responses[1], RequestError))

    def job_query_step(self):
        resp = self.client.job_request(
            '/api/v1/jobs/_query', {'from': self.default_table_name, 'offset': 0, 'limit': 1})
        self.assertEqual(
            resp,
            {'offset': 0, 'total': 8, 'hits': [{'id': 0, 'name': 'some_name', 'amount': 0}]}
        )

    def optimize_step(self, start, end):
        entries = [{'id': idx, 'name': 'some_name', 'amount': idx} for idx in range(start, end)]
        self.client.upload_entries(
            table_name=self.default_table_name,
            entries=entries,
            batch_size=1,
            optimize_on_finished=False)
        self.client.optimize_table(self.default_table_name)

    def get_all_table_entries_step(self, start, end):
        entries = self.client.query_all_entries(self.default_table_name)
        self.assertEqual(entries, [{'id': idx, 'name': 'some_name', 'amount': idx} for idx in range(start, end)])

    def download_table_step(self, start, end):
        self.client.download_table(self.default_table_name, self.output_folder)
        import ndjson
        with (self.output_folder / f'{self.default_table_name}.ndjson').open() as f:
            entries = ndjson.load(f)
        self.assertEqual(entries,  [{'id': idx, 'name': 'some_name', 'amount': idx} for idx in range(start, end)])

    def download_table_gzipped_step(self, start, end):
        self.client.download_table(self.default_table_name, self.output_folder, file_name='invoices', gzip_output=True)
        entries = read_ndjson_gz_file(self.output_folder / 'invoices.ndjson.gz')
        self.assertEqual(entries,  [{'id': idx, 'name': 'some_name', 'amount': idx} for idx in range(start, end)])

    def test_functions(self):
        self.create_table_step()
        self.query_table_all_entries_step(expected_result=0)
        self.upload_by_batch_step(start=0, end=4)
        self.query_table_all_entries_step(expected_result=4)
        self.upload_by_batch_step_generator(start=4, end=8)
        self.query_table_all_entries_step(expected_result=8)
        self.query_table_entries_step()
        self.async_query_step()
        self.bounded_async_query_step()
        self.async_error_query_step()
        self.job_query_step()
        self.optimize_step(start=8, end=12)
        self.get_all_table_entries_step(start=0, end=12)
        self.download_table_step(start=0, end=12)
        self.download_table_gzipped_step(start=0, end=12)
        self.delete_table_step()

    def test_tobe_deprecated_upload(self):
        self.create_table_step()
        self.query_table_all_entries_step(expected_result=0)
        entries = [{'id': idx, 'name': 'some_name', 'amount': idx} for idx in range(0, 4)]
        self.client.upload_entries_by_batches(
            table_name=self.default_table_name,
            entries=entries,
            batch_size=2,
            optimize_on_finished=False)
        self.query_table_all_entries_step(expected_result=4)
        self.delete_table_step()


class TestAitoClientGroceryCase(CompareTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env_var = os.environ
        cls.client = AitoClient(env_var['AITO_GROCERY_DEMO_INSTANCE_URL'], env_var['AITO_GROCERY_DEMO_API_KEY'])
        cls.database_schema = cls.client.get_database_schema()

    @parameterized.expand([
        ('same_table', 'tags', 'products', True, ['category', 'id', 'name', 'price'], None),
        ('linked_table', 'product.tags', 'impressions', False, ['session', 'purchase'], None),
        ('invalid_predicting_field', 'description', 'products', False, None, ValueError),
        ('invalid_linked_column', 'products.tags', 'impressions', False, None, ValueError),
        ('invalid_linked_column_name', 'product.description', 'impressions', False, None, ValueError),
    ])
    def test_naive_predict(
            self, test_name, predicting_field, from_table, fetch_schema, expected_hypothesis_fields, error
    ):
        use_database_schema = None if fetch_schema else self.database_schema
        if error:
            with self.assertRaises(error):
                self.client.naive_predict(predicting_field, from_table, use_database_schema=use_database_schema)
        else:
            predict_query, result, actual_result = self.client.naive_predict(predicting_field, from_table)
            self.assertCountEqual(list(predict_query['where'].keys()), expected_hypothesis_fields)
