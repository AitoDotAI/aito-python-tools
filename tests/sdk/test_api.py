import json
import os
from uuid import uuid4

import ndjson
from parameterized import parameterized

from aito.api import delete_table, check_table_exists, create_table, get_table_schema, upload_entries, query_entries, \
    query_all_entries, job_request, optimize_table, download_table, copy_table, get_existing_tables, rename_table, \
    get_database_schema, naive_predict
from aito.client import AitoClient
from aito.utils._file_utils import read_ndjson_gz_file
from tests.cases import CompareTestCase


class TestAPI(CompareTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env_var = os.environ
        cls.client = AitoClient(env_var['AITO_INSTANCE_URL'], env_var['AITO_API_KEY'])
        cls.default_table_name = f"invoice_{str(uuid4()).replace('-', '_')}"
        cls.input_folder = cls.input_folder.parent.parent / 'sample_invoice'
        with (cls.input_folder / 'invoice_aito_schema.json').open() as f:
            cls.default_schema = json.load(f)

    def delete_table_step(self):
        delete_table(self.client, self.default_table_name)
        self.assertEqual(check_table_exists(self.client, self.default_table_name), False)

    def create_table_step(self):
        create_table(self.client, self.default_table_name, self.default_schema)
        self.assertEqual(check_table_exists(self.client, self.default_table_name), True)

    def get_table_schema_step(self):
        tbl_schema = get_table_schema(self.client, self.default_table_name)
        self.assertDictEqual(tbl_schema.to_json_serializable(), self.default_schema)

    def upload_by_batch_step(self, start, end):
        entries = [{'id': idx, 'name': 'some_name', 'amount': idx} for idx in range(start, end)]
        upload_entries(
            self.client,
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
        upload_entries(
            self.client,
            table_name=self.default_table_name,
            entries=entries,
            batch_size=2,
            optimize_on_finished=False)

    def query_table_entries_step(self):
        entries = query_entries(self.client, self.default_table_name, 2, 2)
        self.assertEqual(entries, [
            {'id': 2, 'name': 'some_name', 'amount': 2},
            {'id': 3, 'name': 'some_name', 'amount': 3}
        ])

    def query_table_all_entries_step(self, expected_result):
        entries = query_entries(self.client, self.default_table_name)
        self.assertEqual(len(entries), expected_result)

    def job_query_step(self):
        resp = job_request(
            self.client,
            '/api/v1/jobs/_query',
            {'from': self.default_table_name, 'offset': 0, 'limit': 1}
        )
        self.assertEqual(
            resp,
            {'offset': 0, 'total': 8, 'hits': [{'id': 0, 'name': 'some_name', 'amount': 0}]}
        )

    def upload_more_and_optimize_step(self, start, end):
        entries = [{'id': idx, 'name': 'some_name', 'amount': idx} for idx in range(start, end)]
        upload_entries(
            self.client,
            table_name=self.default_table_name,
            entries=entries,
            batch_size=1,
            optimize_on_finished=False)
        optimize_table(self.client, self.default_table_name)

    def get_all_table_entries_step(self, start, end):
        entries = query_all_entries(self.client, self.default_table_name)
        self.assertEqual(entries, [{'id': idx, 'name': 'some_name', 'amount': idx} for idx in range(start, end)])

    def download_table_step(self, start, end):
        download_table(self.client, self.default_table_name, self.output_folder)
        self.addCleanup((self.output_folder / f'{self.default_table_name}.ndjson').unlink)
        with (self.output_folder / f'{self.default_table_name}.ndjson').open() as f:
            entries = ndjson.load(f)
        self.assertEqual(entries,  [{'id': idx, 'name': 'some_name', 'amount': idx} for idx in range(start, end)])

    def download_table_gzipped_step(self, start, end):
        download_table(self.client, self.default_table_name, self.output_folder, file_name='invoices', gzip_output=True)
        self.addCleanup((self.output_folder / 'invoices.ndjson.gz').unlink)
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
        self.job_query_step()
        self.upload_more_and_optimize_step(start=8, end=12)
        self.get_all_table_entries_step(start=0, end=12)
        self.download_table_step(start=0, end=12)
        self.download_table_gzipped_step(start=0, end=12)
        self.addCleanup(self.delete_table_step)

    def test_alter_table(self):
        self.create_table_step()
        copy_table_name = f'{self.default_table_name}_copy'
        copy_table(self.client, self.default_table_name, copy_table_name)
        db_tables = get_existing_tables(self.client)
        self.assertIn(self.default_table_name, db_tables)
        self.assertIn(copy_table_name, db_tables)
        rename_table_name = f'{self.default_table_name}_rename'
        rename_table(self.client, copy_table_name, rename_table_name)
        db_tables = get_existing_tables(self.client)
        self.assertIn(self.default_table_name, db_tables)
        self.assertIn(rename_table_name, db_tables)
        self.assertNotIn(copy_table_name, db_tables)

        def clean_up():
            delete_table(self.client, self.default_table_name)
            delete_table(self.client, copy_table_name)
            delete_table(self.client, rename_table_name)

        self.addCleanup(clean_up)


class TestAPIGroceryCase(CompareTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        env_var = os.environ
        cls.client = AitoClient(env_var['AITO_GROCERY_DEMO_INSTANCE_URL'], env_var['AITO_GROCERY_DEMO_API_KEY'])
        cls.database_schema = get_database_schema(cls.client)

    @parameterized.expand([
        ('same_table', 'tags', 'products', True, ['category', 'id', 'name', 'price'], None),
        ('linked_table', 'product.tags', 'impressions', False, ['session', 'purchase'], None),
        ('invalid_predicting_field', 'description', 'products', False, None, ValueError),
        ('invalid_linked_column', 'products.tags', 'impressions', False, None, ValueError),
        ('invalid_linked_column_name', 'product.description', 'impressions', False, None, ValueError),
    ])
    def test_naive_predict(
            self, _, predicting_field, from_table, fetch_schema, expected_hypothesis_fields, error
    ):
        use_database_schema = None if fetch_schema else self.database_schema
        if error:
            with self.assertRaises(error):
                naive_predict(self.client, predicting_field, from_table, use_database_schema=use_database_schema)
        else:
            predict_query, result, actual_result = naive_predict(self.client, predicting_field, from_table)
            self.assertCountEqual(list(predict_query['where'].keys()), expected_hypothesis_fields)
