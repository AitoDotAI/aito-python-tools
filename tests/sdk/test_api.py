import json
import os
import shutil
import unittest
from pathlib import Path
from uuid import uuid4

import ndjson
from parameterized import parameterized

import aito.api as api
from aito.schema import AitoDatabaseSchema, AitoTableSchema, AitoColumnTypeSchema, AitoIntType
from aito.utils._file_utils import read_ndjson_gz_file
from tests.cases import CompareTestCase
from tests.sdk.contexts import default_client, grocery_demo_client, endpoint_methods_test_context


class _TestAPIContext(CompareTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.client = default_client()
        cls.default_table_name = f"invoice_{str(uuid4()).replace('-', '_')}"
        cls.input_folder = cls.input_folder.parent.parent / 'sample_invoice'
        with (cls.input_folder / 'invoice_aito_schema.json').open() as f:
            cls.default_table_schema = json.load(f)

    def delete_default_table_and_check(self):
        api.delete_table(self.client, self.default_table_name)
        self.assertFalse(api.check_table_exists(self.client, self.default_table_name))

    def create_default_table_and_check(self):
        api.create_table(self.client, self.default_table_name, self.default_table_schema)
        self.assertTrue(api.check_table_exists(self.client, self.default_table_name))

    def get_default_table_schema_and_check(self):
        instance_tbl_schema = api.get_table_schema(self.client, self.default_table_name)
        self.assertEqual(AitoTableSchema.from_deserialized_object(self.default_table_schema), instance_tbl_schema)


class TestAPI(_TestAPIContext):
    def upload_by_batch_step(self, start, end):
        entries = [{'id': idx, 'name': 'some_name', 'amount': idx} for idx in range(start, end)]
        api.upload_entries(
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
        api.upload_entries(
            self.client,
            table_name=self.default_table_name,
            entries=entries,
            batch_size=2,
            optimize_on_finished=False)

    def query_table_entries_step(self):
        entries = api.query_entries(self.client, self.default_table_name, 2, 2)
        self.assertEqual(entries, [
            {'id': 2, 'name': 'some_name', 'amount': 2},
            {'id': 3, 'name': 'some_name', 'amount': 3}
        ])

    def query_table_all_entries_step(self, expected_result):
        entries = api.query_entries(self.client, self.default_table_name)
        self.assertEqual(len(entries), expected_result)

    def job_query_step(self):
        resp = api.job_request(
            self.client,
            '/api/v1/jobs/_query',
            {'from': self.default_table_name, 'offset': 0, 'limit': 1}
        )
        self.assertEqual(
            resp.json,
            {'offset': 0, 'total': 8, 'hits': [{'id': 0, 'name': 'some_name', 'amount': 0}]}
        )

    def upload_more_and_optimize_step(self, start, end):
        entries = [{'id': idx, 'name': 'some_name', 'amount': idx} for idx in range(start, end)]
        api.upload_entries(
            self.client,
            table_name=self.default_table_name,
            entries=entries,
            batch_size=1,
            optimize_on_finished=False)
        api.optimize_table(self.client, self.default_table_name)

    def get_all_table_entries_step(self, start, end):
        entries = api.query_all_entries(self.client, self.default_table_name)
        self.assertEqual(entries, [{'id': idx, 'name': 'some_name', 'amount': idx} for idx in range(start, end)])

    def download_table_step(self, start, end):
        api.download_table(self.client, self.default_table_name, self.output_folder)
        self.addCleanup((self.output_folder / f'{self.default_table_name}.ndjson').unlink)
        with (self.output_folder / f'{self.default_table_name}.ndjson').open() as f:
            entries = ndjson.load(f)
        self.assertEqual(entries,  [{'id': idx, 'name': 'some_name', 'amount': idx} for idx in range(start, end)])

    def download_table_gzipped_step(self, start, end):
        api.download_table(
            self.client, self.default_table_name, self.output_folder, file_name='invoices', gzip_output=True
        )
        self.addCleanup((self.output_folder / 'invoices.ndjson.gz').unlink)
        entries = read_ndjson_gz_file(self.output_folder / 'invoices.ndjson.gz')
        self.assertEqual(entries,  [{'id': idx, 'name': 'some_name', 'amount': idx} for idx in range(start, end)])

    def test_functions(self):
        self.create_default_table_and_check()
        self.query_table_all_entries_step(expected_result=0)
        self.upload_by_batch_step(start=0, end=4)
        self.query_table_all_entries_step(expected_result=4)
        self.upload_by_batch_step_generator(start=4, end=8)
        self.query_table_all_entries_step(expected_result=8)
        self.query_table_entries_step()
        self.job_query_step()
        self.job_query_from_request_obj_step()
        self.upload_more_and_optimize_step(start=8, end=12)
        self.get_all_table_entries_step(start=0, end=12)
        self.download_table_step(start=0, end=12)
        self.download_table_gzipped_step(start=0, end=12)
        self.addCleanup(self.delete_default_table_and_check)


class TestAlterSchemaAPI(_TestAPIContext):
    @unittest.skipUnless(
        os.environ.get('RUN_ALTER_INSTANCE_DB_TESTS'),
        "Avoid altering the instance DB when running other tests"
    )
    def test_create_and_delete_database(self):
        self.addCleanup(self.delete_default_table_and_check)

        api.delete_database(client=self.client)
        self.assertEqual(len(api.get_existing_tables(client=self.client)), 0)

        db_schema = {'schema': {self.default_table_name: self.default_table_schema}}
        api.create_database(client=self.client, schema=db_schema)

        instance_db_schema = api.get_database_schema(client=self.client)
        self.assertEqual(AitoDatabaseSchema.from_deserialized_object(db_schema), instance_db_schema)
        self.assertEqual(len(api.get_existing_tables(client=self.client)), 1)
        self.assertTrue(api.check_table_exists(self.client, self.default_table_name))

        api.delete_database(client=self.client)
        self.assertEqual(len(api.get_existing_tables(client=self.client)), 0)

    def test_create_and_delete_table(self):
        self.addCleanup(self.delete_default_table_and_check)
        self.create_default_table_and_check()
        self.get_default_table_schema_and_check()
        self.delete_default_table_and_check()

    def test_alter_table(self):
        self.create_default_table_and_check()
        copy_table_name = f'{self.default_table_name}_copy'
        api.copy_table(self.client, self.default_table_name, copy_table_name)
        db_tables = api.get_existing_tables(self.client)
        self.assertIn(self.default_table_name, db_tables)
        self.assertIn(copy_table_name, db_tables)
        rename_table_name = f'{self.default_table_name}_rename'
        api.rename_table(self.client, copy_table_name, rename_table_name)
        db_tables = api.get_existing_tables(self.client)
        self.assertIn(self.default_table_name, db_tables)
        self.assertIn(rename_table_name, db_tables)
        self.assertNotIn(copy_table_name, db_tables)

        def clean_up():
            api.delete_table(self.client, self.default_table_name)
            api.delete_table(self.client, copy_table_name)
            api.delete_table(self.client, rename_table_name)

        self.addCleanup(clean_up)

    def test_create_and_delete_column(self):
        self.addCleanup(self.delete_default_table_and_check)
        self.create_default_table_and_check()
        api.create_column(
            client=self.client, table_name=self.default_table_name, column_name='new_col', schema={'type': 'Int'}
        )

        updated_tbl_schema = AitoTableSchema.from_deserialized_object(self.default_table_schema)
        new_col_schema = AitoColumnTypeSchema(data_type=AitoIntType())
        updated_tbl_schema['new_col'] = new_col_schema

        instance_tbl_schema = api.get_table_schema(client=self.client, table_name=self.default_table_name)
        self.assertEqual(updated_tbl_schema, instance_tbl_schema)

        instance_col_schema = api.get_column_schema(
            client=self.client, table_name=self.default_table_name, column_name='new_col'
        )
        self.assertEqual(new_col_schema, instance_col_schema)

        api.delete_column(client=self.client, table_name=self.default_table_name, column_name='new_col')
        self.get_default_table_schema_and_check()


class TestAlterTableEntries(_TestAPIContext):
    def test_upload_and_delete_entries(self):
        self.addCleanup(self.delete_default_table_and_check)

        self.create_default_table_and_check()
        entries = [{'id': idx, 'name': 'some_name', 'amount': idx} for idx in range(10)]
        api.upload_entries(client=self.client, table_name=self.default_table_name, entries=entries)

        instance_entries = api.query_entries(self.client, self.default_table_name)
        self.assertEqual(instance_entries, entries)

        api.delete_entries(client=self.client, query={'from': self.default_table_name, 'where': {'id': {'$gte': 5}}})
        instance_entries = api.query_entries(self.client, self.default_table_name)
        self.assertEqual(instance_entries, entries[:5])


class TestQuickAddTableAPI(CompareTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.client = default_client()
        cls.default_table_name = f"invoice_{str(uuid4()).replace('-', '_')}"
        cls.input_folder = cls.input_folder.parent.parent / 'sample_invoice'

    def setUp(self):
        super().setUp()

        def _default_clean_up():
            try:
                api.delete_table(self.client, self.default_table_name)
            except Exception as e:
                self.logger.error(f"failed to delete table in cleanup: {e}")
        self.addCleanup(_default_clean_up)

    def compare_table_entries_to_file_content(self, table_name: str, exp_file_path: Path, compare_order: bool = False):
        table_entries = api.query_entries(self.client, table_name)
        with exp_file_path.open() as exp_f:
            file_content = json.load(exp_f)
        if compare_order:
            self.assertEqual(table_entries, file_content)
        else:
            self.assertCountEqual(table_entries, file_content)

    def test_quick_add_table(self):
        input_file = self.input_folder / f'{self.default_table_name}.csv'
        self.addCleanup(input_file.unlink)
        shutil.copyfile(self.input_folder / 'invoice.csv', input_file)
        api.quick_add_table(client=self.client, input_file=input_file)

        self.compare_table_entries_to_file_content(
            self.default_table_name, self.input_folder / 'invoice_no_null_value.json')

    def test_quick_add_table_different_name_different_format(self):
        input_file = self.input_folder / 'invoice.txt'
        self.addCleanup(input_file.unlink)
        shutil.copyfile(self.input_folder / 'invoice.json', input_file)
        with self.assertRaises(ValueError):
            api.quick_add_table(client=self.client, input_file=input_file)
        api.quick_add_table(
            client=self.client, input_file=input_file, table_name=self.default_table_name, input_format='json'
        )
        self.compare_table_entries_to_file_content(
            self.default_table_name, self.input_folder / 'invoice_no_null_value.json'
        )


class TestAPIGroceryCase(CompareTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.client = grocery_demo_client()

    @parameterized.expand([
        ('same_table', 'products', 'tags', ['category', 'id', 'name', 'price'], None),
        ('linked_table', 'impressions', 'product.tags', ['session', 'purchase'], None),
        ('invalid_predicting_field', 'products', 'description', None, ValueError),
        ('invalid_linked_column', 'impressions', 'products.tags', None, ValueError),
        ('invalid_linked_column_name', 'impressions', 'product.description', None, ValueError),
    ])
    def test_quick_predict_and_evaluate(
            self, _, from_table, predicting_field, expected_hypothesis_fields, error
    ):
        if error:
            with self.assertRaises(error):
                api.quick_predict_and_evaluate(self.client, from_table, predicting_field)
        else:
            predict_query, evaluate_query = api.quick_predict_and_evaluate(self.client, from_table, predicting_field)
            self.assertCountEqual(list(predict_query['where'].keys()), expected_hypothesis_fields)

    @parameterized.expand(endpoint_methods_test_context)
    def test_endpoint_method(self, endpoint, request_cls, query, response_cls):
        method = getattr(api, endpoint)
        resp = method(self.client, query)
        self.assertTrue(isinstance(resp, response_cls))
