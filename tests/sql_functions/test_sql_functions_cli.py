import json
import os
import subprocess

from aito.utils.aito_client import AitoClient
from tests.test_case import TestCaseCompare
from uuid import uuid4


class TestSQLCliFunctions(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass(test_path='sql_functions/cli')
        cls.input_folder = cls.input_folder.parent.parent / 'sample_invoice'
        env_vars = os.environ
        cls.client = AitoClient(env_vars['AITO_INSTANCE_NAME'], env_vars['AITO_API_KEY'])
        cls.default_table_name = f"invoice_{uuid4()}"
        cls.prefix_args = ['python', '-m', 'aito.cli.main_parser_wrapper']
        if os.getenv('TEST_BUILT_PACKAGE'):
            cls.prefix_args = ['aito']
        cls.driver_name = os.getenv('TEST_SQL_DB_DRIVER')
        if not cls.driver_name:
            raise ValueError("Missing SQL DB driver name")

    def setUp(self):
        super().setUp()
        self.client.delete_table(self.default_table_name)


class TestPostgresCliFunctions(TestSQLCliFunctions):
    def create_table(self):
        with (self.input_folder / "invoice_aito_schema.json").open() as f:
            table_schema = json.load(f)
        self.client.put_table_schema(self.default_table_name, table_schema)

    def test_infer_schema_from_query(self):
        with self.out_file_path.open('w') as out_f:
            subprocess.run(
                self.prefix_args + ['infer-table-schema', 'from-sql', f'{self.driver_name}', 'SELECT * FROM invoice'],
                stdout=out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_aito_schema.json')

    def test_upload_data_from_query(self):
        self.create_table()
        subprocess.run(self.prefix_args + ['database', 'upload-data-from-sql', f'{self.driver_name}',
                                           self.default_table_name, 'SELECT * FROM invoice'])
        result_table_entries = self.client.query_table_entries(self.default_table_name)
        with (self.input_folder / 'invoice_no_null_value.json').open() as exp_f:
            self.assertCountEqual(result_table_entries['hits'], json.load(exp_f))

    def test_upload_data_from_query_table_not_existed(self):
        with self.assertRaises(subprocess.CalledProcessError):
            subprocess.check_call(self.prefix_args + ['database', 'upload-data-from-sql', f'{self.driver_name}',
                                                      self.default_table_name, 'SELECT * FROM invoice'])

    def test_quick_add_table_from_query(self):
        subprocess.run(self.prefix_args + ['database', 'quick-add-table-from-sql', f'{self.driver_name}',
                                           self.default_table_name, 'SELECT * FROM invoice'])
        result_table_entries = self.client.query_table_entries(self.default_table_name)
        with (self.input_folder / 'invoice_no_null_value.json').open() as exp_f:
            self.assertCountEqual(result_table_entries['hits'], json.load(exp_f))


class TestMySQLCliFunctions(TestSQLCliFunctions):
    def create_table(self):
        with (self.input_folder / "invoice_aito_schema_lower_case_columns.json").open() as f:
            table_schema = json.load(f)
        self.client.put_table_schema(self.default_table_name, table_schema)

    def test_infer_schema_from_query(self):
        with self.out_file_path.open('w') as out_f:
            subprocess.run(self.prefix_args + ['infer-table-schema', 'from-sql', f'{self.driver_name}',
                                               'SELECT * FROM invoice'],
                           stdout=out_f)
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_aito_schema_lower_case_columns.json')

    def test_upload_data_from_query(self):
        self.create_table()
        subprocess.run(self.prefix_args + ['database', 'upload-data-from-sql', f'{self.driver_name}',
                                           self.default_table_name, 'SELECT * FROM invoice'])
        result_table_entries = self.client.query_table_entries(self.default_table_name)
        with (self.input_folder / 'invoice_no_null_value_lower_case_columns.json').open() as exp_f:
            self.assertCountEqual(result_table_entries['hits'], json.load(exp_f))

    def test_upload_data_from_query_table_not_existed(self):
        with self.assertRaises(subprocess.CalledProcessError):
            subprocess.check_call(self.prefix_args + ['database', 'upload-data-from-sql', f'{self.driver_name}',
                                                      self.default_table_name, 'SELECT * FROM invoice'])

    def test_quick_add_table_from_query(self):
        subprocess.run(self.prefix_args + ['database', 'quick-add-table-from-sql', f'{self.driver_name}',
                                           self.default_table_name, 'SELECT * FROM invoice'])
        result_table_entries = self.client.query_table_entries(self.default_table_name)
        with (self.input_folder / 'invoice_no_null_value_lower_case_columns.json').open() as exp_f:
            self.assertCountEqual(result_table_entries['hits'], json.load(exp_f))

