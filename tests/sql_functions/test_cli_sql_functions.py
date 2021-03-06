import json
import os
from uuid import uuid4

from aito.api import create_table, delete_table, query_entries
from aito.client import AitoClient, RequestError
from tests.cli.parser_and_cli_test_case import ParserAndCLITestCase


class TestSQLFunctions(ParserAndCLITestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.input_folder = cls.input_folder.parent.parent / 'sample_invoice'
        cls.default_main_parser_args = {
            'verbose': False, 'version': False, 'quiet': False,
            'driver': '.env', 'server': '.env', 'port': '.env', 'database': '.env', 'username': '.env',
            'password': '.env'
        }
        cls.default_client_args = {'profile': 'default', 'api_key': '.env', 'instance_url': '.env'}
        cls.client = AitoClient(os.environ['AITO_INSTANCE_URL'], os.environ['AITO_API_KEY'])
        cls.default_table_name = f"invoice_{str(uuid4()).replace('-', '_')}"

    def tearDown(self):
        super().tearDown()
        delete_table(self.client, self.default_table_name)

    def parser_and_execute_infer_schema_from_query(self):
        expected_args = {
            'command': 'infer-table-schema',
            'input-format': 'from-sql',
            'query': 'SELECT * FROM invoice',
            **self.default_main_parser_args
        }
        with self.out_file_path.open('w') as out_f:
            self.parse_and_execute(
                ['infer-table-schema', 'from-sql', 'SELECT * FROM invoice'],
                expected_args,
                stub_stdout=out_f
            )

    def parse_and_execute_upload_data_from_query(self):
        expected_args = {
            'command': 'upload-data-from-sql',
            'table-name': self.default_table_name,
            'query': 'SELECT * FROM invoice',
            **self.default_main_parser_args,
            **self.default_client_args
        }
        self.parse_and_execute(
            ['upload-data-from-sql', self.default_table_name, 'SELECT * FROM invoice'],
            expected_args
        )

    def parse_and_execute_upload_data_from_query_table_not_exist(self):
        expected_args = {
            'command': 'upload-data-from-sql',
            'table-name': self.default_table_name,
            'query': 'SELECT * FROM invoice',
            **self.default_main_parser_args,
            **self.default_client_args
        }
        self.parse_and_execute(
            ['upload-data-from-sql', self.default_table_name, 'SELECT * FROM invoice'],
            expected_args,
            execute_exception=SystemExit
        )

    def parse_and_execute_quick_add_table(self):
        expected_args = {
            'command': 'quick-add-table-from-sql',
            'table-name': self.default_table_name,
            'query': 'SELECT * FROM invoice',
            **self.default_main_parser_args,
            **self.default_client_args
        }
        self.parse_and_execute(
            ['quick-add-table-from-sql', self.default_table_name, 'SELECT * FROM invoice'],
            expected_args,
        )


class TestPostgresFunctions(TestSQLFunctions):
    def create_table(self):
        with (self.input_folder / "invoice_aito_schema.json").open() as f:
            table_schema = json.load(f)
        create_table(self.client, self.default_table_name, table_schema)

    def test_infer_schema_from_query(self):
        self.parser_and_execute_infer_schema_from_query()
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_aito_schema.json')

    def test_upload_data_from_query(self):
        self.create_table()
        self.parse_and_execute_upload_data_from_query()
        result_table_entries = query_entries(self.client, self.default_table_name)
        with (self.input_folder / 'invoice_no_null_value.json').open() as exp_f:
            self.assertCountEqual(result_table_entries, json.load(exp_f))

    def test_upload_data_from_query_table_not_exist(self):
        self.parse_and_execute_upload_data_from_query_table_not_exist()

    def test_quick_add_table_from_query(self):
        self.parse_and_execute_quick_add_table()
        result_table_entries = query_entries(self.client, self.default_table_name)
        with (self.input_folder / 'invoice_no_null_value.json').open() as exp_f:
            self.assertCountEqual(result_table_entries, json.load(exp_f))


class TestMySQLFunctions(TestSQLFunctions):
    def create_table(self):
        with (self.input_folder / "invoice_aito_schema_lower_case_columns.json").open() as f:
            table_schema = json.load(f)
        create_table(self.client, self.default_table_name, table_schema)

    def test_infer_schema_from_query(self):
        self.parser_and_execute_infer_schema_from_query()
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_aito_schema_lower_case_columns.json')

    def test_upload_data_from_query(self):
        self.create_table()
        self.parse_and_execute_upload_data_from_query()
        result_table_entries = query_entries(self.client, self.default_table_name)
        with (self.input_folder / 'invoice_no_null_value_lower_case_columns.json').open() as exp_f:
            self.assertCountEqual(result_table_entries, json.load(exp_f))

    def test_upload_data_from_query_table_not_exist(self):
        self.parse_and_execute_upload_data_from_query_table_not_exist()

    def test_quick_add_table_from_query(self):
        self.parse_and_execute_quick_add_table()
        result_table_entries = query_entries(self.client, self.default_table_name)
        with (self.input_folder / 'invoice_no_null_value_lower_case_columns.json').open() as exp_f:
            self.assertCountEqual(result_table_entries, json.load(exp_f))
