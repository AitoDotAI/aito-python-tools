import json
import os

from aito.utils.aito_client import AitoClient
from tests.test_case import TestCaseCompare
from uuid import uuid4


class TestPostgresCliFunctions(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass(test_path='sql_functions/cli')
        cls.input_folder = cls.input_folder.parent.parent / 'sample_invoice'
        env_vars = os.environ
        cls.client = AitoClient(env_vars['AITO_INSTANCE_NAME'], env_vars['AITO_API_KEY'])
        cls.default_table_name = f"invoice_{uuid4()}"

    def setUp(self):
        super().setUp()
        self.client.delete_table(self.default_table_name)
        self.out_file_path = self.output_folder / f"{self.method_name}_out.json"

    def create_table(self):
        with (self.input_folder / "invoice_aito_schema.json").open() as f:
            table_schema = json.load(f)
        self.client.put_table_schema(self.default_table_name, table_schema)

    def test_infer_schema_from_query(self):
        os.system(f"python -m aito.cli.main_parser_wrapper infer-table-schema from-sql \"PostgreSQL Unicode\" "
                  f"'SELECT * FROM invoice' > {self.out_file_path}")
        with self.out_file_path.open() as out_f, (self.input_folder / 'invoice_aito_schema.json').open() as exp_f:
            self.assertCountEqual(json.load(out_f), json.load(exp_f))

    def test_upload_data_from_query(self):
        self.create_table()
        os.system(f'python -m aito.cli.main_parser_wrapper database upload-data-from-sql \"PostgreSQL Unicode\" '
                  f'{self.default_table_name} \"SELECT * FROM invoice\"')
        table_entries_result = self.client.query_table_entries(self.default_table_name)
        self.assertEqual(table_entries_result['total'], 4)
        with (self.input_folder / 'invoice_no_null_value.json').open() as exp_f:
            self.assertCountEqual(table_entries_result['hits'], json.load(exp_f))

    def test_quick_add_table_from_query(self):
        os.system('python -m aito.cli.main_parser_wrapper database quick-add-table-from-sql \"PostgreSQL Unicode\" '
                  f'{self.default_table_name} "SELECT * FROM invoice"')
        table_entries_result = self.client.query_table_entries(self.default_table_name)
        self.assertEqual(table_entries_result['total'], 4)
        with (self.input_folder / 'invoice_no_null_value.json').open() as exp_f:
            self.assertCountEqual(table_entries_result['hits'], json.load(exp_f))


class TestMySQLCliFunctions(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass(test_path='sql_functions/cli')
        cls.input_folder = cls.input_folder.parent.parent / 'sample_invoice'
        env_vars = os.environ
        cls.client = AitoClient(env_vars['AITO_INSTANCE_NAME'], env_vars['AITO_API_KEY'])
        cls.default_table_name = f"invoice_{uuid4()}"

    def setUp(self):
        super().setUp()
        self.client.delete_table(self.default_table_name)
        self.out_file_path = self.output_folder / f"{self.method_name}_out.json"

    def create_table(self):
        with (self.input_folder / "invoice_aito_schema_lower_case_columns.json").open() as f:
            table_schema = json.load(f)
        self.client.put_table_schema(self.default_table_name, table_schema)

    def test_infer_schema_from_query(self):
        os.system(f"python -m aito.cli.main_parser_wrapper infer-table-schema from-sql \"MySQL ODBC 8.0 Driver\" "
                  f"'SELECT * FROM invoice' > {self.out_file_path}")
        with self.out_file_path.open() as out_f,\
                (self.input_folder / 'invoice_aito_schema_lower_case_columns.json').open() as exp_f:
            self.assertCountEqual(json.load(out_f), json.load(exp_f))

    def test_upload_data_from_query(self):
        self.create_table()
        os.system('python -m aito.cli.main_parser_wrapper database upload-data-from-sql \"MySQL ODBC 8.0 Driver\" '
                  f'{self.default_table_name} "SELECT * FROM invoice"')
        table_entries_result = self.client.query_table_entries(self.default_table_name)
        self.assertEqual(table_entries_result['total'], 4)
        with (self.input_folder / 'invoice_no_null_value_lower_case_columns.json').open() as exp_f:
            self.assertCountEqual(table_entries_result['hits'], json.load(exp_f))

    def test_quick_add_table_from_query(self):
        os.system('python -m aito.cli.main_parser_wrapper database quick-add-table-from-sql \"MySQL ODBC 8.0 Driver\" '
                  f'{self.default_table_name} "SELECT * FROM invoice"')
        table_entries_result = self.client.query_table_entries('invoice_mysql')
        self.assertEqual(table_entries_result['total'], 4)
        with (self.input_folder / 'invoice_no_null_value_lower_case_columns.json').open() as exp_f:
            self.assertCountEqual(table_entries_result['hits'], json.load(exp_f))
