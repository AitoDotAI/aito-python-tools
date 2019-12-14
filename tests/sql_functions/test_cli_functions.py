import json
import os

from aito.utils.aito_client import AitoClient
from tests.test_case import TestCaseCompare


class TestPostgresCliFunctions(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass(test_path='sql_functions/cli')
        cls.input_folder = cls.input_folder.parent.parent / 'sample_invoice'
        env_vars = os.environ
        cls.client = AitoClient(env_vars['AITO_INSTANCE_NAME'], env_vars['AITO_API_KEY'])

    def setUp(self):
        super().setUp()
        self.client.delete_table('invoice_postgres')
        self.out_file_path = self.output_folder / f"{self.method_name}_out.json"

    def create_table(self):
        with (self.input_folder / "invoice_aito_schema.json").open() as f:
            table_schema = json.load(f)
        self.client.put_table_schema('invoice_postgres', table_schema)

    def test_infer_schema_from_query(self):
        os.system(f"python -m aito.cli.main_parser_wrapper infer-table-schema from-sql \"PostgreSQL Unicode\" "
                  f"'SELECT * FROM invoice' > {self.out_file_path}")
        self.assertCountEqual(json.load(self.out_file_path.open()),
                              json.load((self.input_folder / 'invoice_aito_schema.json').open()))

    def test_upload_data_from_query(self):
        self.create_table()
        os.system('python -m aito.cli.main_parser_wrapper database upload-data-from-sql \"PostgreSQL Unicode\" '
                  'invoice_postgres "SELECT * FROM invoice"')
        table_entries_result = self.client.query_table_entries('invoice_postgres')
        self.assertEqual(table_entries_result['total'], 4)
        self.assertCountEqual(table_entries_result['hits'],
                              json.load((self.input_folder / 'invoice_no_null_value.json').open()))

    def test_quick_add_table_from_query(self):
        os.system('python -m aito.cli.main_parser_wrapper database quick-add-table-from-sql \"PostgreSQL Unicode\" '
                  'invoice_postgres "SELECT * FROM invoice"')
        table_entries_result = self.client.query_table_entries('invoice_postgres')
        self.assertEqual(table_entries_result['total'], 4)
        self.assertCountEqual(table_entries_result['hits'],
                              json.load((self.input_folder / 'invoice_no_null_value.json').open()))


class TestMySQLCliFunctions(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass(test_path='sql_functions/cli')
        cls.input_folder = cls.input_folder.parent.parent / 'sample_invoice'
        env_vars = os.environ
        cls.client = AitoClient(env_vars['AITO_INSTANCE_NAME'], env_vars['AITO_API_KEY'])

    def setUp(self):
        super().setUp()
        self.client.delete_table('invoice_mysql')
        self.out_file_path = self.output_folder / f"{self.method_name}_out.json"

    def create_table(self):
        with (self.input_folder / "invoice_aito_schema_lower_case_columns.json").open() as f:
            table_schema = json.load(f)
        self.client.put_table_schema('invoice_mysql', table_schema)

    def test_infer_schema_from_query(self):
        os.system(f"python -m aito.cli.main_parser_wrapper infer-table-schema from-sql \"MySQL ODBC 8.0 Driver\" "
                  f"'SELECT * FROM invoice' > {self.out_file_path}")
        self.assertCountEqual(json.load(self.out_file_path.open()),
                              json.load((self.input_folder / 'invoice_aito_schema_lower_case_columns.json').open()))

    def test_upload_data_from_query(self):
        self.create_table()
        os.system('python -m aito.cli.main_parser_wrapper database upload-data-from-sql \"MySQL ODBC 8.0 Driver\" '
                  'invoice_mysql "SELECT * FROM invoice"')
        table_entries_result = self.client.query_table_entries('invoice_mysql')
        self.assertEqual(table_entries_result['total'], 4)
        self.assertCountEqual(table_entries_result['hits'],
                              json.load((self.input_folder / 'invoice_no_null_value_lower_case_columns.json').open()))

    def test_quick_add_table_from_query(self):
        os.system('python -m aito.cli.main_parser_wrapper database quick-add-table-from-sql \"MySQL ODBC 8.0 Driver\" '
                  'invoice_mysql "SELECT * FROM invoice"')
        table_entries_result = self.client.query_table_entries('invoice_mysql')
        self.assertEqual(table_entries_result['total'], 4)
        self.assertCountEqual(table_entries_result['hits'],
                              json.load((self.input_folder / 'invoice_no_null_value_lower_case_columns.json').open()))
