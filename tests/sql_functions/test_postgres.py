import json
import os

from aito.utils.aito_client import AitoClient
from aito.utils.sql_connection import SQLConnection
from tests.test_case import TestCaseCompare


class TestPostgresConnection(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass(test_path='sql_functions/sql_connector')
        cls.input_folder = cls.input_folder.parent.parent / 'sample_invoice'
        env_variables = os.environ
        cls.connection = SQLConnection(
            env_variables.get('DRIVER'), env_variables.get('SERVER'), env_variables.get('PORT'),
            env_variables.get('DATABASE'), env_variables.get('USER'), env_variables.get('PWD'))
        c = cls.connection.execute_query('DROP TABLE IF EXISTS invoice;')
        c.close()

    def test_create_table(self):
        with (self.input_folder / 'create_table.sql').open() as f:
            query = f.read()
        cursor = self.connection.execute_query(query)
        cursor.close()

    def test_insert_into_table(self):
        with (self.input_folder / 'insert_into_table.sql').open() as f:
            query = f.read()
        cursor = self.connection.execute_query(query)
        cursor.close()

    def test_query_all(self):
        cursor = self.connection.execute_query('SELECT * FROM invoice;')
        col_names = [desc[0] for desc in cursor.description]
        results_as_json = []
        for row in cursor.fetchall():
            row_as_json = {}
            for idx, cell in enumerate(row):
                row_as_json[col_names[idx]] = cell
            results_as_json.append(row_as_json)

        with (self.input_folder / 'invoice.json').open() as f:
            expected_results = json.load(f)
        self.assertCountEqual(results_as_json, expected_results)
        cursor.close()


class TestPostgresCli(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass(test_path='sql_functions/sql_connector')
        cls.input_folder = cls.input_folder.parent.parent / 'sample_invoice'
        env_vars = os.environ
        cls.client = AitoClient(env_vars['AITO_INSTANCE_NAME'], env_vars['AITO_RW_KEY'], env_vars['AITO_RO_KEY'])

    def setUp(self):
        super().setUp()
        self.client.delete_database()
        self.out_file_path = self.output_folder / f"{self.method_name}_out.json"

    def create_table(self):
        with (self.input_folder / "invoice_aito_schema.json").open() as f:
            table_schema = json.load(f)
        self.client.put_table_schema('invoice', table_schema)

    def test_infer_schema_from_query(self):
        os.system(f"python -m aito.cli.main_parser_wrapper infer-table-schema from-sql "
                  f"'SELECT * FROM invoice' > {self.out_file_path}")
        self.assertCountEqual(json.load(self.out_file_path.open()),
                              json.load((self.input_folder / 'invoice_aito_schema.json').open()))

    def test_upload_data_from_query(self):
        self.create_table()
        os.system('python -m aito.cli.main_parser_wrapper database upload-data-from-sql invoice '
                  '"SELECT * FROM invoice"')
        self.assertEqual(self.client.query_table_entries('invoice')['total'], 4)

    def test_quick_add_table_from_query(self):
        os.system('python -m aito.cli.main_parser_wrapper database quick-add-table-from-sql invoice '
                  '"SELECT * FROM invoice"')
        table_entries_result = self.client.query_table_entries('invoice')
        self.assertEqual(table_entries_result['total'], 4)
        self.assertCountEqual(table_entries_result['hits'],
                              json.load((self.input_folder / 'invoice_no_null_value.json').open()))
