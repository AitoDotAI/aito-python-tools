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
            'postgres', server=env_variables.get('SERVER'), database=env_variables.get('DATABASE'),
            port=env_variables.get('PORT'), user=env_variables.get('USER'), pwd=env_variables.get('PASS'))
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

        # Populate data to Postgres DB
        connection = SQLConnection(
            'postgres', server=env_vars.get('SERVER'), database=env_vars.get('DATABASE'),
            port=env_vars.get('PORT'), user=env_vars.get('USER'), pwd=env_vars.get('PASS'))
        connection.execute_query('DROP TABLE IF EXISTS invoice;')
        with (cls.input_folder / 'create_table.sql').open() as f:
            query = f.read()
        connection.execute_query(query)
        with (cls.input_folder / 'insert_into_table.sql').open() as f:
            query = f.read()
        connection.execute_query(query)
        connection.close()

        cls.client = AitoClient(env_vars['AITO_INSTANCE_NAME'], env_vars['AITO_RW_KEY'], env_vars['AITO_RO_KEY'])

    def setUp(self):
        super().setUp()
        self.client.delete_database()

    def create_table(self):
        with (self.input_folder / "invoice_aito_schema.json").open() as f:
            table_schema = json.load(f)
        self.client.put_table_schema('invoice', table_schema)

    def test_upload_data_from_query(self):
        self.create_table()
        os.system('python -m aito.cli.main_parser_wrapper database upload-data-from-sql postgres invoice '
                  '"SELECT * FROM invoice"')
        self.assertEqual(self.client.query_table_entries('invoice')['total'], 4)
