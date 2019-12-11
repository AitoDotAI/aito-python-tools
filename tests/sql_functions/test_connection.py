import json
import os

from aito.utils.sql_connection import SQLConnection
from tests.test_case import TestCaseCompare


class TestSQLConnection(TestCaseCompare):
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
