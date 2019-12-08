from aito.utils.sql_connection import SQLConnection
from tests.test_case import TestCaseCompare
import os


class TestPostgreSQLConnection(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass(test_path='sql_functions/sql_connector')
        cls.input_folder = cls.input_folder.parent.parent / 'sample_invoice'
        env_variables = os.environ
        cls.connection = SQLConnection(
            'postgres', server=env_variables.get('SERVER'), database=env_variables.get('DATABASE'),
            port=env_variables.get('PORT'), user=env_variables.get('USER'), pwd=env_variables.get('PASS'))

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
        cursor.close()