from aito.utils.sql_connection import SQLConnection
from tests.test_case import TestCaseCompare
import os


class TestPostgreSQLConnection(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass(test_path='sql_functions/sql_connector')

    @staticmethod
    def get_connection_from_env_vars():
        env_variables = os.environ
        return SQLConnection('postgres',
                             server=env_variables.get('SERVER'),
                             database=env_variables.get('DATABASE'),
                             port=env_variables.get('PORT'),
                             user=env_variables.get('USER'),
                             pwd=env_variables.get('PASS'))

    def test_connection(self):
        self.get_connection_from_env_vars()

    def test_query_result(self):
        connection = self.get_connection_from_env_vars()
        # cursor = connector.execute_query("SELECT * FROM metrics")