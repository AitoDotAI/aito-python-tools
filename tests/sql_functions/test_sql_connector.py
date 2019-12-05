from aito.utils.sql_connector import SQLConnector
from tests.test_case import TestCaseCompare
import os


class TestSQLConnector(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass(test_path='sql_functions/sql_connector')

    def test_postgres_connection(self):
        env_variables = os.environ
        connector = SQLConnector('postgres',
                                 server=env_variables.get('SERVER'),
                                 database=env_variables.get('DATABASE'),
                                 user=env_variables.get('USER'),
                                 pwd=env_variables.get('PASS'))
        connector.connect()