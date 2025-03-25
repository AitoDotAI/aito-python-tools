import json
import os

from aito.utils.sql_connection import SQLConnection
from tests.cases import CompareTestCase


class TestPostgresConnection(CompareTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.input_folder = cls.input_folder.parent.parent / 'sample_invoice'
        env_variables = os.environ
        cls.connection = SQLConnection(
            "PostgreSQL Unicode", env_variables.get('SQL_SERVER'), env_variables.get('SQL_PORT'),
            env_variables.get('SQL_DATABASE'), env_variables.get('SQL_USERNAME'), env_variables.get('SQL_PASSWORD'))
        c = cls.connection.execute_query('DROP TABLE IF EXISTS invoice;')
        c.close()

    def test_create_table(self):
        cursor = self.connection.execute_query("""
        CREATE TABLE invoice(
            id serial primary key,
            name VARCHAR(355) not null,
            amount double precision not null,
            \"Remark\" VARCHAR (355)
        );
        """)
        cursor.close()

    def test_insert_into_table(self):
        cursor = self.connection.execute_query("""
        INSERT INTO invoice(name, amount, \"Remark\") VALUES 
            ('Johnson, Smith, and Jones Co.', 345.33, 'Pays on time'),
            (E'Sam \"Mad Dog\" Smith', 993.44, NULL),
            ('Barney & Company', 0, E'Great to work with\nand always pays with cash.'),
            (E'Johnson''s Automotive', 2344, NULL);
        """)
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


class TestMySQLConnection(CompareTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.input_folder = cls.input_folder.parent.parent / 'sample_invoice'
        env_variables = os.environ
        cls.connection = SQLConnection(
            env_variables.get('SQL_DRIVER', "MySQL ODBC 8.0 Driver"),
            env_variables.get('SQL_SERVER'),
            env_variables.get('SQL_PORT'),
            env_variables.get('SQL_DATABASE'),
            env_variables.get('SQL_USERNAME'),
            env_variables.get('SQL_PASSWORD'))
        c = cls.connection.execute_query('DROP TABLE IF EXISTS invoice;')
        c.close()

    def test_create_table(self):
        cursor = self.connection.execute_query("""
        CREATE TABLE invoice(
            id serial primary key,
            name VARCHAR(355) not null,
            amount double precision not null,
            remark VARCHAR (355)
        );
        """)
        cursor.close()

    def test_insert_into_table(self):
        cursor = self.connection.execute_query("""
        INSERT INTO invoice(name, amount, remark) VALUES
          ('Johnson, Smith, and Jones Co.', 345.33, 'Pays on time'),
          ('Sam \"Mad Dog\" Smith', 993.44, NULL),
          ('Barney & Company', 0, 'Great to work with\nand always pays with cash.'),
          ('Johnson''s Automotive', 2344, NULL);
        """)
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

        # MySQL column name not case sensitive
        with (self.input_folder / 'invoice_lower_case_columns.json').open() as f:
            expected_results = json.load(f)
        self.assertCountEqual(results_as_json, expected_results)
        cursor.close()
