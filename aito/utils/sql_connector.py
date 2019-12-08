import pyodbc
import pandas as pd


class SQLConnectorError(Exception):
    def __init__(self, message):
        super().__init__(message)


class SQLConnector():
    allowed_database_types = ['postgres']
    database_driver_mapper = {
        'postgres': 'PostgreSQL Unicode'
    }

    def __init__(self, typ: str, server=None, port=None, database=None, user=None, pwd=None):
        if typ not in self.allowed_database_types:
            raise SQLConnectorError(f"Invalid database type: {typ}. Must be among: {self.allowed_database_types}")

        self.typ = typ
        self.params = {
            'server': server,
            'port': port,
            'database': database,
            'uid': user,
            'pwd': pwd
        }
        self.cnxn = None

    def build_connection_string(self) -> str:
        connection_str = f"driver={{{self.database_driver_mapper[self.typ]}}};"
        for param in self.params:
            if self.params[param]:
                connection_str += f"{param}={self.params[param]};"
        return connection_str

    def connect(self) -> pyodbc.Connection:
        cnxn_str = self.build_connection_string()
        try:
            cnxn = pyodbc.connect(cnxn_str)
        except Exception as e:
            raise SQLConnectorError(f"Failed to connect: {e}")
        if self.typ == 'postgres':
            cnxn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
            cnxn.setencoding(encoding='utf-8')

        self.cnxn = cnxn
        return cnxn

    def execute_query(self, query_string: str) -> pyodbc.Cursor:
        if not self.cnxn:
            raise SQLConnectorError("No connection found. Connect before execute query")
        try:
            cursor = self.cnxn.execute(query_string)
        except Exception as e:
            raise SQLConnectorError(f"Failed to execute querry: {e}")
        return cursor

    @staticmethod
    def save_result_to_df(cursor: pyodbc.Cursor) -> pd.DataFrame:
        descriptions = cursor.description
        col_names = [desc[0] for desc in descriptions]
        return pd.DataFrame.from_records(cursor.fetchall(), columns=col_names)



    def close(self):
        self.cnxn.close()
