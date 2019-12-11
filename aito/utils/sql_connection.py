import pyodbc
import pandas as pd
import logging


class SQLConnectionError(Exception):
    def __init__(self, message):
        super().__init__(message)


class SQLConnection():
    def __init__(self,
                 driver: str,
                 server: str = None,
                 port : str = None,
                 database : str = None,
                 user: str = None,
                 pwd: str = None):
        self.logger = logging.getLogger('SQLConnection')
        available_drivers = pyodbc.drivers()
        if driver not in available_drivers:
            raise SQLConnectionError(f"Driver {driver} not found. Available drivers: {available_drivers}")
        self.driver = driver

        connection_params = {
            'server': server,
            'port': port,
            'database': database,
            'uid': user,
            'pwd': pwd
        }
        connection_string = f"driver={{{self.driver}}};"
        for param in connection_params:
            if connection_params[param]:
                connection_string += f"{param}={connection_params[param]};"
        self.connection_string = connection_string

        try:
            connection = pyodbc.connect(self.connection_string)
        except Exception as e:
            raise SQLConnectionError(f"Failed to establish connection: {e}")

        connection.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
        connection.setencoding(encoding='utf-8')

        self.connection = connection

    def save_cursor_result_to_df(self, cursor: pyodbc.Cursor) -> pd.DataFrame:
        descriptions = cursor.description
        col_names = [desc[0] for desc in descriptions]
        df = pd.DataFrame.from_records(cursor.fetchall(), columns=col_names)
        self.logger.debug('Query result saved to Dataframe')
        return df

    def execute_query(self, query_string: str) -> pyodbc.Cursor:
        try:
            cursor = self.connection.execute(query_string)
        except Exception as e:
            raise SQLConnectionError(f"Failed to execute query {query_string}: {e}")
        self.logger.debug('Query executed')
        return cursor

    def execute_query_and_save_result(self, query: str) -> pd.DataFrame:
        cursor = self.execute_query(query)
        return self.save_cursor_result_to_df(cursor)

    def close(self):
        self.connection.close()
