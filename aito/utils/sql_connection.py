import pyodbc
import pandas as pd
import logging


class SQLConnectionError(Exception):
    def __init__(self, message):
        super().__init__(message)


class SQLConnection():
    def __init__(self,
                 sql_driver: str,
                 sql_server: str = None,
                 sql_port : str = None,
                 sql_database : str = None,
                 sql_username: str = None,
                 sql_password: str = None):
        self.logger = logging.getLogger('SQLConnection')
        if not sql_driver:
            raise SQLConnectionError(f"Missing Driver")
        available_drivers = pyodbc.drivers()
        if sql_driver not in available_drivers:
            raise SQLConnectionError(f"Driver {sql_driver} not found. Available drivers: {available_drivers}")
        self.driver = sql_driver

        connection_params = {
            'server': sql_server,
            'port': sql_port,
            'database': sql_database,
            'uid': sql_username,
            'pwd': sql_password
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
