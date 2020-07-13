"""A utility to connect to a SQL Database

"""

import pyodbc
import pandas as pd
import logging


LOG = logging.getLogger('SQLConnection')


class SQLConnectionError(Exception):
    def __init__(self, message):
        super().__init__(message)


class SQLConnection:
    """Connection to a SQL Database using the pyodbc module
    """
    def __init__(
            self,
            sql_driver: str,
            sql_server: str = None,
            sql_port: str = None,
            sql_database: str = None,
            sql_username: str = None,
            sql_password: str = None
    ):
        """

        :param sql_driver: ODBC driver name
        :type sql_driver: str
        :param sql_server: database server, defaults to None
        :type sql_server: str, optional
        :param sql_port: database port, defaults to None
        :type sql_port: str, optional
        :param sql_database: database name, defaults to None
        :type sql_database: str, optional
        :param sql_username: database username, defaults to None
        :type sql_username: str, optional
        :param sql_password: database password, defaults to None
        :type sql_password: str, optional
        :raises SQLConnectionError: An error occurred during the establishment of the connection
        """
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

    @staticmethod
    def save_cursor_result_to_df(cursor: pyodbc.Cursor) -> pd.DataFrame:
        """Save the executed query cursor to a pandas DataFrame

        :param cursor: query cursor
        :type cursor: pyodbc.Cursor
        :return: result Pandas DataFrame
        :rtype: pd.DataFrame
        """
        descriptions = cursor.description
        col_names = [desc[0] for desc in descriptions]
        df = pd.DataFrame.from_records(cursor.fetchall(), columns=col_names)
        LOG.debug('saved cursor to DataFrame')
        return df

    def execute_query(self, query_string: str) -> pyodbc.Cursor:
        """execute a query

        :param query_string: input query
        :type query_string: str
        :raises SQLConnectionError: An error occurred during the execution of the query
        :return: cursor
        :rtype: pyodbc.Cursor
        """
        LOG.debug(f'executing query: {query_string}')
        try:
            cursor = self.connection.execute(query_string)
        except Exception as e:
            raise SQLConnectionError(f"Failed to execute query {query_string}: {e}")
        LOG.debug('executed query')
        return cursor

    def execute_query_and_save_result(self, query: str) -> pd.DataFrame:
        """execute a query and save the result to pandas DataFrame

        :param query: input query
        :type query: str
        :return: result Pandas DataFrame
        :rtype: pd.DataFrame
        """
        cursor = self.execute_query(query)
        return self.save_cursor_result_to_df(cursor)

    def close(self):
        """close the connection
        """
        self.connection.close()
