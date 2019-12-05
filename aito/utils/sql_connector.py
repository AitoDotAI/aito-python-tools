import pyodbc


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

    def build_connection_string(self):
        connection_str = f"driver={{{self.database_driver_mapper[self.typ]}}};"
        for param in self.params:
            if self.params[param]:
                connection_str += f"{param}={self.params[param]};"
        return connection_str

    def connect(self):
        cnxn_str = self.build_connection_string()
        try:
            pyodbc.connect(cnxn_str)
        except Exception as e:
            raise SQLConnectorError(f"Failed to connect: {e}")