import argparse
import os
import sys
import warnings
from abc import abstractmethod
from pathlib import Path

from dotenv import load_dotenv

from aito.utils._typing import FilePathOrBuffer
from aito.utils.aito_client import AitoClient


class AitoArgParser(argparse.ArgumentParser):
    def __init__(self, **kwargs):
        super().__init__(formatter_class=argparse.RawTextHelpFormatter, **kwargs)

    def error(self, message):
        sys.stderr.write(f"error: {message}\n")
        self.print_help()
        sys.exit(2)

    def parse_path_value(self, path, check_exists=False) -> Path:
        try:
            path = Path(path)
        except Exception as e:
            self.error(f"invalid path {path}")
            raise e
        if check_exists and not path.exists():
            self.error(f"path {path} does not exist")
        return path

    def parse_input_arg_value(self, input_arg: str) -> FilePathOrBuffer:
        return sys.stdin if input_arg == '-' else self.parse_path_value(input_arg, True)

    def parse_output_arg_value(self, output_arg: str) -> FilePathOrBuffer:
        return sys.stdout if output_arg == '-' else self.parse_path_value(output_arg)

    @staticmethod
    def parse_env_variable(var_name):
        if var_name not in os.environ:
            warnings.warn(f'{var_name} environment variable not found')
            return None
        return os.environ[var_name]

    @staticmethod
    def ask_confirmation(content, default: bool = None) -> bool:
        valid_responses = {
            'yes': True,
            'y': True,
            'no': False,
            'n': False
        }
        if not default:
            prompt = '[y/n]'
        elif default:
            prompt = '[Y/n]'
        else:
            prompt = '[y/N]'
        while True:
            sys.stdout.write(f"{content} {prompt}")
            response = input().lower()
            if default and response == '':
                return default
            elif response in valid_responses:
                return valid_responses[response]
            else:
                sys.stdout.write("Please respond with yes(y) or no(n)'\n")

    def add_aito_credentials_arguments_flags(self, add_use_env_arg = False):
        args = self.add_argument_group("aito credential arguments")
        if add_use_env_arg:
            args.add_argument('-e', '--use-env-file', type=str, metavar='env-input-file',
                              help='set up the credentials using a .env file containing the required env variables')
        args.add_argument('-i', '--instance-url', type=str, default='.env', help='specify aito instance url')
        args.add_argument('-k', '--api-key', type=str, default='.env',
                          help='specify aito read-write or read-only API key')
        epilog_str = '''You must provide your Aito credentials to execute database operations
If no Aito credential is given, the following environment variables are used to connect to your Aito database:
  AITO_INSTANCE_URL, AITO_API_KEY
  '''
        if not self.epilog:
            self.epilog = epilog_str
        else:
            self.epilog += epilog_str

    def add_sql_credentials_arguments_flags(self, add_use_env_arg: bool = False):
        import pyodbc
        self.add_argument('driver', type=str, choices=list(pyodbc.drivers()), help='the ODBC driver name to be used')
        args = self.add_argument_group('database connection arguments')
        if add_use_env_arg:
            args.add_argument('-e', '--use-env-file', type=str, metavar='env-input-file',
                              help='set up the credentials using a .env file containing the required env variables')

        args.add_argument('--server', '-s', type=str, help='server to connect to', default='.env')
        args.add_argument('--port', '-P', type=str, help='port to connect to', default='.env')
        args.add_argument('--database', '-d', type=str, help='database to connect to', default='.env')
        args.add_argument('--username', '-u', type=str, help='username for authentication', default='.env')
        args.add_argument('--password', '-p', type=str, help='password for authentication', default='.env')

        epilog_str = '''Each database requires different odbc driver. Please refer to our docs for more info.
If no database connection is given, the following environment variable are used to connect to your SQL database:
  SQL_SERVER, SQL_PORT, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD
  '''
        if not self.epilog:
            self.epilog = epilog_str
        else:
            self.epilog += epilog_str

    def create_client_from_parsed_args(self, parsed_args):
        if parsed_args['use_env_file']:
            env_file_path = self.parse_path_value(parsed_args['use_env_file'], True)
            load_dotenv(env_file_path)

        client_args = {
            'instance_url': self.parse_env_variable('AITO_INSTANCE_URL') if parsed_args['instance_url'] == '.env'
            else parsed_args['instance_url'],
            'api_key': self.parse_env_variable('AITO_API_KEY') if parsed_args['api_key'] == '.env'
            else parsed_args['api_key']
        }
        return AitoClient(**client_args)

    def create_sql_connecting_from_parsed_args(self, parsed_args):
        if 'driver' not in parsed_args:
            self.error("Cannot create SQL connection without driver")
        connection_args = {'sql_driver': parsed_args['driver']}
        for arg in ['server', 'port', 'database', 'username', 'password']:
            connection_arg_name = f"sql_{arg}"
            connection_args[connection_arg_name] = self.parse_env_variable(connection_arg_name.upper()) if \
                parsed_args[arg] == '.env' else parsed_args[arg]
        from aito.utils.sql_connection import SQLConnection
        return SQLConnection(**connection_args)
