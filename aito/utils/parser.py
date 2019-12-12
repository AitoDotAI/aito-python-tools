import argparse
import sys
from abc import abstractmethod
import logging
from pathlib import Path
from aito.utils._typing import FilePathOrBuffer
import os
from dotenv import load_dotenv

from aito.utils.aito_client import AitoClient


class AitoArgParser(argparse.ArgumentParser):
    def __init__(self, **kwargs):
        super().__init__(formatter_class=argparse.RawTextHelpFormatter, **kwargs)
        self.logger = logging.getLogger('Parser')

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

    def parse_env_variable(self, var_name):
        if var_name not in os.environ:
            self.logger.warning(f"{var_name} environment variable not found")
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

    def add_aito_credentials_arguments(self, add_use_env_arg = False):
        args = self.add_argument_group("aito credential arguments")
        if add_use_env_arg:
            args.add_argument('-e', '--use-env-file', type=str, metavar='env-input-file',
                              help='set up the credentials using a .env file containing the required env variables')
        args.add_argument('-r', '--read-only-key', type=str, default='.env',
                          help='specify aito read-only API key')
        args.add_argument('-i', '--instance-name', type=str, default='.env',
                          help='specify aito instance name')
        args.add_argument('-w', '--read-write-key', type=str, default='.env',
                          help='specify aito read-write API key')
        epilog_str = '''You must provide your Aito credentials to execute database operations
If no credential options is given, the following environment variable is used to connect to your Aito database:
  AITO_INSTANCE_NAME=your-instance-name
  AITO_RW_KEY=your read-write api key
  AITO_RO_KEY=your read-only key (optional)
  '''
        if not self.epilog:
            self.epilog = epilog_str
        else:
            self.epilog += epilog_str

    def add_sql_credentials_arguments(self, add_use_env_arg: bool = False):
        args = self.add_argument_group('database connection arguments')
        if add_use_env_arg:
            args.add_argument('-e', '--use-env-file', type=str, metavar='env-input-file',
                              help='set up the credentials using a .env file containing the required env variables')

        args.add_argument('--driver', '-D', type=str, default='.env',
                          help='use the default driver for the specified database type')
        args.add_argument('--server', '-s', type=str, help='server to connect to', default='.env')
        args.add_argument('--port', '-P', type=str, help='port to connect to', default='.env')
        args.add_argument('--database', '-d', type=str, help='database to connect to', default='.env')
        args.add_argument('--user', '-u', type=str, help='username for authentication', default='.env')
        args.add_argument('--pwd', '-p', type=str, help='password for authentication', default='.env')

        epilog_str = '''Each database requires different odbc driver. Please refer to our docs for more info.
If no credential options is given, the following environment variable is used to connect to your SQL database:
  SQL_DRIVER, SQL_SERVER, SQL_PORT, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD          
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
            'instance_name': self.parse_env_variable('AITO_INSTANCE_NAME') if parsed_args['instance_name'] == '.env'
            else parsed_args['instance_name'],
            'rw_key': self.parse_env_variable('AITO_RW_KEY') if parsed_args['read_write_key'] == '.env'
            else parsed_args['read_write_key'],
            'ro_key': self.parse_env_variable('AITO_RO_KEY') if parsed_args['read_only_key'] == '.env'
            else parsed_args['read_only_key']
        }
        return AitoClient(**client_args)

    def create_sql_connecting_from_parsed_args(self, parsed_args):
        args = {}
        for arg_name in ['sql_driver', 'sql_server', 'sql_port', 'sql_database', 'sql_username', 'sql_password']:
            args[arg_name] = self.parse_env_variable(arg_name.upper()) if parsed_args[arg_name] == '.env' \
                else parsed_args[arg_name]
        from aito.utils.sql_connection import SQLConnection
        return SQLConnection(**args)


class ParserWrapper:
    def __init__(self, add_help=True):
        if add_help:
            self.parser = AitoArgParser()
        else:
            self.parser = AitoArgParser(add_help=False)

    @abstractmethod
    def parse_and_execute(self, parsing_args):
        pass
