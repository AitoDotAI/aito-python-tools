import json
import logging.handlers
import sys
from abc import ABC, abstractmethod
from argparse import ArgumentParser, ArgumentTypeError, RawTextHelpFormatter
from os import getenv
from pathlib import Path
from typing import Union, TextIO

from aito.client import AitoClient
from aito.exceptions import BaseError
from aito.utils._credentials_file_utils import get_credentials_file_config

DEFAULT_CONFIG_DIR = Path.home() / '.config' / 'aito'

LOG = logging.getLogger('Parser')


class ArgParser(ArgumentParser):
    """Extends argparse.ArgumentParser to support flag templates for sql and aito_credentials

    """
    def __init__(self, **kwargs):
        super().__init__(formatter_class=RawTextHelpFormatter, **kwargs)

    def error_and_print_help(self, message):
        self.print_help(sys.stderr)
        self.exit(2, f"{self.prog}: error: {message}\n")

    def add_aito_default_credentials_arguments(self):
        """aito credentials (instance_url and api_key) default arguments
        """
        args = self.add_argument_group("aito credential arguments")
        args.add_argument(
            '--profile', type=str, default="default",
            help='use the credentials of the specified profile from the credentials file'
        )
        args.add_argument('-i', '--instance-url', type=str, default='.env', help='specify aito instance url')
        args.add_argument(
            '-k', '--api-key', type=str, default='.env', help='specify aito read-write or read-only API key'
        )
        epilog_str = '''You must provide your Aito credentials to execute database operations.
The CLI checks for flag options, environment variables, and credentials of the specified profile in that order
'''
        self.epilog = epilog_str if not self.epilog else self.epilog + epilog_str

    def add_sql_default_credentials_arguments(self):
        """sql connection default arguments
        """
        args = self.add_argument_group('database connection arguments')
        args.add_argument('--driver', '-D', type=str, help='the name of the ODBC driver', default='.env')
        args.add_argument('--server', '-s', type=str, help='server to connect to', default='.env')
        args.add_argument('--port', '-P', type=str, help='port to connect to', default='.env')
        args.add_argument('--database', '-d', type=str, help='database to connect to', default='.env')
        args.add_argument('--username', '-u', type=str, help='username for authentication', default='.env')
        args.add_argument('--password', '-p', type=str, help='password for authentication', default='.env')

        epilog_str = '''If no database credentials flag is given, the following environment variable are used: 
  SQL_DRIVER, SQL_SERVER, SQL_PORT, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD
'''
        self.epilog = epilog_str if not self.epilog else self.epilog + epilog_str

    def add_csv_format_default_arguments(self):
        self.add_argument('-d', '--delimiter', type=str, default=',', help="delimiter character(default: ',')")
        self.add_argument('-p', '--decimal', type=str, default='.', help="decimal point character(default '.')")

    def add_excel_format_default_arguments(self):
        self.add_argument('-o', '--one-sheet', type=str, metavar='sheet-name', help='read a sheet by sheet name')


class ParseError(BaseError):
    def __init__(self, message):
        super().__init__(message=message, logger=LOG)


class ArgType(ABC):
    @abstractmethod
    def __call__(self, string):
        pass


class PathArgType(ArgType):
    """Customized PathType instead of argparse.FileType to handle close file more gracefully

    """
    def __init__(self, parent_must_exist: bool = False, must_exist: bool = False):
        self.parent_must_exist = parent_must_exist
        self.must_exist = must_exist

    def __call__(self, string) -> Path:
        try:
            path = Path(string)
        except Exception:
            raise ArgumentTypeError(f'invalid path: {string}')
        if self.parent_must_exist and not path.parent.exists():
            raise ArgumentTypeError(f'{path.parent} does not exist')
        if self.must_exist and not path.exists():
            raise ArgumentTypeError(f'{path} does not exist')
        return path


class IOArgType(ArgType):
    """Input Type Argument that parse into FilePathOrBuffer
    """

    def __init__(self, default: str = '-'):
        """

        :param default: special character to signal stdin and stdout
        """
        self.default = default

    def __call__(self, string):
        pass


class InputArgType(IOArgType):
    """parse into sys.stdin if match the default input or a must exist path
    """
    def __call__(self, string) -> Union[TextIO, Path]:
        if string == self.default:
            return sys.stdin
        try:
            path = Path(string)
        except Exception:
            raise ArgumentTypeError(f'invalid path: {string}')
        if not path.exists():
            raise ArgumentTypeError(f'{path} does not exist')
        return path


class OutputArgType(IOArgType):
    """parse into sys.stdout if match the default input or a path whose parent must exist
    """
    def __call__(self, string) -> Union[TextIO, Path]:
        if string == self.default:
            return sys.stdout
        try:
            path = Path(string)
        except Exception:
            raise ArgumentTypeError(f'invalid path: {string}')
        if not path.parent.exists():
            raise ArgumentTypeError(f'{path.parent} does not exist')
        return path


def parse_env_variable(var_name, required=False):
    env_var = getenv(var_name)
    if not env_var and required:
        raise ParseError(f'environment variable `{var_name}` not found')
    return env_var


def prompt_confirmation(content, default: bool = None) -> bool:
    valid_responses = {
        'yes': True,
        'y': True,
        'no': False,
        'n': False
    }
    if default is None:
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


def parse_path_value(path, check_exists=False) -> Path:
    try:
        path = Path(path)
    except Exception:
        raise ParseError(f'invalid path: {path}')
    if check_exists and not path.exists():
        raise ParseError(f'path {path} does not exist')
    return path


def try_load_json(fp: TextIO, parsing_object_name: str = ''):
    try:
        return json.load(fp)
    except json.decoder.JSONDecodeError as e:
        raise ParseError(f'failed to parse JSON {parsing_object_name}: {e.msg}')
    except Exception as e:
        raise ParseError(f'failed to parse JSON {parsing_object_name}: {e}')


def load_json_from_parsed_input_arg(parsed_input_arg: Union[Path, TextIO], parsing_object_name: str = ''):
    if isinstance(parsed_input_arg, Path):
        with parsed_input_arg.open() as in_f:
            return try_load_json(in_f, parsing_object_name)
    else:
        return try_load_json(parsed_input_arg, parsing_object_name)


def pyodbc_is_installed() -> bool:
    import importlib
    return True if importlib.util.find_spec('pyodbc') else False


def create_client_from_parsed_args(parsed_args, check_credentials=True) -> AitoClient:
    """create client from parsed args with the default aito credentials arguments from
    add_aito_default_credentials_arguments

    """
    def check_flag_env_var_default_credential(flag_name, env_var_name, credential_key):
        if parsed_args[flag_name] != '.env':
            return parsed_args[flag_name]
        if parse_env_variable(env_var_name):
            return parse_env_variable(env_var_name)
        LOG.debug(f"{env_var_name} environment variable not found. Checking credentials file")
        config = get_credentials_file_config()
        profile = parsed_args['profile']
        if not config.has_section(profile):
            raise ParseError(f"profile `{profile}` not found. "
                             f"Please edit the credentials file or run `aito configure`")
        if credential_key not in config[profile]:
            raise ParseError(f"{credential_key} not found in profile `{profile}`."
                             f"Please edit the credentials file or run `aito configure`")
        return config[profile][credential_key]

    instance_url = check_flag_env_var_default_credential('instance_url', 'AITO_INSTANCE_URL', 'instance_url')
    api_key = check_flag_env_var_default_credential('api_key', 'AITO_API_KEY', 'api_key')

    client_args = {
        'instance_url': instance_url,
        'api_key': api_key,
        'check_credentials': check_credentials
    }
    return AitoClient(**client_args)


def create_sql_connecting_from_parsed_args(parsed_args):
    """create client from parsed args with the default sql connection arguments from
    add_sql_default_credentials_arguments

    """
    if not pyodbc_is_installed():
        raise ParseError('pyodbc is not installed. Please refer to our documentation: '
                         'https://aito-python-sdk.readthedocs.io/en/latest/sql.html#additional-installation')
    connection_args = {}
    for arg in ('driver', 'server', 'port', 'database', 'username', 'password'):
        connection_arg_name = f"sql_{arg}"
        connection_args[connection_arg_name] = parse_env_variable(connection_arg_name.upper()) if \
            parsed_args[arg] == '.env' else parsed_args[arg]
    from aito.utils.sql_connection import SQLConnection
    return SQLConnection(**connection_args)
