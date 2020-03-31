import json
import sys
from os import getenv
from pathlib import Path
from typing import Union, TextIO

from dotenv import load_dotenv

from aito.utils.aito_client import AitoClient
from .parser import ParseError


def parse_env_variable(var_name, required=False):
    env_var = getenv(var_name)
    if not env_var and required:
        raise ParseError(f'missing environment variable `{var_name}`')
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


def try_json_load(fp: TextIO, parsing_object_name: str = ''):
    try:
        return json.load(fp)
    except json.decoder.JSONDecodeError as e:
        raise ParseError(f'failed to parse {parsing_object_name}: {e.msg}')
    except Exception as e:
        raise ParseError(f'failed to parse {parsing_object_name}: {e}')


def load_json_from_parsed_input_arg(parsed_input_arg: Union[Path, TextIO], parsing_object_name: str = ''):
    if isinstance(parsed_input_arg, Path):
        with parsed_input_arg.open() as in_f:
            return try_json_load(in_f, parsing_object_name)
    else:
        return try_json_load(parsed_input_arg, parsing_object_name)


def pyodbc_installed() -> bool:
    import importlib
    if not importlib.util.find_spec('pyodbc'):
        return False
    return True


def create_client_from_parsed_args(parsed_args):
    if parsed_args['use_env_file']:
        env_file_path = parse_path_value(parsed_args['use_env_file'], True)
        load_dotenv(str(env_file_path))

    client_args = {
        'instance_url': parse_env_variable('AITO_INSTANCE_URL') if parsed_args['instance_url'] == '.env'
        else parsed_args['instance_url'],
        'api_key': parse_env_variable('AITO_API_KEY') if parsed_args['api_key'] == '.env'
        else parsed_args['api_key']
    }
    return AitoClient(**client_args)


def create_sql_connecting_from_parsed_args(parsed_args):
    if not pyodbc_installed():
        raise ParseError('pyodbc is not installed. Please refer to our documentation: '
                         'https://aitodotai.github.io/aito-python-tools/sql.html#additional-installation')

    connection_args = {}
    for arg in ('driver', 'server', 'port', 'database', 'username', 'password'):
        connection_arg_name = f"sql_{arg}"
        connection_args[connection_arg_name] = parse_env_variable(connection_arg_name.upper()) if \
            parsed_args[arg] == '.env' else parsed_args[arg]
    from aito.utils.sql_connection import SQLConnection
    return SQLConnection(**connection_args)
