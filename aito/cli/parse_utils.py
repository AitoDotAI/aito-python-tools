import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from aito.utils._typing import FilePathOrBuffer
from aito.utils.aito_client import AitoClient


class ParseError(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(message)


def parse_env_variable(var_name, required=False):
    env_var = os.getenv(var_name)
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


def parse_input_arg_value(input_arg_value: str) -> FilePathOrBuffer:
    return sys.stdin if input_arg_value == '-' else parse_path_value(input_arg_value, True)


def parse_output_arg_value(output_arg_value: str) -> FilePathOrBuffer:
    return sys.stdout if output_arg_value == '-' else parse_path_value(output_arg_value)


def try_json_load(fp, parsing_object_name: str=''):
    try:
        return json.load(fp)
    except json.decoder.JSONDecodeError as e:
        raise ParseError(f'failed to parse {parsing_object_name}: {e.msg}')
    except Exception as e:
        raise ParseError(f'failed to parse {parsing_object_name}: {e}')


def load_json_from_input_arg_value(input_arg_value: str, parsing_object_name: str = ''):
    if input_arg_value == '-':
        return try_json_load(sys.stdin, parsing_object_name)
    else:
        input_path = parse_path_value(input_arg_value)
        with input_path.open() as f:
            return try_json_load(f, parsing_object_name)


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
