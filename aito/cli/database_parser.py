import argparse
import json
import os
import sys
from datetime import datetime

from dotenv import load_dotenv

from aito.utils.aito_client import AitoClient
from aito.cli.parser import AitoArgParser
from aito.utils.data_frame_handler import DataFrameHandler


def create_client_from_parsed_args(main_parser, parsed_args):
    def get_env_variable(variable_name):
        if variable_name not in env_var:
            main_parser.error(f"{variable_name} env variable not found")
        return env_var[variable_name]

    if parsed_args['use_env_file']:
        env_file_path = main_parser.check_valid_path(parsed_args['use_env_file'], True)
        load_dotenv(env_file_path)
    env_var = os.environ

    client_args = {
        'url': get_env_variable('AITO_INSTANCE_URL') if parsed_args['url'] == '.env' else parsed_args['url'],
        'rw_key': get_env_variable('AITO_RW_KEY') if parsed_args['read_write_key'] == '.env'
        else parsed_args['read_write_key'],
        'ro_key': get_env_variable('AITO_RO_KEY') if parsed_args['read_only_key'] == '.env'
        else parsed_args['read_only_key']
    }
    return AitoClient(**client_args)


def add_quick_add_table_parser(operation_subparsers):
    parser = operation_subparsers.add_parser(
        'quick-add-table', help="infer schema, create table from file, and upload to the database"
    )
    parser.epilog = '''Note: this operation requires write permission to create a ndjson file for file upload
Example:
  aito database quick-add-table myFile.json
  aito database quick-add-table --table-name aTableName myFile.csv
  aito database quick-add-table -n myTable -f csv myFile
'''
    parser.add_argument(
        '-n', '--table-name', type=str,
        help='create a table with the given name (default: the input file name without the extension)')

    file_format_choices = ['infer'] + DataFrameHandler.allowed_format
    parser.add_argument(
        '-f', '--file-format', type=str, choices=file_format_choices, default='infer',
        help='specify the input file format (default: infer from the file extension)')
    parser.add_argument('input-file', type=str, help="path to the input file")
    return parser


def execute_quick_add_table(main_parser, parsed_args):
    input_file_path = main_parser.check_valid_path(parsed_args['input-file'])
    table_name = parsed_args['table_name'] if parsed_args['table_name'] else input_file_path.stem
    in_format = input_file_path.suffixes[0].replace('.', '') if parsed_args['file_format'] == 'infer' \
        else parsed_args['file_format']

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    converted_file_path = input_file_path.parent / f"{input_file_path.stem}_{now}.ndjson.gz"
    schema_path = input_file_path.parent / f"{table_name}_schema_{now}.json"
    convert_options = {
        'read_input': input_file_path,
        'write_output': converted_file_path,
        'in_format': in_format,
        'out_format': 'ndjson',
        'convert_options': {'compression': 'gzip'},
        'create_table_schema': schema_path
    }
    df_handler = DataFrameHandler()
    df_handler.convert_file(**convert_options)

    client = create_client_from_parsed_args(main_parser, parsed_args)
    with schema_path.open() as f:
        inferred_schema = json.load(f)
    client.put_table_schema(table_name, inferred_schema)
    client.populate_table_by_file_upload(table_name, converted_file_path)

    if converted_file_path.exists():
        converted_file_path.unlink()
    schema_path.unlink()
    return 0


def add_create_table_parser(operation_subparsers):
    parser = operation_subparsers.add_parser('create-table', help='create a table using the input table schema')
    parser.epilog = '''With no table-schema-input or when input is -, read from standard input
Example:
  aito client create-table aTableName < path/to/tableSchemaFile.json
  '''
    parser.add_argument('table-name', type=str, help="name of the table to be created")
    parser.add_argument('schema-input', default='-', type=str, nargs='?', help="table schema input")
    return parser


def execute_create_table(main_parser, parsed_args):
    client = create_client_from_parsed_args(main_parser, parsed_args)
    table_name = parsed_args['table-name']
    if parsed_args['schema-input'] == '-':
        table_schema = json.load(sys.stdin)
    else:
        input_path = main_parser.check_valid_path(parsed_args['schema-input'])
        with input_path.open() as f:
            table_schema = json.load(f)
    client.put_table_schema(table_name, table_schema)
    return 0


def add_delete_table_parser(operation_subparsers):
    parser = operation_subparsers.add_parser('delete-table',
                                             help="delete a table schema and all content inside the table")
    parser.add_argument('table-name', type=str, help="name of the table to be deleted")
    return parser


def execute_delete_table(main_parser, parsed_args):
    client = create_client_from_parsed_args(main_parser, parsed_args)
    table_name = parsed_args['table-name']
    if main_parser.ask_confirmation(f"Confirm delete table '{table_name}'? The action is irreversible", False):
        client.delete_table(table_name)
    return 0


def add_delete_database_parser(operation_subparsers):
    parser = operation_subparsers.add_parser('delete-database', help='delete the whole database')
    return parser


def execute_delete_database(main_parser, parsed_args):
    client = create_client_from_parsed_args(main_parser, parsed_args)
    if main_parser.ask_confirmation(f"Confirm delete the whole database? The action is irreversible", False):
        client.delete_database()
    return 0


def add_upload_batch_parser(operation_subparsers):
    parser = operation_subparsers.add_parser('upload-batch', help='populate table entries to a table')
    parser.epilog = '''With no input, or when input is -, read table content from standard input
Example:
  aito client upload-batch myTable path/to/myTableEntries.json
  aito client upload-batch myTable < path/to/myTableEntries.json
  '''
    parser.add_argument('table-name', type=str, help='name of the table to be populated')
    parser.add_argument('input', default='-', type=str, nargs='?', help='input file or stream in JSON array format')


def execute_upload_batch(main_parser, parsed_args):
    client = create_client_from_parsed_args(main_parser, parsed_args)
    table_name = parsed_args['table-name']
    if parsed_args['input'] == '-':
        table_content = json.load(sys.stdin)
    else:
        input_path = main_parser.check_valid_path(parsed_args['input'])
        with input_path.open() as f:
            table_content = json.load(f)
    client.populate_table_entries(table_name, table_content)
    return 0


def add_upload_file_parser(operation_subparsers):
    parser = operation_subparsers.add_parser('upload-file', help='populate a file to a table')
    parser.epilog = '''Example:
  aito client upload-file tableName path/to/myFile.csv
  aito client upload-file -f csv tableName path/to/myFile.txt
  '''

    parser.add_argument('table-name', type=str, help='name of the table to be populated')
    parser.add_argument('input-file', type=str, help="path to the input file")
    file_format_choices = ['infer'] + DataFrameHandler.allowed_format
    parser.add_argument('-f', '--file-format', type=str, choices=file_format_choices,
                        default='infer', help='specify input file format (default: infer from the file extension)')


def execute_upload_file(main_parser, parsed_args):
    client = create_client_from_parsed_args(main_parser, parsed_args)
    table_name = parsed_args['table-name']

    if not client.check_table_existed(table_name):
        main_parser.error(f"Table '{table_name}' does not exist. Please create table first.")

    input_file_path = main_parser.check_valid_path(parsed_args['input-file'])

    in_format = input_file_path.suffixes[0].replace('.', '') if parsed_args['file_format'] == 'infer' \
        else parsed_args['file_format']

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    converted_file_path = input_file_path.parent / f"{input_file_path.stem}_{now}.ndjson.gz"
    convert_options = {
        'read_input': input_file_path,
        'write_output': converted_file_path,
        'in_format': in_format,
        'out_format': 'ndjson',
        'convert_options': {'compression': 'gzip'},
        'use_table_schema': client.get_table_schema(table_name)
    }
    df_handler = DataFrameHandler()
    df_handler.convert_file(**convert_options)
    client.populate_table_by_file_upload(table_name, converted_file_path)

    if converted_file_path.exists():
        converted_file_path.unlink()
    return 0


def add_database_parser(action_subparsers):
    database_parser = action_subparsers.add_parser('database',
                                                   help='perform operations with your Aito database instance')
    database_parser.formatter_class = argparse.RawTextHelpFormatter
    database_parser.epilog = '''You must provide credentials to execute database operations
If no credential options is given, use the credentials from environment variable by defaults.
The AITO_INSTANCE_URL should look similar to https://my-instance.api.aito.ai

To see help for a specific operation:
  aito database <operation> -h  

Example:
  aito database quick-add-table myTable.csv
  aito database create-table tableName < path/to/tableSchema
  aito database -e path/to/myCredentials.env upload-file myTable path/to/myFile
  aito database -u MY_AITO_INSTANCE_URL -r MY_RO_KEY -w MY_RW_KEY upload-batch myTable < path/to/myTableEntries
    '''
    credential_args = database_parser.add_argument_group("optional credential arguments")
    credential_args.add_argument('-e', '--use-env-file', type=str, metavar='env-input-file',
                                 help='set up the client using a .env file containing the required env variables')
    credential_args.add_argument('-r', '--read-only-key', type=str, default='.env',
                                 help='specify aito read-only API key')
    credential_args.add_argument('-u', '--url', type=str, default='.env', help='specify aito instance url')
    credential_args.add_argument('-w', '--read-write-key', type=str, default='.env',
                                 help='specify aito read-write API key')

    operation_subparsers = database_parser.add_subparsers(title="operation",
                                                          description="operation to perform",
                                                          parser_class=AitoArgParser,
                                                          dest="operation",
                                                          metavar="<operation>")
    operation_subparsers.required=True

    add_quick_add_table_parser(operation_subparsers)
    add_create_table_parser(operation_subparsers)
    add_delete_table_parser(operation_subparsers)
    add_delete_database_parser(operation_subparsers)
    add_upload_batch_parser(operation_subparsers)
    add_upload_file_parser(operation_subparsers)


def execute_database_operation(main_parser, parsed_args):
    if parsed_args['operation'] == 'quick-add-table':
        execute_quick_add_table(main_parser, parsed_args)
    elif parsed_args['operation'] == 'create-table':
        execute_create_table(main_parser, parsed_args)
    elif parsed_args['operation'] == 'delete-table':
        execute_delete_table(main_parser, parsed_args)
    elif parsed_args['operation'] == 'delete-database':
        execute_delete_database(main_parser, parsed_args)
    elif parsed_args['operation'] == 'upload-batch':
        execute_upload_batch(main_parser, parsed_args)
    elif parsed_args['operation'] == 'upload-file':
        execute_upload_file(main_parser, parsed_args)

    return 0
