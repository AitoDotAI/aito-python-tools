import argparse
import json
import os
import sys
import tempfile
from pathlib import Path

from dotenv import load_dotenv

from aito.utils.parser import AitoArgParser
from aito.utils.aito_client import AitoClient
from aito.utils.data_frame_handler import DataFrameHandler
from aito.utils.schema_handler import SchemaHandler
from aito.utils.sql_connection import SQLConnection


def create_client_from_parsed_args(main_parser: AitoArgParser, parsed_args):
    if parsed_args['use_env_file']:
        env_file_path = main_parser.parse_path_value(parsed_args['use_env_file'], True)
        load_dotenv(env_file_path)

    env_variables = os.environ
    client_args = {
        'instance_name': env_variables.get('AITO_INSTANCE_NAME') if parsed_args['instance_name'] == '.env'
        else parsed_args['instance_name'],
        'rw_key': env_variables.get('AITO_RW_KEY') if parsed_args['read_write_key'] == '.env'
        else parsed_args['read_write_key'],
        'ro_key': env_variables.get('AITO_RO_KEY') if parsed_args['read_only_key'] == '.env'
        else parsed_args['read_only_key']
    }
    return AitoClient(**client_args)


def create_sql_connecting_from_parsed_args(parsed_args):
    env_variables = os.environ
    args = {
        'typ': parsed_args['database-name'],
        'server': env_variables.get('SERVER') if parsed_args['server'] == '.env' else parsed_args['server'],
        'port': env_variables.get('PORT') if parsed_args['port'] == '.env' else parsed_args['port'],
        'database': env_variables.get('DATABASE') if parsed_args['database'] == '.env' else parsed_args['database'],
        'user': env_variables.get('USER') if parsed_args['user'] == '.env' else parsed_args['user'],
        'pwd': env_variables.get('PASS') if parsed_args['pass'] == '.env' else parsed_args['pass']
    }
    return SQLConnection(**args)


def add_quick_add_table_parser(operation_subparsers):
    parser = operation_subparsers.add_parser(
        'quick-add-table', help="infer schema, create table, and upload a file to the database"
    )
    parser.epilog = '''Example:
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


def execute_quick_add_table(main_parser: AitoArgParser, parsed_args):
    input_file_path = main_parser.parse_path_value(parsed_args['input-file'])
    table_name = parsed_args['table_name'] if parsed_args['table_name'] else input_file_path.stem
    in_format = input_file_path.suffixes[0].replace('.', '') if parsed_args['file_format'] == 'infer' \
        else parsed_args['file_format']

    converted_tmp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson.gz', delete=True)
    convert_options = {
        'read_input': input_file_path,
        'write_output': Path(converted_tmp_file.name),
        'in_format': in_format,
        'out_format': 'ndjson',
        'convert_options': {'compression': 'gzip'}
    }
    df_handler = DataFrameHandler()
    converted_df = df_handler.convert_file(**convert_options)
    converted_tmp_file.seek(0)

    schema_handler = SchemaHandler()
    inferred_schema = schema_handler.infer_table_schema_from_pandas_dataframe(converted_df)

    client = create_client_from_parsed_args(main_parser, parsed_args)
    client.put_table_schema(table_name, inferred_schema)
    client.populate_table_by_file_upload(table_name, Path(converted_tmp_file.name))
    converted_tmp_file.close()
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


def execute_create_table(main_parser: AitoArgParser, parsed_args):
    client = create_client_from_parsed_args(main_parser, parsed_args)
    table_name = parsed_args['table-name']
    if parsed_args['schema-input'] == '-':
        table_schema = json.load(sys.stdin)
    else:
        input_path = main_parser.parse_path_value(parsed_args['schema-input'])
        with input_path.open() as f:
            table_schema = json.load(f)
    client.put_table_schema(table_name, table_schema)
    return 0


def add_delete_table_parser(operation_subparsers):
    parser = operation_subparsers.add_parser('delete-table',
                                             help="delete a table schema and all content inside the table")
    parser.add_argument('table-name', type=str, help="name of the table to be deleted")
    return parser


def execute_delete_table(main_parser: AitoArgParser, parsed_args):
    client = create_client_from_parsed_args(main_parser, parsed_args)
    table_name = parsed_args['table-name']
    if main_parser.ask_confirmation(f"Confirm delete table '{table_name}'? The action is irreversible", False):
        client.delete_table(table_name)
    return 0


def add_delete_database_parser(operation_subparsers):
    parser = operation_subparsers.add_parser('delete-database', help='delete the whole database')
    return parser


def execute_delete_database(main_parser: AitoArgParser, parsed_args):
    client = create_client_from_parsed_args(main_parser, parsed_args)
    if main_parser.ask_confirmation(f"Confirm delete the whole database? The action is irreversible", False):
        client.delete_database()
    return 0


def add_upload_batch_parser(operation_subparsers):
    parser = operation_subparsers.add_parser('upload-batch', help='populate table entries to an existing table')
    parser.epilog = '''With no input, or when input is -, read table content from standard input
Example:
  aito client upload-batch myTable path/to/myTableEntries.json
  aito client upload-batch myTable < path/to/myTableEntries.json
  '''
    parser.add_argument('table-name', type=str, help='name of the table to be populated')
    parser.add_argument('input', default='-', type=str, nargs='?', help='input file or stream in JSON array format')


def execute_upload_batch(main_parser: AitoArgParser, parsed_args):
    client = create_client_from_parsed_args(main_parser, parsed_args)
    table_name = parsed_args['table-name']
    if parsed_args['input'] == '-':
        table_content = json.load(sys.stdin)
    else:
        input_path = main_parser.parse_path_value(parsed_args['input'])
        with input_path.open() as f:
            table_content = json.load(f)
    client.populate_table_entries(table_name, table_content)
    return 0


def add_upload_file_parser(operation_subparsers):
    parser = operation_subparsers.add_parser('upload-file', help='populate a file to an existing table')
    parser.epilog = '''Example:
  aito client upload-file tableName path/to/myFile.csv
  aito client upload-file -f csv tableName path/to/myFile.txt
  '''

    parser.add_argument('table-name', type=str, help='name of the table to be populated')
    parser.add_argument('input-file', type=str, help="path to the input file")
    file_format_choices = ['infer'] + DataFrameHandler.allowed_format
    parser.add_argument('-f', '--file-format', type=str, choices=file_format_choices,
                        default='infer', help='specify input file format (default: infer from the file extension)')


def execute_upload_file(main_parser: AitoArgParser, parsed_args):
    client = create_client_from_parsed_args(main_parser, parsed_args)
    table_name = parsed_args['table-name']

    if not client.check_table_existed(table_name):
        main_parser.error(f"Table '{table_name}' does not exist. Please create table first.")

    input_file_path = main_parser.parse_path_value(parsed_args['input-file'])

    in_format = input_file_path.suffixes[0].replace('.', '') if parsed_args['file_format'] == 'infer' \
        else parsed_args['file_format']

    converted_tmp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson.gz', delete=True)
    convert_options = {
        'read_input': input_file_path,
        'write_output': Path(converted_tmp_file.name),
        'in_format': in_format,
        'out_format': 'ndjson',
        'convert_options': {'compression': 'gzip'},
        'use_table_schema': client.get_table_schema(table_name)
    }
    df_handler = DataFrameHandler()
    df_handler.convert_file(**convert_options)
    converted_tmp_file.seek(0)
    client.populate_table_by_file_upload(table_name, Path(converted_tmp_file.name))
    converted_tmp_file.close()
    return 0


def add_upload_data_from_sql_parser(operation_subparsers):
    parser = operation_subparsers.add_parser('upload-data-from-sql',
                                             help="populate data from the result of a SQL query to an existing table")
    parser.add_argument('database-name', type=str, choices=['postgres'], help='database name')
    parser.add_argument('table-name', type=str, help='name of the table to be populated')
    parser.add_argument('query', type=str, help='query to get the data from your database')
    parser.add_argument('--server', '-s', type=str, help='server to connect to', default='.env')
    parser.add_argument('--port', '-P', type=str, help='port to connect to', default='.env')
    parser.add_argument('--database', '-d', type=str, help='database to connect to', default='.env')
    parser.add_argument('--user', '-u', type=str, help='username for authentication', default='.env')
    parser.add_argument('--pass', '-p', type=str, help='password for authentication', default='.env')
    parser.epilog = '''Each database requires different odbc driver. Please refer to our docs for more info.
If no credential options is given, the following environment variable is used to connect to your SQL database:
  SERVER, PORT, DATABASE, USER, PASS          
  '''


def execute_upload_data_from_sql(main_parser: AitoArgParser, parsed_args):
    table_name = parsed_args['table-name']

    connection = create_sql_connecting_from_parsed_args(parsed_args)
    result_df = connection.execute_query_and_save_result(parsed_args['query'])
    dataframe_handler = DataFrameHandler()

    converted_tmp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson.gz', delete=True)
    dataframe_handler.df_to_format(result_df, 'ndjson', converted_tmp_file.name, {'compression': 'gzip'})

    client = create_client_from_parsed_args(main_parser, parsed_args)
    converted_tmp_file.seek(0)
    client.populate_table_by_file_upload(table_name, Path(converted_tmp_file.name))
    converted_tmp_file.close()
    return 0


def add_quick_add_table_from_sql_parser(operation_subparsers):
    parser = operation_subparsers.add_parser(
        'quick-add-table-from-sql',
        help="infer schema, create table, and upload the result of a SQL to the database"
    )
    parser.add_argument('database-name', type=str, choices=['postgres'], help='database name')
    parser.add_argument('table-name', type=str, help='name of the table to be populated')
    parser.add_argument('query', type=str, help='query to get the data from your database')
    parser.add_argument('--server', '-s', type=str, help='server to connect to', default='.env')
    parser.add_argument('--port', '-P', type=str, help='port to connect to', default='.env')
    parser.add_argument('--database', '-d', type=str, help='database to connect to', default='.env')
    parser.add_argument('--user', '-u', type=str, help='username for authentication', default='.env')
    parser.add_argument('--pass', '-p', type=str, help='password for authentication', default='.env')
    parser.epilog = '''Each database requires different odbc driver. Please refer to our docs for more info.
If no credential options is given, the following environment variable is used to connect to your SQL database:
  SERVER, PORT, DATABASE, USER, PASS          
  '''


def execute_quick_add_table_from_sql(main_parser: AitoArgParser, parsed_args):
    table_name = parsed_args['table-name']

    connection = create_sql_connecting_from_parsed_args(parsed_args)
    result_df = connection.execute_query_and_save_result(parsed_args['query'])

    schema_handler = SchemaHandler()
    inferred_schema = schema_handler.infer_table_schema_from_pandas_dataframe(result_df)

    dataframe_handler = DataFrameHandler()
    converted_tmp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson.gz', delete=True)
    dataframe_handler.df_to_format(result_df, 'ndjson', converted_tmp_file.name, {'compression': 'gzip'})
    converted_tmp_file.seek(0)

    client = create_client_from_parsed_args(main_parser, parsed_args)
    client.put_table_schema(table_name, inferred_schema)
    client.populate_table_by_file_upload(table_name, Path(converted_tmp_file.name))
    converted_tmp_file.close()
    return 0


def add_database_parser(action_subparsers):
    database_parser = action_subparsers.add_parser('database',
                                                   help='perform operations with your Aito database instance')
    database_parser.formatter_class = argparse.RawTextHelpFormatter
    database_parser.epilog = '''You must provide your Aito credentials to execute database operations
If no credential options is given, the following environment variable is used to connect to your Aito database:
  AITO_INSTANCE_NAME=your-instance-name
  AITO_RW_KEY=your read-write api key
  AITO_RO_KEY=your read-only key (optional)

To see help for a specific operation:
  aito database <operation> -h  

Example:
  aito database quick-add-table myTable.csv
  aito database create-table tableName < path/to/tableSchema
  aito database -e path/to/myCredentials.env upload-file myTable path/to/myFile
  aito database -i MY_INSTANCE_NAME -r MY_RO_KEY -w MY_RW_KEY upload-batch myTable < path/to/myTableEntries
  aito database quick-add-table-from-sql postgres tableName 'SELECT * FROM tableName;'
    '''
    credential_args = database_parser.add_argument_group("optional credential arguments")
    credential_args.add_argument('-e', '--use-env-file', type=str, metavar='env-input-file',
                                 help='set up the credentials using a .env file containing the required env variables')
    credential_args.add_argument('-r', '--read-only-key', type=str, default='.env',
                                 help='specify aito read-only API key')
    credential_args.add_argument('-i', '--instance-name', type=str, default='.env', help='specify aito instance name')
    credential_args.add_argument('-w', '--read-write-key', type=str, default='.env',
                                 help='specify aito read-write API key')

    operation_subparsers = database_parser.add_subparsers(title="operation",
                                                          description="operation to perform",
                                                          parser_class=AitoArgParser,
                                                          dest="operation",
                                                          metavar="<operation>")
    operation_subparsers.required = True

    add_quick_add_table_parser(operation_subparsers)
    add_create_table_parser(operation_subparsers)
    add_delete_table_parser(operation_subparsers)
    add_delete_database_parser(operation_subparsers)
    add_upload_batch_parser(operation_subparsers)
    add_upload_file_parser(operation_subparsers)
    add_upload_data_from_sql_parser(operation_subparsers)
    add_quick_add_table_from_sql_parser(operation_subparsers)


def execute_database_operation(main_parser: AitoArgParser, parsed_args):
    if parsed_args['operation'] == 'quick-add-table':
        return execute_quick_add_table(main_parser, parsed_args)
    elif parsed_args['operation'] == 'create-table':
        return execute_create_table(main_parser, parsed_args)
    elif parsed_args['operation'] == 'delete-table':
        return execute_delete_table(main_parser, parsed_args)
    elif parsed_args['operation'] == 'delete-database':
        return execute_delete_database(main_parser, parsed_args)
    elif parsed_args['operation'] == 'upload-batch':
        return execute_upload_batch(main_parser, parsed_args)
    elif parsed_args['operation'] == 'upload-file':
        return execute_upload_file(main_parser, parsed_args)
    elif parsed_args['operation'] == 'upload-data-from-sql':
        return execute_upload_data_from_sql(main_parser, parsed_args)
    elif parsed_args['operation'] == 'quick-add-table-from-sql':
        return execute_quick_add_table_from_sql(main_parser, parsed_args)
