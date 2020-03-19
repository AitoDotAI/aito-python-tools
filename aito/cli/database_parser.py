import argparse
import json
import sys
import tempfile
from os import unlink

from aito.utils.data_frame_handler import DataFrameHandler
from aito.utils.parser import AitoArgParser
from aito.utils.schema_handler import SchemaHandler


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

    converted_tmp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson.gz', delete=False)
    convert_options = {
        'read_input': input_file_path,
        'write_output': converted_tmp_file.name,
        'in_format': in_format,
        'out_format': 'ndjson',
        'convert_options': {'compression': 'gzip'}
    }
    df_handler = DataFrameHandler()
    converted_df = df_handler.convert_file(**convert_options)
    converted_tmp_file.close()

    schema_handler = SchemaHandler()
    inferred_schema = schema_handler.infer_table_schema_from_pandas_data_frame(converted_df)

    client = main_parser.create_client_from_parsed_args(parsed_args)
    client.put_table_schema(table_name, inferred_schema)

    with open(converted_tmp_file.name, 'rb') as in_f:
        client.populate_table_by_file_upload(table_name, in_f)
    converted_tmp_file.close()
    unlink(converted_tmp_file.name)
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
    client = main_parser.create_client_from_parsed_args(parsed_args)
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
    client = main_parser.create_client_from_parsed_args(parsed_args)
    table_name = parsed_args['table-name']
    if main_parser.ask_confirmation(f"Confirm delete table '{table_name}'? The action is irreversible", False):
        client.delete_table(table_name)
    return 0


def add_delete_database_parser(operation_subparsers):
    parser = operation_subparsers.add_parser('delete-database', help='delete the whole database')
    return parser


def execute_delete_database(main_parser: AitoArgParser, parsed_args):
    client = main_parser.create_client_from_parsed_args(parsed_args)
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
    client = main_parser.create_client_from_parsed_args(parsed_args)
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
    client = main_parser.create_client_from_parsed_args(parsed_args)
    table_name = parsed_args['table-name']

    if not client.check_table_exists(table_name):
        main_parser.error(f"Table '{table_name}' does not exist. Please create table first.")

    input_file_path = main_parser.parse_path_value(parsed_args['input-file'])

    in_format = input_file_path.suffixes[0].replace('.', '') if parsed_args['file_format'] == 'infer' \
        else parsed_args['file_format']

    converted_tmp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson.gz', delete=False)
    convert_options = {
        'read_input': input_file_path,
        'write_output': converted_tmp_file.name,
        'in_format': in_format,
        'out_format': 'ndjson',
        'convert_options': {'compression': 'gzip'},
        'use_table_schema': client.get_table_schema(table_name)
    }
    df_handler = DataFrameHandler()
    df_handler.convert_file(**convert_options)
    converted_tmp_file.close()

    with open(converted_tmp_file.name, 'rb') as in_f:
        client.populate_table_by_file_upload(table_name, in_f)
    converted_tmp_file.close()
    unlink(converted_tmp_file.name)
    return 0


def add_upload_data_from_sql_parser(operation_subparsers):
    parser = operation_subparsers.add_parser('upload-data-from-sql',
                                             help="populate data from the result of a SQL query to an existing table")
    parser.add_sql_credentials_arguments_flags(add_use_env_arg=False)
    parser.add_argument('table-name', type=str, help='name of the table to be populated')
    parser.add_argument('query', type=str, help='query to get the data from your database')


def execute_upload_data_from_sql(main_parser: AitoArgParser, parsed_args):
    table_name = parsed_args['table-name']

    connection = main_parser.create_sql_connecting_from_parsed_args(parsed_args)
    result_df = connection.execute_query_and_save_result(parsed_args['query'])
    dataframe_handler = DataFrameHandler()

    converted_tmp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson.gz', delete=False)
    dataframe_handler.df_to_format(result_df, 'ndjson', converted_tmp_file.name, {'compression': 'gzip'})
    converted_tmp_file.close()

    client = main_parser.create_client_from_parsed_args(parsed_args)
    with open(converted_tmp_file.name, 'rb') as in_f:
        client.populate_table_by_file_upload(table_name, in_f)
    converted_tmp_file.close()
    unlink(converted_tmp_file.name)
    return 0


def add_quick_add_table_from_sql_parser(operation_subparsers):
    parser = operation_subparsers.add_parser(
        'quick-add-table-from-sql',
        help="infer schema, create table, and upload the result of a SQL to the database"
    )
    parser.add_sql_credentials_arguments_flags(add_use_env_arg=False)
    parser.add_argument('table-name', type=str, help='name of the table to be populated')
    parser.add_argument('query', type=str, help='query to get the data from your database')


def execute_quick_add_table_from_sql(main_parser: AitoArgParser, parsed_args):
    table_name = parsed_args['table-name']

    connection = main_parser.create_sql_connecting_from_parsed_args(parsed_args)
    result_df = connection.execute_query_and_save_result(parsed_args['query'])

    schema_handler = SchemaHandler()
    inferred_schema = schema_handler.infer_table_schema_from_pandas_data_frame(result_df)

    dataframe_handler = DataFrameHandler()
    converted_tmp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson.gz', delete=False)
    dataframe_handler.df_to_format(result_df, 'ndjson', converted_tmp_file.name, {'compression': 'gzip'})
    converted_tmp_file.close()

    client = main_parser.create_client_from_parsed_args(parsed_args)
    client.put_table_schema(table_name, inferred_schema)

    with open(converted_tmp_file.name, 'rb') as in_f:
        client.populate_table_by_file_upload(table_name, in_f)
    converted_tmp_file.close()
    unlink(converted_tmp_file.name)
    return 0


def add_database_parser(action_subparsers, enable_sql_functions):
    database_parser = action_subparsers.add_parser('database',
                                                   help='perform operations with your Aito database instance')
    database_parser.formatter_class = argparse.RawTextHelpFormatter
    database_parser.add_aito_credentials_arguments_flags(add_use_env_arg=True)
    database_parser.epilog += '''
To see help for a specific operation:
  aito database <operation> -h  

Example:
  aito database quick-add-table myTable.csv
  aito database quick-add-table-from-sql "PostgreSQL Unicode" tableName 'SELECT * FROM tableName;'
  aito database create-table tableName < path/to/tableSchema
  aito database -e path/to/myCredentials.env upload-file myTable path/to/myFile
  aito database -i MY_INSTANCE_NAME -k MY_API_KEY upload-batch myTable < path/to/myTableEntries
  '''
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
    if enable_sql_functions:
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
