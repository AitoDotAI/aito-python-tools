import argparse
import json
import os
import sys

from dotenv import load_dotenv

from aito.utils.data_frame_handler import DataFrameHandler
from aito.utils.parser import AitoArgParser
from aito.utils.schema_handler import SchemaHandler


def create_sql_connecting_from_parsed_args(main_parser: AitoArgParser, parsed_args):
    if parsed_args['use_env_file']:
        env_file_path = main_parser.parse_path_value(parsed_args['use_env_file'], True)
        load_dotenv(env_file_path)

    args = {'typ': parsed_args['database-name']}
    for arg_name in ['server', 'port', 'database', 'user', 'pwd']:
        args[arg_name] = main_parser.parse_env_variable(arg_name.upper()) if parsed_args[arg_name] == '.env' \
            else parsed_args[arg_name]
    from aito.utils.sql_connection import SQLConnection
    return SQLConnection(**args)


def add_infer_format_parser(format_subparsers, format_name):
    format_parser = format_subparsers.add_parser(format_name,
                                                 help=f"infer a table schema from a {format_name} file")
    format_parser.formatter_class = argparse.RawTextHelpFormatter
    # add share arguments between formats
    format_parser.add_argument('-e', '--encoding', type=str, default='utf-8',
                               help="encoding to use (default: 'utf-8')")
    format_parser.add_argument('input', default='-', type=str, nargs='?',
                               help="input file (when no input or when input is -, read standard input)")
    return format_parser


def add_infer_csv_parser(format_subparsers):
    parser = add_infer_format_parser(format_subparsers, 'csv')
    parser.description = 'infer a table schema from an excel file. '
    parser.add_argument('-d', '--delimiter', type=str, default=',',
                        help="delimiter to use. Need escape (default: ',')")
    parser.add_argument('-p', '--decimal', type=str, default='.',
                        help="Character to recognize decimal point (default '.')")
    parser.epilog = '''Example:
  aito infer-table-schema csv myFile.csv
  aito infer-table-schema csv -d ';' < mySemicolonDelimiterFile.csv > inferredSchema.json
  '''
    return parser


def add_infer_excel_parser(format_subparsers):
    parser = add_infer_format_parser(format_subparsers, 'excel')
    parser.description = '''Infer table schema from an excel file, accept both xls and xlsx.
If the file has multiple sheets, read the first sheet by default
    '''
    parser.add_argument('-o', '--one-sheet', type=str, metavar='sheet-name',
                        help='read a sheet by sheet name')
    parser.epilog = '''Example:
  aito infer-table-schema excel ./myFile.xls
  aito infer-table-schema excel -o firstSheet < myFile.xslx
  '''
    return parser


def add_infer_json_parser(format_subparsers):
    parser = add_infer_format_parser(format_subparsers, 'json')
    return parser


def add_infer_ndjson_parser(format_subparsers):
    parser = add_infer_format_parser(format_subparsers, 'ndjson')
    return parser


def add_infer_from_sql(format_subparsers):
    parser = format_subparsers.add_parser('from-sql', help="infer table schema the result of a SQL query")
    parser.add_argument('database-name', type=str, choices=['postgres'], help='database name')
    parser.add_argument('query', type=str, help='query to get the data from your database')
    credential_args = parser.add_argument_group("optional credential arguments")
    credential_args.add_argument('-e', '--use-env-file', type=str, metavar='env-input-file',
                                 help='set up the credentials using a .env file containing the required env variables')
    credential_args.add_argument('--server', '-s', type=str, help='server to connect to', default='.env')
    credential_args.add_argument('--port', '-P', type=str, help='port to connect to', default='.env')
    credential_args.add_argument('--database', '-d', type=str, help='database to connect to', default='.env')
    credential_args.add_argument('--user', '-u', type=str, help='username for authentication', default='.env')
    credential_args.add_argument('--pwd', '-p', type=str, help='password for authentication', default='.env')
    parser.epilog = '''Each database requires different odbc driver. Please refer to our docs for more info.
If no credential options is given, the following environment variable is used to connect to your SQL database:
  SERVER, PORT, DATABASE, USER, PWD          
  '''


def execute_infer_from_sql(main_parser: AitoArgParser, parsed_args):
    connection = create_sql_connecting_from_parsed_args(main_parser, parsed_args)
    result_df = connection.execute_query_and_save_result(parsed_args['query'])
    inferred_schema = SchemaHandler().infer_table_schema_from_pandas_dataframe(result_df)
    json.dump(inferred_schema, sys.stdout, indent=4, sort_keys=True)
    return 0


def add_infer_table_schema_parser(action_subparsers, enable_sql_functions):
    """
    :param action_subparsers: Action subparsers from the main parser
    :return:
    """
    infer_parser = action_subparsers.add_parser('infer-table-schema', help='infer Aito table schema from a file')
    infer_parser.formatter_class = argparse.RawTextHelpFormatter
    infer_parser.epilog = '''To see help for a specific format:
  aito infer-table-schema <input-format> - h

When no input or when input is -, read standard input.
You must use input file instead of standard input for excel file

Example:
  aito infer-table-schema json myFile.json > inferredSchema.json
  aito infer-table-schema csv myFile.csv > inferredSchema.json
  aito infer-table-schema excel < myFile.xlsx > inferredSchema.json
  '''

    format_sub_parsers = infer_parser.add_subparsers(title='input-format',
                                                     description='infer from a specific format',
                                                     parser_class=AitoArgParser,
                                                     dest='input-format',
                                                     metavar="<input-format>")
    format_sub_parsers.required = True

    add_infer_csv_parser(format_sub_parsers)
    add_infer_excel_parser(format_sub_parsers)
    add_infer_json_parser(format_sub_parsers)
    add_infer_ndjson_parser(format_sub_parsers)
    if enable_sql_functions:
        add_infer_from_sql(format_sub_parsers)


def execute_infer_table_schema(main_parser: AitoArgParser, parsed_args):
    in_format = parsed_args['input-format']
    if in_format == 'from-sql':
        return execute_infer_from_sql(main_parser, parsed_args)

    read_args = {
        'read_input': main_parser.parse_input_arg_value(parsed_args['input']),
        'in_format': in_format,
        'read_options': {
            'encoding': parsed_args['encoding']
        }
    }

    if in_format == 'csv':
        read_args['read_options']['delimiter'] = parsed_args['delimiter']
        read_args['read_options']['decimal'] = parsed_args['decimal']
    elif in_format == 'excel':
        if parsed_args['input'] == '-':
            main_parser.error('Use file path instead of standard input for excel file')
        if parsed_args['one_sheet']:
            read_args['read_options']['sheet_name'] = parsed_args['one_sheet']

    df = DataFrameHandler().read_file_to_df(**read_args)
    inferred_schema = SchemaHandler().infer_table_schema_from_pandas_dataframe(df)
    json.dump(inferred_schema, sys.stdout, indent=4, sort_keys=True)
    return 0
