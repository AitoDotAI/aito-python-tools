import argparse
import json
import sys

from aito.utils.data_frame_handler import DataFrameHandler
from aito.utils.parser import AitoArgParser
from aito.utils.schema_handler import SchemaHandler


def add_convert_format_parser(format_subparsers, format_name):
    format_parser = format_subparsers.add_parser(format_name,
                                                 help=f"convert a {format_name} into ndjson|json format")
    format_parser.formatter_class = argparse.RawTextHelpFormatter
    # add share arguments between formats
    either_use_or_create_schema = format_parser.add_mutually_exclusive_group()
    either_use_or_create_schema.add_argument('-c', '--create-table-schema', metavar='schema-output-file',
                                             type=str,
                                             help='create an inferred aito schema and write to output file')
    either_use_or_create_schema.add_argument('-s', '--use-table-schema', metavar='schema-input-file', type=str,
                                             help='convert the data to match the input table schema')
    format_parser.add_argument('-e', '--encoding', type=str, default='utf-8',
                               help="encoding to use (default: 'utf-8')")
    format_parser.add_argument('-j', '--json', action='store_true', help='convert to json format')

    format_parser.add_argument('input', default='-', type=str, nargs='?',
                               help="input file (when no input or when input is -, read standard input)")
    return format_parser


def add_convert_csv_parser(format_subparsers):
    parser = add_convert_format_parser(format_subparsers, 'csv')
    parser.add_argument('-d', '--delimiter', type=str, default=',', help="delimiter to use (default: ',')")
    parser.add_argument('-p', '--decimal', type=str, default='.',
                        help="character to recognize decimal point (default '.')")
    parser.epilog = '''Example:
  aito convert csv myFile.csv
  aito convert csv -d ';' --json < mySemicolonDelimiterFile.csv > convertedFile.json
  '''
    return parser


def add_convert_excel_parser(format_subparsers):
    parser = add_convert_format_parser(format_subparsers, 'excel')
    parser.description = 'Convert excel format input, accept both xls and xlsx. ' \
                         'Read the first sheet of the file by default'
    parser.add_argument('-o', '--one-sheet', type=str, metavar='sheet-name', help='read a sheet of the excel file')
    parser.epilog = '''Example:
    aito convert excel ./myFile.xls
    aito convert excel -o firstSheet myMultipleSheetsFile.xslx
    '''
    return parser


def add_convert_json_parser(format_subparsers):
    parser = add_convert_format_parser(format_subparsers, 'json')
    return parser


def add_convert_ndjson_parser(format_subparsers):
    parser = add_convert_format_parser(format_subparsers, 'ndjson')
    return parser


def add_convert_parser(action_subparsers):
    """
    :param action_subparsers: Action subparsers from the main parser
    :return:
    """
    convert_parser = action_subparsers.add_parser('convert', help='convert a file into ndjson|json format')
    convert_parser.formatter_class = argparse.RawTextHelpFormatter
    convert_parser.epilog = '''To see help for a specific format:
  aito convert <input-format> - h

When no input or when input is -, read standard input. 
You must use input file instead of standard input for excel file

Example:
  aito convert json myFile.json > convertedFile.ndjson
  aito convert csv -c myInferredTableSchema.json --json myFile.csv > convertedFile.json
  aito convert excel -s desiredSchema.json < myFile.xlsx > convertedFile.ndjson
  '''

    format_subparsers = convert_parser.add_subparsers(title='input-format',
                                                      description='convert from a specific format',
                                                      parser_class=AitoArgParser,
                                                      dest='input-format',
                                                      metavar='<input-format>')
    format_subparsers.required = True
    add_convert_csv_parser(format_subparsers)
    add_convert_excel_parser(format_subparsers)
    add_convert_json_parser(format_subparsers)
    add_convert_ndjson_parser(format_subparsers)


def execute_convert(main_parser: AitoArgParser, parsed_args):
    in_format = parsed_args['input-format']
    convert_args = {
        'read_input': main_parser.parse_input_arg_value(parsed_args['input']),
        'write_output': sys.stdout,
        'in_format': parsed_args['input-format'],
        'out_format': 'json' if parsed_args['json'] else 'ndjson',
        'read_options': {
            'encoding': parsed_args['encoding']
        },
        'convert_options': {},
    }

    schema_handler = SchemaHandler()
    if parsed_args['use_table_schema']:
        schema_path = main_parser.check_valid_path(parsed_args['use_table_schema'], check_exists=True)
        with schema_path.open() as f:
            table_schema = json.load(f)
        schema_handler.validate_table_schema(table_schema)
        convert_args['use_table_schema'] = table_schema

    if in_format == 'csv':
        convert_args['read_options']['delimiter'] = parsed_args['delimiter']
        convert_args['read_options']['decimal'] = parsed_args['decimal']

    if in_format == 'excel':
        if parsed_args['input'] == '-':
            main_parser.error('Use file path instead of standard input for excel file')
        if parsed_args['one_sheet']:
            convert_args['read_options']['sheet_name'] = parsed_args['one_sheet']

    converted_df = DataFrameHandler().convert_file(**convert_args)

    if parsed_args['create_table_schema']:
        output_schema_path = main_parser.check_valid_path(parsed_args['create_table_schema'])
        inferred_schema = schema_handler.infer_table_schema_from_pandas_dataframe(converted_df)
        with output_schema_path.open(mode='w') as f:
            json.dump(inferred_schema, f, indent=2, sort_keys=True)

    return 0
