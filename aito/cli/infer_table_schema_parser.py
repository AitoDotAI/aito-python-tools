import argparse
import json
import sys

from aito.cli.parser import AitoArgParser
from aito.utils.data_frame_handler import DataFrameHandler
from aito.utils.schema_handler import SchemaHandler


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


def add_infer_table_schema_parser(action_subparsers):
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
    format_sub_parsers.required=True

    add_infer_csv_parser(format_sub_parsers)
    add_infer_excel_parser(format_sub_parsers)
    add_infer_json_parser(format_sub_parsers)
    add_infer_ndjson_parser(format_sub_parsers)


def execute_infer_table_schema(main_parser, parsed_args):
    in_format = parsed_args['input-format']
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
    inferred_schema = SchemaHandler().generate_table_schema_from_pandas_dataframe(df)
    json.dump(inferred_schema, sys.stdout, indent=4, sort_keys=True)
    return 0