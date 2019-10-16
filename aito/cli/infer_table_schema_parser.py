import argparse
import json
import sys

from aito.cli.parser import AitoArgParser
from aito.data_frame_handler import DataFrameHandler
from aito.schema_handler import SchemaHandler


def add_specific_format_parser(format_sub_parser, format_name: str):
    format_parser = format_sub_parser.add_parser(format_name)
    format_parser.usage = f"aito infer-table-schema [-h] {format_name} ... [input]"
    format_parser.formatter_class = argparse.RawTextHelpFormatter
    # add share arguments between formats
    format_parser.add_argument('-e', '--encoding', type=str, default='utf-8', help="encoding to use (default: 'utf-8')")
    format_parser.add_argument('input', default='-', type=str, nargs='?',
                               help="input file or stream (with no input or when input is -, read standard input)")
    return format_parser


def add_infer_table_schema_parser(action_sub_parser: argparse._SubParsersAction):
    parser = action_sub_parser.add_parser('infer-table-schema', help='infer Aito table schema from a file')
    parser.formatter_class = argparse.RawTextHelpFormatter
    parser.usage = '''aito infer-table-schema [-h] <file-format> ... [input]
When no input or when input is -, read standard input
It is recommended to use input file, especially for excel format

To see help for a specific file format:
  aito infer-table-schema <file-format> -h
'''
    parser.epilog = '''example:
  aito infer-table-schema json myFile.json > inferredSchema.json
  aito infer-table-schema csv myFile.csv > inferredSchema.json
  aito infer-table-schema excel < myFile.xlsx > inferredSchema.json
  '''

    format_sub_parser = parser.add_subparsers(title='file-format', description='infer from a specific format',
                                              dest='file-format')
    format_sub_parser.required = True

    infer_from_csv_parser = add_specific_format_parser(format_sub_parser, 'csv')
    infer_from_csv_parser.add_argument('-d', '--delimiter', type=str, default=',',
                                       help="delimiter to use. Need escape (default: ',')")
    infer_from_csv_parser.add_argument('-p', '--decimal', type=str, default='.',
                                       help="Character to recognize decimal point (default '.')")
    infer_from_csv_parser.epilog = '''example:
  aito infer-table-schema csv myFile.csv
  aito infer-table-schema csv -d ';' < mySemicolonDelimiterFile.csv > inferredSchema.json
  '''

    infer_from_excel_parser = add_specific_format_parser(format_sub_parser, 'excel')
    infer_from_excel_parser.description = 'Infer table schema from an excel fie, accept both xls and xlsx. ' \
                                          'Read the first sheet of the file by default'
    infer_from_excel_parser.add_argument('-o', '--one-sheet', type=str, metavar='sheet-name',
                                         help='read a sheet of the excel file')
    infer_from_excel_parser.epilog = '''example:
  aito infer-table-schema excel ./myFile.xls
  aito infer-table-schema excel -o firstSheet < myFile.xslx
  '''

    add_specific_format_parser(format_sub_parser, 'json')

    add_specific_format_parser(format_sub_parser, 'ndjson')


def execute_infer_table_schema(parser: AitoArgParser, parsed_args):
    in_format = parsed_args['file-format']
    read_args = {
        'read_input': parser.parse_input_arg_value(parsed_args['input']),
        'in_format': in_format,
        'read_options': {
            'encoding': parsed_args['encoding']
        }
    }

    if in_format == 'csv':
        read_args['read_options']['delimiter'] = parsed_args['delimiter']
        read_args['read_options']['decimal'] = parsed_args['decimal']
    elif in_format == 'excel':
        if parsed_args['one_sheet']:
            read_args['read_options']['sheet_name'] = parsed_args['one_sheet']

    df = DataFrameHandler().read_file_to_df(**read_args)
    inferred_schema = SchemaHandler().generate_table_schema_from_pandas_dataframe(df)
    json.dump(inferred_schema, sys.stdout, indent=4, sort_keys=True)


# class InferTableSchemaParserWrapper(ParserWrapper):
#     def __init__(self):
#         super().__init__(add_help=False)
#         parser = self.parser
#         parser.description = 'infer Aito table schema from a file'
#         parser.usage = ''' aito infer-table-schema [-h] <file-format> [input] [<options>]
#                 To see help for a specific input format:
#                     aito convert <file-format> -h
#                 '''
#         parser.epilog = '''example:
#                 aito infer-table-schema json myFile.json > inferredSchema.json
#                 aito infer-table-schema csv myFile.csv > inferredSchema.json
#                 aito infer-table-schema excel < myFile.xlsx > inferredSchema.json
#                 '''
#         self.file_format_to_parser = {
#             'csv': InferCsvFileParserWrapper,
#             'excel': InferExcelFileParserWrapper,
#             'json': InferJsonFileParserWrapper,
#             'ndjson': InferNdJsonFileParserWrapper,
#         }
#         self.file_format_arg = parser.add_argument('file-format', choices=list(self.file_format_to_parser.keys()),
#                                                     help='input file format')
#
#     def parse_and_execute(self, parsing_args) -> int:
#         parsed_args, unknown = self.parser.parse_known_args(parsing_args)
#         parsed_args = vars(parsed_args)
#         file_format_parser = self.file_format_to_parser[parsed_args['file-format']](self.parser,
#                                                                                        self.file_format_arg)
#         file_format_parser.parse_and_execute(parsing_args)
#         return 0

#
# class InferFromFormatParserWrapper:
#     def __init__(self, parent_parser: AitoArgParser, file_format_arg, file_format: str):
#         self.df_handler = DataFrameHandler()
#         self.schema_handler = SchemaHandler()
#         self.file_format = file_format
#         file_format_arg.help = argparse.SUPPRESS
#         self.parser = AitoArgParser(formatter_class=argparse.RawTextHelpFormatter,
#                                     parents=[parent_parser],
#                                     usage=f"aito infer-table-schema {file_format} [input] [<options>]")
#         parser = self.parser
#
#         parser.add_argument('-e', '--encoding', type=str, default='utf-8',
#                             help="encoding to use (default: 'utf-8')")
#         parser.add_argument('input', default='-', type=str, nargs='?',
#                             help="input file or stream (with no input or when input is -, read standard input) "
#                                  "\nit is recommended to use input file, especially for excel format")
#
#     def get_read_args_from_parsed_args(self, parsed_args):
#         parser = self.parser
#         read_args = {
#             'read_input': parser.parse_input_arg_value(parsed_args['input']),
#             'in_format': parsed_args['file-format'],
#             'read_options': {
#                 'encoding': parsed_args['encoding']
#             }
#         }
#         return read_args
#
#     def read_df_create_schema(self, read_args):
#
#     @abstractmethod
#     def parse_and_execute(self, parsing_args):
#         pass
#
#
# class InferCsvFileParserWrapper(InferFromFormatParserWrapper):
#     def __init__(self, parent_parser: AitoArgParser, file_format_arg):
#         super().__init__(parent_parser, file_format_arg, 'csv')
#         parser = self.parser
#         parser.add_argument('-d', '--delimiter', type=str, default=',',
#                             help="delimiter to use. Need escape (default: ',')")
#         parser.add_argument('-p', '--decimal', type=str, default='.',
#                             help="Character to recognize decimal point (default '.')")
#         parser.epilog = '''example:
#             aito infer-table-schema csv myFile.csv
#             aito infer-table-schema csv -d ';' < mySemicolonDelimiterFile.csv > inferredSchema.json
#             '''
#
#     def parse_and_execute(self, parsing_args) -> int:
#         parsed_args = vars(self.parser.parse_args(parsing_args))
#         read_args = self.get_read_args_from_parsed_args(parsed_args)
#
#         self.read_df_create_schema(read_args)
#         return 0
#
#
# class InferJsonFileParserWrapper(InferFromFormatParserWrapper):
#     def __init__(self, parent_parser: AitoArgParser, file_format_arg):
#         super().__init__(parent_parser, file_format_arg, 'json')
#
#     def parse_and_execute(self, parsing_args) -> int:
#         parsed_args = vars(self.parser.parse_args(parsing_args))
#         read_args = self.get_read_args_from_parsed_args(parsed_args)
#         self.read_df_create_schema(read_args)
#         return 0
#
#
# class InferNdJsonFileParserWrapper(InferFromFormatParserWrapper):
#     def __init__(self, parent_parser: AitoArgParser, file_format_arg):
#         super().__init__(parent_parser, file_format_arg, 'ndjson')
#
#     def parse_and_execute(self, parsing_args) -> int:
#         parsed_args = vars(self.parser.parse_args(parsing_args))
#         read_args = self.get_read_args_from_parsed_args(parsed_args)
#         self.read_df_create_schema(read_args)
#         return 0
#
#
# class InferExcelFileParserWrapper(InferFromFormatParserWrapper):
#     def __init__(self, parent_parser: AitoArgParser, file_format_arg):
#         super().__init__(parent_parser, file_format_arg, 'excel')
#         self.parser.description = 'Create table schema from an excel file, accept both xls and xlsx. ' \
#                                   'Read the first sheet of the file by default'
#         self.parser.add_argument('-o', '--one-sheet', type=str, metavar='sheet-name',
#                                  help='read a sheet of the excel file')
#         self.parser.epilog = '''example:
#             aito infer-table-schema excel ./myFile.xls
#             aito infer-table-schema excel -o firstSheet < myFile.xslx
#             '''
#
#     def parse_and_execute(self, parsing_args) -> int:
#         parsed_args = vars(self.parser.parse_args(parsing_args))
#         read_args = self.get_read_args_from_parsed_args(parsed_args)
#         if parsed_args['one_sheet']:
#             read_args['read_options']['sheet_name'] = parsed_args['one_sheet']
#         self.read_df_create_schema(read_args)
#         return 0
