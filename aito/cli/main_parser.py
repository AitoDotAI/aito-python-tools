import argparse
import json
import logging
import sys

import argcomplete

from aito.cli.parser import AitoArgParser
from aito.data_frame_handler import DataFrameHandler
from aito.schema_handler import SchemaHandler


class MainParser:
    def __init__(self):
        self.parser = AitoArgParser(formatter_class=argparse.RawTextHelpFormatter)
        self.action_subparsers = self.parser.add_subparsers(title='action', description='action to perform', dest='action',
                                                       parser_class=AitoArgParser)
        self.action_subparsers.required = True

        self.add_infer_table_schema_parser()
        self.add_convert_parser()

    def add_infer_table_schema_parser(self):
        def add_format_parser(format_name):
            format_parser = format_subparsers.add_parser(format_name)
            format_parser.usage = f"aito infer-table-schema {format_name} [-h] ... [input]"
            format_parser.formatter_class = argparse.RawTextHelpFormatter
            # add share arguments between formats
            format_parser.add_argument('-e', '--encoding', type=str, default='utf-8',
                                       help="encoding to use (default: 'utf-8')")
            format_parser.add_argument('input', default='-', type=str, nargs='?',
                                       help="input file (when no input or when input is -, read standard input)")
            return format_parser

        infer_parser = self.action_subparsers.add_parser('infer-table-schema',
                                                         help='infer Aito table schema from a file')
        infer_parser.formatter_class = argparse.RawTextHelpFormatter
        infer_parser.usage = '''aito infer-table-schema [-h] <input-format> ... [input]
  When no input or when input is -, read standard input
  It is recommended to use input file, especially for excel format
  
  To see help for a specific file format:
    aito infer-table-schema <input-format> -h
    '''
        infer_parser.epilog = '''example:
  aito infer-table-schema json myFile.json > inferredSchema.json
  aito infer-table-schema csv myFile.csv > inferredSchema.json
  aito infer-table-schema excel < myFile.xlsx > inferredSchema.json
  '''

        format_subparsers = infer_parser.add_subparsers(title='input-format',
                                                        description='infer from a specific format', dest='input-format')
        format_subparsers.required = True

        infer_from_csv_parser = add_format_parser('csv')
        infer_from_csv_parser.add_argument('-d', '--delimiter', type=str, default=',',
                                           help="delimiter to use. Need escape (default: ',')")
        infer_from_csv_parser.add_argument('-p', '--decimal', type=str, default='.',
                                           help="Character to recognize decimal point (default '.')")
        infer_from_csv_parser.epilog = '''example:
  aito infer-table-schema csv myFile.csv
  aito infer-table-schema csv -d ';' < mySemicolonDelimiterFile.csv > inferredSchema.json
  '''

        infer_from_excel_parser = add_format_parser('excel')
        infer_from_excel_parser.description = 'Infer table schema from an excel fie, accept both xls and xlsx. ' \
                                              'Read the first sheet of the file by default'
        infer_from_excel_parser.add_argument('-o', '--one-sheet', type=str, metavar='sheet-name',
                                             help='read a sheet of the excel file')
        infer_from_excel_parser.epilog = '''example:
  aito infer-table-schema excel ./myFile.xls
  aito infer-table-schema excel -o firstSheet < myFile.xslx
  '''

        add_format_parser('json')

        add_format_parser('ndjson')

    def add_convert_parser(self):
        def add_format_parser(format_name):
            format_parser = format_subparsers.add_parser(format_name)
            format_parser.usage = f"aito convert {format_name} [-h] ... [input]"
            format_parser.formatter_class = argparse.RawTextHelpFormatter
            # add share arguments between formats
            either_use_or_create_schema = format_parser.add_mutually_exclusive_group()
            either_use_or_create_schema.add_argument('-c', '--create-table-schema', metavar='schema-output-file',
                                                     type=str,
                                                     help='create an inferred aito schema and write to a json file')
            format_parser.add_argument('-e', '--encoding', type=str, default='utf-8',
                                       help="encoding to use (default: 'utf-8')")
            format_parser.add_argument('-j', '--json', action='store_true', help='convert to json format')
            either_use_or_create_schema.add_argument('-s', '--use-table-schema', metavar='schema-input-file', type=str,
                                                     help='convert the data to match the input table schema')
            format_parser.add_argument('input', default='-', type=str, nargs='?',
                                       help="input file (when no input or when input is -, read standard input)"
                                            "\nfor excel format, it is recommended to use input file")
            return format_parser

        convert_parser = self.action_subparsers.add_parser('convert', help='convert a file into json or ndjson format')
        convert_parser.usage = ''' aito convert [-h] <input-format> [input] [<options>]
  To see help for a specific input format:
    aito convert <input-format> -h
    '''
        convert_parser.epilog = '''example:
  aito convert json myFile.json > convertedFile.ndjson
  aito convert csv myFile.csv -c myInferredTableSchema.json --json > convertedFile.json
  aito convert excel -s desiredSchema.json < myFile.xlsx > convertedFile.ndjson
  '''
        format_subparsers = convert_parser.add_subparsers(title='input-format', help='convert from a specific format',
                                                          dest='input-format')
        format_subparsers.required = True

        convert_csv_parser = add_format_parser('csv')
        convert_csv_parser.add_argument('-d', '--delimiter', type=str, default=',',
                                        help="delimiter to use (default: ',')")
        convert_csv_parser.add_argument('-p', '--decimal', type=str, default='.',
                                        help="character to recognize decimal point (default '.')")
        convert_csv_parser.epilog = '''example:
  aito convert csv myFile.csv
  aito convert csv -d ';' --json < mySemicolonDelimiterFile.csv > convertedFile.json
  '''

        convert_excel_parser = add_format_parser('excel')
        convert_excel_parser.description = 'Convert excel format input, accept both xls and xlsx. ' \
                                           'Read the first sheet of the file by default'
        convert_excel_parser.add_argument('-o', '--one-sheet', type=str, metavar='sheet-name',
                                          help='read a sheet of the excel file')
        convert_excel_parser.epilog = '''example:
  aito convert excel ./myFile.xls
  aito convert excel -o firstSheet < myFile.xslx
  '''
        add_format_parser('json')

        add_format_parser('ndjson')

    def execute_infer_table_schema(self, parsed_args):
        in_format = parsed_args['input-format']
        read_args = {
            'read_input': self.parser.parse_input_arg_value(parsed_args['input']),
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

    def execute_convert(self, parsed_args):
        in_format = parsed_args['input-format']
        convert_args = {
            'read_input': self.parser.parse_input_arg_value(parsed_args['input']),
            'write_output': sys.stdout,
            'in_format': parsed_args['input-format'],
            'out_format': 'json' if parsed_args['json'] else 'ndjson',
            'read_options': {
                'encoding': parsed_args['encoding']
            },
            'convert_options': {},
            'create_table_schema':
                self.parser.check_valid_path(parsed_args['create_table_schema'])
                if parsed_args['create_table_schema'] else None,
        }

        if parsed_args['use_table_schema']:
            schema_path = self.parser.check_valid_path(parsed_args['use_table_schema'], check_exists=True)
            with schema_path.open() as f:
                table_schema = json.load(f)
            convert_args['use_table_schema'] = table_schema

        if in_format == 'csv':
            convert_args['read_options']['delimiter'] = parsed_args['delimiter']
            convert_args['read_options']['decimal'] = parsed_args['decimal']

        if in_format == 'excel':
            if parsed_args['one_sheet']:
                convert_args['read_options']['sheet_name'] = parsed_args['one_sheet']

        DataFrameHandler().convert_file(**convert_args)

    def parse_and_execute(self, parsing_args):
        argcomplete.autocomplete(self.parser)
        parsed_args = vars(self.parser.parse_args(parsing_args))
        action = parsed_args['action']
        if action == 'infer-table-schema':
            self.execute_infer_table_schema(parsed_args)
        elif action == 'convert':
            self.execute_convert(parsed_args)
        return 0


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)-5s %(name)-5s %(levelname)-10s %(message)s',
                        datefmt='%H:%M:%S')
    main_parser = MainParser()
    main_parser.parse_and_execute(sys.argv[1:])


if __name__ == '__main__':
    main()
