import argparse

from aito.cli.parser import AitoParser
from aito.convert.data_frame_handler import DataFrameHandler
from abc import abstractmethod


class ConvertParser:
    def __init__(self):
        self.parser = AitoParser(formatter_class=argparse.RawTextHelpFormatter,
                                 description='convert data into desired format',
                                 usage='''
        aito convert [<convert-options>] <input-format> [input] [output] [<input-format-options>] 
        
        To see help for a specific input format, you can run:
        aito convert <input-format> -h 
        ''',
                                 epilog='''example:
        aito convert json ./myFile.json
        aito convert -c myInferredTableSchema csv - convertedFile.ndjson < myFile.csv
        aito convert -zs givenSchema.json excel - convertedFile.ndjson < myFile.xlsx 
        ''',
                                 add_help=False)
        parser = self.parser
        parser.add_argument('-c', '--create-table-schema', metavar='schema-output-file', type=str,
                            help='create an inferred aito schema and write to a json file')
        parser.add_argument('-e', '--encoding', type=str, default='utf-8',
                            help="encoding to use (default: 'utf-8')")
        parser.add_argument('-f', '--output-format', default='ndjson', choices=['csv', 'excel', 'json', 'ndjson'],
                            help='output format (default: ndjson)')
        parser.add_argument('-s', '--use-table-schema', metavar='schema-input-file', type=str,
                            help='convert the data to match the input table schema')
        parser.add_argument('-z', '--compress-output-file', action='store_true',
                            help='compress output file with gzip')

        parser.add_argument('input-format', choices=['csv', 'json', 'excel'], help='input format')
        parser.add_argument('input', default='-', type=str, nargs='?',
                            help="input file or stream (with no input, or when input is -, read from standard input)")
        parser.add_argument('output', default='-', type=str, nargs='?',
                            help="output file or stream (with no output, or when output is -, "
                                 "read from standard output)")

        self.input_format_parser = {
            'csv': ConvertCsvParser(parser),
            'json': ConvertJsonParser(parser),
            'excel': ConvertExcelParser(parser)
        }

    def parse_and_execute(self, parsing_args) -> int:
        parsed_args, unknown = self.parser.parse_known_args(parsing_args)
        parsed_args = vars(parsed_args)
        convert_args = {
            'read_input': self.parser.parse_input_arg_value(parsed_args['input']),
            'write_output': self.parser.parse_output_arg_value(parsed_args['output']),
            'in_format': parsed_args['input-format'],
            'out_format': parsed_args['output_format'],
            'read_options': {
                'encoding': parsed_args['encoding']
            },
            'convert_options': {
                'compression': 'gzip' if parsed_args['compress_output_file'] else None
            },
            'create_table_schema': self.parser.check_valid_path((parsed_args['create_table_schema']))
            if parsed_args['create_table_schema'] else None,
            'use_table_schema': self.parser.check_valid_path((parsed_args['use_table_schema']))
            if parsed_args['use_table_schema'] else None
        }
        input_format_parser = self.input_format_parser[parsed_args['input-format']]
        input_format_parser.parse_and_execute(parsing_args, convert_args)
        return 0


class ConvertFormatParser:
    def __init__(self, converter_parser: AitoParser, input_format: str):
        self.df_handler = DataFrameHandler()
        self.parser = AitoParser(formatter_class=argparse.RawTextHelpFormatter,
                                 parents=[converter_parser],
                                 usage=f"aito convert [<convert-options>] {input_format} [input] [output] "
                                       f"[<{input_format}-options>]")
        self.convert_format_options = self.parser.add_argument_group(f"optional convert {input_format} arguments")
        self.input_format = input_format

    @abstractmethod
    def parse_and_execute(self, parsing_args, convert_args):
        pass


class ConvertCsvParser(ConvertFormatParser):
    def __init__(self, converter_parser: AitoParser):
        super().__init__(converter_parser, 'csv')
        self.convert_format_options.add_argument('-d', '--delimiter', type=str, default=',',
                                                 help="delimiter to use. Need escape (default: ',')")
        self.convert_format_options.add_argument('-p', '--decimal', type=str, default='.',
                                                 help="Character to recognize decimal point (default '.')")
        self.parser.epilog = '''example:
        aito convert csv ./myFile.csv
        aito convert csv -d ';' < mySemicolonDelimiterFile.csv
        '''

    def parse_and_execute(self, parsing_args, convert_args) -> int:
        parsed_args = vars(self.parser.parse_args(parsing_args))
        convert_args['read_options']['delimiter'] = parsed_args['delimiter']
        convert_args['read_options']['decimal'] = parsed_args['decimal']
        self.df_handler.convert_file(**convert_args)
        return 0


class ConvertJsonParser(ConvertFormatParser):
    def __init__(self, converter_parser: AitoParser):
        super().__init__(converter_parser, 'json')

    def parse_and_execute(self, parsing_args, convert_args) -> int:
        parsed_args = vars(self.parser.parse_args(parsing_args))
        self.df_handler.convert_file(**convert_args)
        return 0


class ConvertExcelParser(ConvertFormatParser):
    def __init__(self, converter_parser: AitoParser):
        super().__init__(converter_parser, 'excel')
        self.parser.description = 'Convert excel format input, accept both xls and xlsx. ' \
                                  'Only read the first sheet of the file by default'
        self.convert_format_options.add_argument('-o', '--one-sheet', type=str, metavar='sheet-name',
                                                 help='read one sheets of the excel file')
        self.parser.epilog = '''example:
        aito convert excel ./myFile.xls
        aito convert excel -o firstSheet < myFile.xslx
        '''

    def parse_and_execute(self, parsing_args, convert_args) -> int:
        parsed_args = vars(self.parser.parse_args(parsing_args))
        if parsed_args['one_sheet']:
            convert_args['read_options']['sheet_name'] = parsed_args['one_sheet']
        self.df_handler.convert_file(**convert_args)
        return 0
