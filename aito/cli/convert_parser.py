import argparse

import sys
from aito.cli.parser import ParserWrapper, AitoArgParser
from aito.convert.data_frame_handler import DataFrameHandler


class ConvertParserWrapper(ParserWrapper):
    def __init__(self):
        super().__init__(add_help=False)
        parser = self.parser
        parser.description = 'convert data of table entries into ndjson or json (if specified)'
        parser.usage = ''' aito convert [-h] <input-format> [<options>] [input]
        To see help for a specific input format:
            aito convert <input-format> -h 
        '''
        parser.epilog = '''example:
        aito convert json myFile.json > convertedFile.ndjson 
        aito convert csv myFile.csv -c myInferredTableSchema.json --json > convertedFile.json
        aito convert excel -s desiredSchema.json < myFile.xlsx > convertedFile.ndjson 
        '''
        self.input_format_to_parser = {
            'csv': ConvertCsvParserWrapper,
            'excel': ConvertExcelParserWrapper,
            'json': ConvertJsonParserWrapper,
            'ndjson': ConvertNdJsonParserWrapper,
        }
        self.input_format_arg = parser.add_argument('input-format', choices=list(self.input_format_to_parser.keys()),
                                                    help='input format')

    def parse_and_execute(self, parsing_args) -> int:
        parsed_args, unknown = self.parser.parse_known_args(parsing_args)
        parsed_args = vars(parsed_args)
        input_format_parser = self.input_format_to_parser[parsed_args['input-format']](self.parser,
                                                                                       self.input_format_arg)
        input_format_parser.parse_and_execute(parsing_args)
        return 0


class ConvertFormatParserWrapper(ParserWrapper):
    def __init__(self, parent_parser: AitoArgParser, input_format_arg, input_format: str):
        super().__init__()
        self.df_handler = DataFrameHandler()
        self.input_format = input_format
        input_format_arg.help = argparse.SUPPRESS
        self.parser = AitoArgParser(formatter_class=argparse.RawTextHelpFormatter,
                                    parents=[parent_parser],
                                    usage=f"aito convert {input_format} [<options>] [input]")
        parser = self.parser
        either_use_or_create_schema = parser.add_mutually_exclusive_group()
        either_use_or_create_schema.add_argument('-c', '--create-table-schema', metavar='schema-output-file', type=str,
                                                 help='create an inferred aito schema and write to a json file')
        parser.add_argument('-e', '--encoding', type=str, default='utf-8', help="encoding to use (default: 'utf-8')")
        parser.add_argument('-j', '--json', action='store_true', help='convert to json format')
        either_use_or_create_schema.add_argument('-s', '--use-table-schema', metavar='schema-input-file', type=str,
                                                 help='convert the data to match the input table schema')
        parser.add_argument('input', default='-', type=str, nargs='?',
                            help="input file or stream (with no input or when input is -, read standard input) "
                                 "\nit is recommended to use input file, especially for excel format")

    def get_shared_convert_args_from_parsed_args(self, parsed_args):
        parser = self.parser
        convert_args = {
            'read_input': parser.parse_input_arg_value(parsed_args['input']),
            'write_output': sys.stdout,
            'in_format': parsed_args['input-format'],
            'out_format': 'json' if parsed_args['json'] else 'ndjson',
            'read_options': {
                'encoding': parsed_args['encoding']
            },
            'convert_options': {},
            'create_table_schema':
                parser.check_valid_path(parsed_args['create_table_schema'])
                if parsed_args['create_table_schema'] else None,
            'use_table_schema':
                self.parser.check_valid_path(parsed_args['use_table_schema'], check_exists=True)
                if parsed_args['use_table_schema'] else None
        }
        return convert_args

    def parse_and_execute(self, parsing_args):
        pass


class ConvertCsvParserWrapper(ConvertFormatParserWrapper):
    def __init__(self, parent_parser: AitoArgParser, input_format_arg):
        super().__init__(parent_parser, input_format_arg, 'csv')
        parser = self.parser
        parser.add_argument('-d', '--delimiter', type=str, default=',',
                            help="delimiter to use. Need escape (default: ',')")
        parser.add_argument('-p', '--decimal', type=str, default='.',
                            help="Character to recognize decimal point (default '.')")
        parser.epilog = '''example:
        aito convert csv myFile.csv
        aito convert csv -d ';' --json < mySemicolonDelimiterFile.csv > convertedFile.json 
        '''

    def parse_and_execute(self, parsing_args) -> int:
        parsed_args = vars(self.parser.parse_args(parsing_args))
        convert_args = self.get_shared_convert_args_from_parsed_args(parsed_args)
        convert_args['read_options']['delimiter'] = parsed_args['delimiter']
        convert_args['read_options']['decimal'] = parsed_args['decimal']
        self.df_handler.convert_file(**convert_args)
        return 0


class ConvertJsonParserWrapper(ConvertFormatParserWrapper):
    def __init__(self, parent_parser: AitoArgParser, input_format_arg):
        super().__init__(parent_parser, input_format_arg, 'json')

    def parse_and_execute(self, parsing_args) -> int:
        parsed_args = vars(self.parser.parse_args(parsing_args))
        convert_args = self.get_shared_convert_args_from_parsed_args(parsed_args)
        self.df_handler.convert_file(**convert_args)
        return 0


class ConvertNdJsonParserWrapper(ConvertFormatParserWrapper):
    def __init__(self, parent_parser: AitoArgParser, input_format_arg):
        super().__init__(parent_parser, input_format_arg, 'ndjson')

    def parse_and_execute(self, parsing_args) -> int:
        parsed_args = vars(self.parser.parse_args(parsing_args))
        convert_args = self.get_shared_convert_args_from_parsed_args(parsed_args)
        self.df_handler.convert_file(**convert_args)
        return 0


class ConvertExcelParserWrapper(ConvertFormatParserWrapper):
    def __init__(self, parent_parser: AitoArgParser, input_format_arg):
        super().__init__(parent_parser, input_format_arg, 'excel')
        self.parser.description = 'Convert excel format input, accept both xls and xlsx. ' \
                                  'Read the first sheet of the file by default'
        self.parser.add_argument('-o', '--one-sheet', type=str, metavar='sheet-name',
                                                 help='read a sheet of the excel file')
        self.parser.epilog = '''example:
        aito convert excel ./myFile.xls
        aito convert excel -o firstSheet < myFile.xslx
        '''

    def parse_and_execute(self, parsing_args) -> int:
        parsed_args = vars(self.parser.parse_args(parsing_args))
        convert_args = self.get_shared_convert_args_from_parsed_args(parsed_args)
        if parsed_args['one_sheet']:
            convert_args['read_options']['sheet_name'] = parsed_args['one_sheet']
        self.df_handler.convert_file(**convert_args)
        return 0
