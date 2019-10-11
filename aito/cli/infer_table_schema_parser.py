import argparse
from abc import abstractmethod

from aito.cli.parser import ParserWrapper, AitoArgParser
import sys
import json

from aito.data_frame_handler import DataFrameHandler
from aito.schema_handler import SchemaHandler


class InferTableSchemaParserWrapper(ParserWrapper):
    def __init__(self):
        super().__init__(add_help=False)
        parser = self.parser
        parser.description = 'infer Aito table schema from a file'
        parser.usage = ''' aito infer-table-schema [-h] <file-format> [input] [<options>]
                To see help for a specific input format:
                    aito convert <file-format> -h 
                '''
        parser.epilog = '''example:
                aito infer-table-schema json myFile.json > inferredSchema.json
                aito infer-table-schema csv myFile.csv > inferredSchema.json
                aito infer-table-schema excel < myFile.xlsx > inferredSchema.json 
                '''
        self.file_format_to_parser = {
            'csv': InferCsvFileParserWrapper,
            'excel': InferExcelFileParserWrapper,
            'json': InferJsonFileParserWrapper,
            'ndjson': InferNdJsonFileParserWrapper,
        }
        self.file_format_arg = parser.add_argument('file-format', choices=list(self.file_format_to_parser.keys()),
                                                    help='input file format')

    def parse_and_execute(self, parsing_args) -> int:
        parsed_args, unknown = self.parser.parse_known_args(parsing_args)
        parsed_args = vars(parsed_args)
        file_format_parser = self.file_format_to_parser[parsed_args['file-format']](self.parser,
                                                                                       self.file_format_arg)
        file_format_parser.parse_and_execute(parsing_args)
        return 0


class InferFromFormatParserWrapper:
    def __init__(self, parent_parser: AitoArgParser, file_format_arg, file_format: str):
        self.df_handler = DataFrameHandler()
        self.schema_handler = SchemaHandler()
        self.file_format = file_format
        file_format_arg.help = argparse.SUPPRESS
        self.parser = AitoArgParser(formatter_class=argparse.RawTextHelpFormatter,
                                    parents=[parent_parser],
                                    usage=f"aito infer-table-schema {file_format} [input] [<options>]")
        parser = self.parser

        parser.add_argument('-e', '--encoding', type=str, default='utf-8',
                            help="encoding to use (default: 'utf-8')")
        parser.add_argument('input', default='-', type=str, nargs='?',
                            help="input file or stream (with no input or when input is -, read standard input) "
                                 "\nit is recommended to use input file, especially for excel format")

    def get_read_args_from_parsed_args(self, parsed_args):
        parser = self.parser
        read_args = {
            'read_input': parser.parse_input_arg_value(parsed_args['input']),
            'in_format': parsed_args['file-format'],
            'read_options': {
                'encoding': parsed_args['encoding']
            }
        }
        return read_args

    def read_df_create_schema(self, read_args):
        df = self.df_handler.read_file_to_df(**read_args)
        schema = self.schema_handler.generate_table_schema_from_pandas_dataframe(df)
        json.dump(schema, sys.stdout, indent=4, sort_keys=True)

    @abstractmethod
    def parse_and_execute(self, parsing_args):
        pass


class InferCsvFileParserWrapper(InferFromFormatParserWrapper):
    def __init__(self, parent_parser: AitoArgParser, file_format_arg):
        super().__init__(parent_parser, file_format_arg, 'csv')
        parser = self.parser
        parser.add_argument('-d', '--delimiter', type=str, default=',',
                            help="delimiter to use. Need escape (default: ',')")
        parser.add_argument('-p', '--decimal', type=str, default='.',
                            help="Character to recognize decimal point (default '.')")
        parser.epilog = '''example:
            aito infer-table-schema csv myFile.csv
            aito infer-table-schema csv -d ';' < mySemicolonDelimiterFile.csv > inferredSchema.json 
            '''

    def parse_and_execute(self, parsing_args) -> int:
        parsed_args = vars(self.parser.parse_args(parsing_args))
        read_args = self.get_read_args_from_parsed_args(parsed_args)
        read_args['read_options']['delimiter'] = parsed_args['delimiter']
        read_args['read_options']['decimal'] = parsed_args['decimal']
        self.read_df_create_schema(read_args)
        return 0


class InferJsonFileParserWrapper(InferFromFormatParserWrapper):
    def __init__(self, parent_parser: AitoArgParser, file_format_arg):
        super().__init__(parent_parser, file_format_arg, 'json')

    def parse_and_execute(self, parsing_args) -> int:
        parsed_args = vars(self.parser.parse_args(parsing_args))
        read_args = self.get_read_args_from_parsed_args(parsed_args)
        self.read_df_create_schema(read_args)
        return 0


class InferNdJsonFileParserWrapper(InferFromFormatParserWrapper):
    def __init__(self, parent_parser: AitoArgParser, file_format_arg):
        super().__init__(parent_parser, file_format_arg, 'ndjson')

    def parse_and_execute(self, parsing_args) -> int:
        parsed_args = vars(self.parser.parse_args(parsing_args))
        read_args = self.get_read_args_from_parsed_args(parsed_args)
        self.read_df_create_schema(read_args)
        return 0


class InferExcelFileParserWrapper(InferFromFormatParserWrapper):
    def __init__(self, parent_parser: AitoArgParser, file_format_arg):
        super().__init__(parent_parser, file_format_arg, 'excel')
        self.parser.description = 'Create table schema from an excel file, accept both xls and xlsx. ' \
                                  'Read the first sheet of the file by default'
        self.parser.add_argument('-o', '--one-sheet', type=str, metavar='sheet-name',
                                 help='read a sheet of the excel file')
        self.parser.epilog = '''example:
            aito infer-table-schema excel ./myFile.xls
            aito infer-table-schema excel -o firstSheet < myFile.xslx
            '''

    def parse_and_execute(self, parsing_args) -> int:
        parsed_args = vars(self.parser.parse_args(parsing_args))
        read_args = self.get_read_args_from_parsed_args(parsed_args)
        if parsed_args['one_sheet']:
            read_args['read_options']['sheet_name'] = parsed_args['one_sheet']
        self.read_df_create_schema(read_args)
        return 0
