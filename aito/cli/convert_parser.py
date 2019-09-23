import argparse
import sys
from pathlib import Path

from aito.cli.parser import AitoParser
from aito.schema.data_frame_converter import DataFrameConverter


class ConvertParser:
    def __init__(self):
        self.parser = AitoParser(formatter_class=argparse.RawTextHelpFormatter,
                                 description='convert data into ndjson format',
                                 usage='''
        aito.py convert [<convert-options>] <input-format> [input] [output] [<input-format-options>] 
        
        To see help for a specific input format, you can run:
        aito.py convert <input-format> -h 
        ''',
                                 epilog='''example:
        python aito.py convert json ./myFile.json
        python aito.py convert json - convertedFile.ndjson < myFile.json
        python aito.py convert -zs myFile_schema.json json - convertedFile.ndjson < myFile.json 
        ''',
                                 add_help=False)
        parser = self.parser
        parser.add_argument('-e', '--encoding', type=str, default='utf-8',
                            help="encoding to use (default: 'utf-8')")
        parser.add_argument('-g', '--generate-aito-schema', metavar='schema-output-file', type=str,
                            help='write inferred aito schema to a json file')
        parser.add_argument('-u', '--use-aito-schema', metavar='schema-input-file', type=str,
                            help='convert the data to match the desired schema')
        parser.add_argument('-z', '--compress-output-file', action='store_true',
                            help='compress output file with gzip')

        self.input_format_parser = {
            'csv': ConvertCsvParser(parser),
            'json': ConvertJsonParser(parser),
            'xlsx': ConvertXlsxParser(parser)
        }
        parser.add_argument('input-format', choices=list(self.input_format_parser.keys()), help='input format')
        parser.add_argument('input', default='-', type=str, nargs='?',
                            help="input file or stream (with no input, or when input is -, read from standard input)")
        parser.add_argument('output', default='-', type=str, nargs='?',
                            help="output file or stream (with no output, or when output is -, "
                                 "read from standard output)")

    def parse_and_execute(self, parsing_args) -> int:
        parsed_args, unknown = self.parser.parse_known_args(parsing_args)
        parsed_args = vars(parsed_args)
        input_format_parser = self.input_format_parser[parsed_args['input-format']]
        input_format_parser.parse_and_execute(parsing_args)
        return 0


class ConvertFormatParser:
    def __init__(self, parent_convert_parser: AitoParser, input_format: str):
        self.converter = DataFrameConverter()

        self.parser = AitoParser(formatter_class=argparse.RawTextHelpFormatter,
                                 parents=[parent_convert_parser],
                                 usage=f"aito.py convert [<options>] {input_format} [input] [output] "
                                       f"[<{input_format}-options>]")
        self.input_format = input_format

    def parse_shared_args(self, parsing_args):
        parsed_args = vars(self.parser.parse_args(parsing_args))
        parsed_args['input'] = self.parser.parse_input_arg_value(parsed_args['input'])
        parsed_args['output'] = self.parser.parse_output_arg_value(parsed_args['output'])

        if parsed_args['generate_aito_schema']:
            parsed_args['generate_aito_schema'] = self.parser.check_valid_path((parsed_args['generate_aito_schema']))
        if parsed_args['use_aito_schema']:
            parsed_args['use_aito_schema'] = self.parser.check_valid_path((parsed_args['use_aito_schema']))

        shared_convert_args = {
            'read_input': parsed_args['input'],
            'write_output': parsed_args['output'],
            'in_format': self.input_format,
            'out_format': 'ndjson',
            'read_options': {
                'encoding': parsed_args['encoding']
            },
            'convert_options': {
                'compression': 'gzip' if parsed_args['compress_output_file'] else None
            },
            'generate_aito_schema': parsed_args['generate_aito_schema'],
            'use_aito_schema': parsed_args['use_aito_schema']
        }

        return parsed_args, shared_convert_args


class ConvertCsvParser(ConvertFormatParser):
    def __init__(self, parent_convert_parser: AitoParser):
        super().__init__(parent_convert_parser, 'csv')
        csv_args_group = self.parser.add_argument_group('optional convert csv arguments')
        csv_args_group.add_argument('-d', '--delimiter', type=str, default=',',
                                    help="delimiter to use. Need escape (default: ',')")

    def parse_and_execute(self, parsing_args) -> int:
        parsed_args, convert_args = super().parse_shared_args(parsing_args)
        convert_args['read_options']['delimiter'] = parsed_args['delimiter']
        self.converter.convert_file(**convert_args)
        return 0


class ConvertJsonParser(ConvertFormatParser):
    def __init__(self, parent_convert_parser: AitoParser):
        super().__init__(parent_convert_parser, 'json')

    def parse_and_execute(self, parsing_args) -> int:
        parsed_args, convert_args = super().parse_shared_args(parsing_args)
        self.converter.convert_file(**convert_args)
        return 0


class ConvertXlsxParser(ConvertFormatParser):
    def __init__(self, parent_convert_parser: AitoParser):
        super().__init__(parent_convert_parser, 'xlsx')

    def parse_and_execute(self, parsing_args) -> int:
        parsed_args, convert_args = super().parse_shared_args(parsing_args)
        self.converter.convert_file(**convert_args)
        return 0