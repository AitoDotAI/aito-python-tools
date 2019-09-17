import argparse

from aito.cli.parser import AitoParser
from aito.schema.converter import Converter
import sys
from pathlib import Path
from abc import ABC, abstractmethod


class ConvertParser:
    def __init__(self):
        self.parser = AitoParser(formatter_class=argparse.RawTextHelpFormatter,
                                 description='convert data into ndjson format',
                                 usage='''
        aito.py convert [<options>] <input-format> [input] [output] [<input-format-options>] 

        Convert an input from input-format to ndjson
        
        aito.py convert <input-format> -h 
        
        for more specific input format options
        ''',
                                 epilog='''example:
        python aito.py convert json ./myFile.json
        python aito.py convert json - convertedFile.ndjson < myFile.json
        python aito.py convert -zs myFile_schema.json json - convertedFile.ndjson < myFile.json
        
        With no input, or when input is -, read from standard input
        With no output, or when output is -, read from standard output 
        ''',
                                 add_help=False)
        parser = self.parser
        parser.add_argument('-z', '--compress-output-file', action='store_true',
                            help='compress output file with gzip')
        parser.add_argument('-s', '--generate-aito-schema', metavar='schema-file', type=str,
                            help='write inferred aito schema to a json file')

        parser.add_argument('input-format', choices=['csv', 'json', 'xlsx'], help='input format', )
        parser.add_argument('input', default='-', type=str, nargs='?', help="input file, dir, or stream")
        parser.add_argument('output', default='-', type=str, nargs='?', help="output file, dir, or stream")

        self.input_format_parser = {
            'csv': ConvertCsvParser(parser),
            'json': ConvertJsonParser(parser),
            'xlsx': ConvertXlsxParser(parser)
        }

    def parse_and_execute(self, parsing_args) -> int:
        parsed_args, unknown = self.parser.parse_known_args(parsing_args)
        parsed_args = vars(parsed_args)
        input_format_parser = self.input_format_parser[parsed_args['input-format']]
        input_format_parser.parse_and_execute(parsing_args)
        return 0


class ConvertFormatParser:
    def __init__(self, parent_convert_parser: AitoParser, input_format: str):

        self.parser = AitoParser(formatter_class=argparse.RawTextHelpFormatter,
                                 parents=[parent_convert_parser],
                                 usage=f"aito.py convert [<options>] {input_format} [input] [output] "
                                       f"[<{input_format}-options>]")
        self.input_format = input_format

    def parse_shared_args(self, parsing_args):
        def check_valid_path(str_path):
            try:
                path = Path(str_path)
            except Exception:
                raise Exception(f"invalid path {str_path}")
            return path

        parsed_args = vars(self.parser.parse_args(parsing_args))
        if parsed_args['input'] == '-':
            parsed_args['input'] = sys.stdin
        else:
            input_path = check_valid_path(parsed_args['input'])
            if not input_path.exists():
                raise Exception(f"input path {parsed_args['input']} does not exist")
            parsed_args['input'] = input_path
        if parsed_args['output'] == '-':
            parsed_args['output'] = sys.stdout
        else:
            parsed_args['output'] = check_valid_path(parsed_args['output'])

        if parsed_args['generate_aito_schema']:
            parsed_args['generate_aito_schema'] = check_valid_path((parsed_args['generate_aito_schema']))
        shared_convert_args = {
            'read_input': parsed_args['input'],
            'write_output': parsed_args['output'],
            'in_format': self.input_format,
            'out_format': 'ndjson',
            'read_options': {},
            'convert_options': {
                'compression': 'gzip' if parsed_args['compress_output_file'] else None
            },
            'generate_aito_schema': parsed_args['generate_aito_schema']
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
        Converter().convert_file(**convert_args)
        return 0


class ConvertJsonParser(ConvertFormatParser):
    def __init__(self, parent_convert_parser: AitoParser):
        super().__init__(parent_convert_parser, 'json')

    def parse_and_execute(self, parsing_args) -> int:
        parsed_args, convert_args = super().parse_shared_args(parsing_args)
        Converter().convert_file(**convert_args)
        return 0


class ConvertXlsxParser(ConvertFormatParser):
    def __init__(self, parent_convert_parser: AitoParser):
        super().__init__(parent_convert_parser, 'xlsx')

    def parse_and_execute(self, parsing_args) -> int:
        parsed_args, convert_args = super().parse_shared_args(parsing_args)
        Converter().convert_file(**convert_args)
        return 0