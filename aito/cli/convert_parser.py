import argparse

from aito.cli.parser import AitoParser
from aito.schema.converter import Converter


class ConvertParser():
    def __init__(self):
        self.parser = AitoParser(formatter_class=argparse.RawTextHelpFormatter,
                                 description='convert data into ndjson format',
                                 epilog='''example:
        python aito.py convert json -i ./myFile.json 
        ''')
        parser = self.parser
        parser.add_argument('-i', '--input', type=argparse.FileType('r'), default='-',
                                 help="input file or stream (default: -)")
        parser.add_argument('-o', '--output', type=argparse.FileType('w'), default='-',
                                 help="output file or stream (default: -)")
        parser.add_argument('-z', '--compress-output-file', action='store_false',
                                 help='compress output file with gzip')
        parser.add_argument('-s', '--generate-aito-schema', action='store_true',
                                 help='generate an inferred aito schema')

        self.input_format_sub_parser = self.parser.add_subparsers(help='input format', dest='input_format')
        in_f_parser = self.input_format_sub_parser
        in_f_parser.required = True
        csv_parser = in_f_parser.add_parser('csv')
        xlsx_parser = in_f_parser.add_parser('xlsx')
        json_parser = in_f_parser.add_parser('json')
        csv_parser.add_argument('-d', '--delimiter', type=str, default=',', help='delimiter to use')

    def parse_and_execute(self, parsing_args=None) -> int:
        args = vars(self.parser.parse_args(parsing_args))
        convert_args = {
            'read_input': args['input'],
            'write_output': args['output'],
            'in_format': args['input_format'],
            'out_format': 'ndjson',
            'read_options': {},
            'convert_options': {},
            'generate_aito_schema': args['generate_aito_schema']
        }

        if args['compress_output_file']:
            convert_args['convert_options']['compression'] = 'gzip'
        if args['input_format'] == 'csv':
            convert_args['read_options']['delimiter'] = args['delimiter']

        converter = Converter()
        converter.convert_file(**convert_args)

        return 0