import argparse
import sys

from aito.schema.converter import Converter
from typing import List, Dict
import pandas as pd


class ArgParser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write(f"error: {message}\n")
        self.print_help()
        sys.exit(2)


def create_convert_parser(main_sub_parser):
    c_parser = main_sub_parser.add_parser('convert', help='convert data into ndjson format')
    c_parser.add_argument('-i', '--input', type=argparse.FileType('r'), default='-',
                          help="input file or stream (default: -)")
    c_parser.add_argument('-o', '--output', type=argparse.FileType('w'), default='-',
                          help="output file or stream (default: -)")
    c_parser.add_argument('-z', '--compress-output-file', action='store_false',
                          help='compress output file with gzip')
    c_parser.add_argument('-s', '--generate-aito-schema', action='store_true',
                          help='generate an inferred aito schema')

    c_sub_parser = c_parser.add_subparsers(help='input format', dest='input_format')
    c_sub_parser.required = True
    csv_parser = c_sub_parser.add_parser('csv')
    xlsx_parser = c_sub_parser.add_parser('xlsx')
    json_parser = c_sub_parser.add_parser('json')
    csv_parser.add_argument('-d', '--delimiter', type=str, default=',', help='delimiter to use')


def create_parser():
    example_text = """example:
    python aito.py convert json -i ./myFile.json 
    """
    parser = ArgParser(formatter_class=argparse.RawTextHelpFormatter, epilog=example_text)
    parser.add_argument('-l', '--use-log-file', action='store_true',
                        help='log to a log file in .log directory instead of output stream (default: false)')
    sub_parser = parser.add_subparsers(help='action to perform', dest='action')
    sub_parser.required = True

    create_convert_parser(sub_parser)
    return parser


def parse_convert_args(args_dict) -> Dict:
    convert_args = {
        'read_input': args_dict['input'],
        'write_output': args_dict['output'],
        'in_format': args_dict['input_format'],
        'out_format': 'ndjson',
        'read_options': {},
        'convert_options': {},
        'generate_aito_schema': args_dict['generate_aito_schema']
    }
    if args_dict['compress_output_file']:
        convert_args['convert_options']['compression'] = 'gzip'
    if args_dict['input_format'] == 'csv':
        convert_args['read_options']['delimiter'] = convert_args['delimiter']

    return convert_args


if __name__ == '__main__':
    main_parser = create_parser()
    args = vars(main_parser.parse_args())
    print(args)
    if args['action'] == 'convert':
        converter = Converter()
        converter_args = parse_convert_args(args)
        converter.convert_file(**converter_args)
