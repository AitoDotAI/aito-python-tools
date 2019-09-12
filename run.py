import argparse
import sys

from aito.schema.converter import Converter
from typing import List, Dict


class ArgParser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write(f"error: {message}\n")
        self.print_help()
        sys.exit(2)

    @staticmethod
    def parse_key_value_args_to_dict(key_value_args: List[str]):
        def parse_key_value_string(key_value_string):
            key_value = key_value_string.split('=')
            return key_value[0], '='.join(key_value[1:])

        args_dict = {}
        for k_v_arg in key_value_args:
            key, value = parse_key_value_string(k_v_arg)
            args_dict[key] = value
        return args_dict


def create_parser():
    example_text = """example:
    python run.py converter ./myFile.json . ndjson
    python run.py converter ./myFile.json.gzip . csv -f json -o newName -r [engine=python] -c [sep=;] --s
    """

    parser = ArgParser(formatter_class=argparse.RawTextHelpFormatter, epilog=example_text)
    sub_parser = parser.add_subparsers(help='action to perform', dest='action')
    sub_parser.required = True

    converter_parser = sub_parser.add_parser('convert',
                                             help='convert a data file to a format')
    converter_parser.add_argument('input-file-path', type=str, help="path to input file")
    converter_parser.add_argument('output-folder-path', type=str, help="path to output file folder")
    converter_parser.add_argument('output-format', type=str, choices=Converter.allowed_format, help='output format')
    converter_parser.add_argument('-f', '--in-format',
                                  type=str, choices=Converter.allowed_format,
                                  help='input format. Inferred from input file extension if not specified. Needed if '
                                       'the input file extension does not match the desired input format '
                                       '(e.g: file.json.zip)')
    converter_parser.add_argument('-o', '--output-file-name', type=str,
                                  help='output file name if it is different from input file name')
    converter_parser.add_argument('-s', '--generate-aito-schema', action='store_false',
                                  help='generate an inferred aito schema json file at the same output folder')
    converter_parser.add_argument('-r', '--read-options', nargs='+',
                                  help='pandas read function parameter and value pairs '
                                       '(do not put spaces before or after the = sign). '
                                       'value containing space should be in quotes.'
                                       '(e.g: [engine=python, low_memory=false])')
    converter_parser.add_argument('-c', '--convert-options', nargs='+',
                                  help='pandas read function parameter and value pairs '
                                       '(do not put spaces before or after the = sign). '
                                       'value containing space should be in quotes.'
                                       '(e.g: [sep=;, encoding=utf-16])')
    return parser


def parse_converter_args(args_dict) -> Dict:
    return {
        'in_file_path': args_dict['input-file-path'],
        'out_file_folder': args_dict['output-folder-path'],
        'out_format': args_dict['output-format'],
        'in_format': args_dict['in_format'],
        'out_file_name': args_dict['output_file_name'],
        'read_options': main_parser.parse_key_value_args_to_dict(args_dict['read_options'])
        if (args_dict['read_options']) else None,
        'convert_options': main_parser.parse_key_value_args_to_dict(args_dict['convert_options'])
        if (args_dict['convert_options']) else None,
        'generate_aito_schema': args_dict['generate_aito_schema']
    }


if __name__ == '__main__':
    main_parser = create_parser()
    args = vars(main_parser.parse_args())
    print(args)
    if args['action'] == 'convert':
        converter = Converter()
        converter_args = parse_converter_args(args)
        print(converter_args)
        converter.convert_file(**converter_args)
