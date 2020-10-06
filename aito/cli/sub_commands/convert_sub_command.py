import json
import sys
from typing import Dict, List

from aito.schema import AitoTableSchema
from aito.utils.data_frame_handler import DataFrameHandler
from .sub_command import SubCommand
from ..parser import PathArgType, InputArgType, ParseError, try_load_json


class ConvertFromFormatSubCommand(SubCommand):
    def build_parser(self, parser):
        # add share arguments between formats
        either_use_or_create_schema = parser.add_mutually_exclusive_group()
        either_use_or_create_schema.add_argument(
            '-c', '--create-table-schema', metavar='schema-output-file', type=PathArgType(parent_must_exist=True),
            help='create an inferred aito schema and write to output file'
        )
        either_use_or_create_schema.add_argument(
            '-s', '--use-table-schema', metavar='schema-input-file', type=PathArgType(must_exist=True),
            help='convert the data to match the input table schema'
        )
        parser.add_argument('-j', '--json', action='store_true', help='convert to json format')
        parser.add_argument(
            'input', default='-', type=InputArgType(), nargs='?',
            help="path to the input file (when no input file is given or when input is -, read from the standard input)"
        )

    @staticmethod
    def parsed_args_to_data_frame_handler_convert_args(parsed_args: Dict) -> Dict:
        in_format = parsed_args['input-format']
        convert_args = {
            'read_input': parsed_args['input'],
            'write_output': sys.stdout,
            'in_format': parsed_args['input-format'],
            'out_format': 'json' if parsed_args['json'] else 'ndjson',
            'read_options': {},
            'convert_options': {},
        }

        if parsed_args['use_table_schema']:
            with parsed_args['use_table_schema'].open() as f:
                table_schema = try_load_json(f, 'table schema')
            convert_args['use_table_schema'] = table_schema

        if in_format == 'csv':
            convert_args['read_options']['delimiter'] = parsed_args['delimiter']
            convert_args['read_options']['decimal'] = parsed_args['decimal']

        if in_format == 'excel':
            if parsed_args['input'] == sys.stdin:
                raise ParseError('input must be a file path for excel files')
            if parsed_args['one_sheet']:
                convert_args['read_options']['sheet_name'] = parsed_args['one_sheet']
        return convert_args

    def parse_and_execute(self, parsed_args: Dict):
        parsed_convert_args = self.parsed_args_to_data_frame_handler_convert_args(parsed_args)
        output_schema_path = parsed_args['create_table_schema'] if parsed_args['create_table_schema'] else None

        converted_df = DataFrameHandler().convert_file(**parsed_convert_args)
        if output_schema_path:
            inferred_schema = AitoTableSchema.infer_from_pandas_data_frame(converted_df)
            with output_schema_path.open(mode='w') as f:
                json.dump(inferred_schema.to_json_serializable(), f, indent=2, sort_keys=True)
        return 0


class ConvertFromCSVSubCommand(ConvertFromFormatSubCommand):
    def __init__(self):
        super().__init__('csv', 'convert CSV data')

    def build_parser(self, parser):
        super().build_parser(parser)
        parser.add_csv_format_default_arguments()


class ConvertFromExcelSubCommand(ConvertFromFormatSubCommand):
    def __init__(self):
        super().__init__('excel', 'convert EXCEL data')

    def build_parser(self, parser):
        super().build_parser(parser)
        parser.add_excel_format_default_arguments()
        parser.description = 'Convert EXCEL data, accept both xls and xlsx'


class ConvertSubCommand(SubCommand):
    _default_sub_commands = [
        ConvertFromCSVSubCommand(),
        ConvertFromExcelSubCommand(),
        ConvertFromFormatSubCommand('json', 'convert JSON data'),
        ConvertFromFormatSubCommand('ndjson', 'convert NDJSON data'),
    ]

    def __init__(self, sub_commands: List[SubCommand] = None):
        super().__init__('convert', 'convert from a given format into NDJSON|JSON')
        if not sub_commands:
            sub_commands = self._default_sub_commands
        self._sub_commands_map = {cmd.name: cmd for cmd in sub_commands}

    def build_parser(self, parser):
        parser.epilog = '''To see help for a specific format:
  aito convert <input-format> - h

When no input or when input is -, read standard input.
You must use input file instead of standard input for excel file
'''
        sub_commands_subparsers = parser.add_subparsers(
            title='input-format',
            dest='input-format',
            metavar='<input-format>'
        )
        sub_commands_subparsers.required = True

        for sub_cmd in self._sub_commands_map.values():
            sub_cmd_parser = sub_commands_subparsers.add_parser(sub_cmd.name, help=sub_cmd.help_message)
            sub_cmd.build_parser(sub_cmd_parser)

    def parse_and_execute(self, parsed_args: Dict):
        self._sub_commands_map[parsed_args['input-format']].parse_and_execute(parsed_args)
        return 0
