import json
import sys
from typing import Dict
from typing import List

from aito.schema import AitoTableSchema
from aito.utils.data_frame_handler import DataFrameHandler
from .sub_command import SubCommand
from ..parser import InputArgType, ParseError, create_sql_connecting_from_parsed_args


class InferFromFormatSubCommand(SubCommand):
    def build_parser(self, parser):
        parser.add_argument(
            'input', default='-', type=InputArgType(), nargs='?',
            help="path to the input file (when no input file is given or when input is -, read from the standard input)"
        )

    @staticmethod
    def parsed_args_to_data_frame_handler_read_args(parsed_args: Dict) -> Dict:
        in_format = parsed_args['input-format']

        read_args = {
            'read_input': parsed_args['input'],
            'in_format': in_format,
            'read_options': {}
        }

        if in_format == 'csv':
            read_args['read_options']['delimiter'] = parsed_args['delimiter']
            read_args['read_options']['decimal'] = parsed_args['decimal']
        elif in_format == 'excel':
            if parsed_args['input'] == sys.stdin:
                raise ParseError('input must be a file path for excel files')
            if parsed_args['one_sheet']:
                read_args['read_options']['sheet_name'] = parsed_args['one_sheet']
        return read_args

    def parse_and_execute(self, parsed_args: Dict):
        parsed_read_args = self.parsed_args_to_data_frame_handler_read_args(parsed_args)
        df = DataFrameHandler().read_file_to_df(**parsed_read_args)
        inferred_schema = AitoTableSchema.infer_from_pandas_data_frame(df)
        json.dump(inferred_schema.to_json_serializable(), sys.stdout, indent=4, sort_keys=True)
        return 0


class InferFromCSVSubCommand(InferFromFormatSubCommand):
    def __init__(self):
        super().__init__('csv', 'infer a table schema from CSV data')

    def build_parser(self, parser):
        super().build_parser(parser)
        parser.add_csv_format_default_arguments()


class InferFromExcelSubCommand(InferFromFormatSubCommand):
    def __init__(self):
        super().__init__('excel', 'infer a table schema from EXCEL data')

    def build_parser(self, parser):
        super().build_parser(parser)
        parser.add_excel_format_default_arguments()


class InferFromSQLSubCommand(SubCommand):
    def __init__(self):
        super().__init__('from-sql', 'infer a table schema from the result of a SQL query')

    def build_parser(self, parser):
        parser.add_sql_default_credentials_arguments()
        parser.add_argument('query', type=str, help='query to get the data from your database')

    def parse_and_execute(self, parsed_args: Dict):
        connection = create_sql_connecting_from_parsed_args(parsed_args)
        result_df = connection.execute_query_and_save_result(parsed_args['query'])
        inferred_schema = AitoTableSchema.infer_from_pandas_data_frame(result_df)
        json.dump(inferred_schema.to_json_serializable(), sys.stdout, indent=4, sort_keys=True)
        return 0


class InferTableSchemaSubCommand(SubCommand):
    _default_sub_commands = [
        InferFromCSVSubCommand(),
        InferFromExcelSubCommand(),
        InferFromFormatSubCommand('json', 'infer a table schema from JSON data'),
        InferFromFormatSubCommand('ndjson', 'infer a table schema from NDJSON data'),
        InferFromSQLSubCommand()
    ]

    def __init__(self, sub_commands: List[SubCommand] = None):
        super().__init__('infer-table-schema', 'infer an Aito table schema from a file')
        if not sub_commands:
            sub_commands = self._default_sub_commands
        self._sub_commands_map = {cmd.name: cmd for cmd in sub_commands}

    def build_parser(self, parser):
        parser.epilog = '''To see help for a specific format:
  aito infer-table-schema <input-format> - h

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
