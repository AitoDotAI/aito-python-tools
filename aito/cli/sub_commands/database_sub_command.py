import tempfile
from os import unlink, environ
from typing import Dict, List

from aito.sdk.data_frame_handler import DataFrameHandler
from aito.sdk.aito_schema import AitoTableSchema
from aito.sdk.aito_client import AitoClient, BaseError
from .sub_command import SubCommand
from ..parser import PathArgType, InputArgType, ParseError, ArgParser
from ..parser_utils import create_client_from_parsed_args, load_json_from_parsed_input_arg, \
    prompt_confirmation, create_sql_connecting_from_parsed_args
import getpass


class LoginSubCommand(SubCommand):
    def __init__(self):
        super().__init__('login', 'login to your Aito instance')

    def build_parser(self, parser):
        pass

    def parse_and_execute(self, parsed_args: Dict):
        if environ.get('AITO_INSTANCE_URL') is not None or environ.get('AITO_API_KEY') is not None:
            if not prompt_confirmation(
                "Aito credentials environment variables already exists, do you want to overwrite?"
            ):
                return 0

        instance_url = getpass.getpass(prompt='instance url: ')
        api_key = getpass.getpass(prompt='api key: ')
        try:
            AitoClient(instance_url=instance_url, api_key=api_key, check_credentials=True)
        except BaseError:
            raise ParseError('invalid credentials, please try again')
        environ['AITO_INSTANCE_URL'] = instance_url
        environ['AITO_API_KEY'] = api_key


class QuickAddTableSubCommand(SubCommand):
    def __init__(self):
        super().__init__('quick-add-table', 'infer schema, create table, and upload a file to Aito')

    def build_parser(self, parser):
        parser.add_argument(
            '-n', '--table-name', type=str,
            help='name of the table to be created (default: the input file name without the extension)')
        file_format_choices = ['infer'] + DataFrameHandler.allowed_format
        parser.add_argument(
            '-f', '--file-format', type=str, choices=file_format_choices, default='infer',
            help='specify the input file format (default: infer from the file extension)')
        parser.add_argument('input-file', type=PathArgType(must_exist=True), help="path to the input file")
        return parser

    def parse_and_execute(self, parsed_args: Dict):
        df_handler = DataFrameHandler()
        in_f_path = parsed_args['input-file']
        in_format = in_f_path.suffixes[0].replace('.', '') \
            if parsed_args['file_format'] == 'infer' else parsed_args['file_format']

        if in_format not in df_handler.allowed_format:
            raise ParseError(f'failed to infer file {in_f_path} format. '
                             f'Please give the exact file format instead of `infer`')

        table_name = parsed_args['table_name'] if parsed_args['table_name'] else in_f_path.stem
        converted_tmp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson.gz', delete=False)
        convert_options = {
            'read_input': in_f_path,
            'write_output': converted_tmp_file.name,
            'in_format': in_format,
            'out_format': 'ndjson',
            'convert_options': {'compression': 'gzip'}
        }
        converted_df = df_handler.convert_file(**convert_options)
        converted_tmp_file.close()

        inferred_schema = AitoTableSchema.infer_from_pandas_dataframe(converted_df)
        client = create_client_from_parsed_args(parsed_args)
        client.create_table(table_name, inferred_schema.to_json_serializable())

        with open(converted_tmp_file.name, 'rb') as in_f:
            client.upload_binary_file(table_name, in_f)
        converted_tmp_file.close()
        unlink(converted_tmp_file.name)
        return 0


class CreateTableSubCommand(SubCommand):
    def __init__(self):
        super().__init__('create-table', 'create a table using the input table schema')

    def build_parser(self, parser):
        parser.add_argument('table-name', type=str, help="name of the table to be created")
        parser.add_argument(
            'input', default='-', type=InputArgType(), nargs='?',
            help="path to the schema file (when no file is given or when input is -, read from the standard input)")

    def parse_and_execute(self, parsed_args: Dict):
        client = create_client_from_parsed_args(parsed_args)
        table_name = parsed_args['table-name']
        table_schema = load_json_from_parsed_input_arg(parsed_args['input'], 'table schema')
        client.create_table(table_name, table_schema)
        return 0


class DeleteTableSubCommand(SubCommand):
    def __init__(self):
        super().__init__('delete-table', 'delete a table schema and all content inside the table')

    def build_parser(self, parser):
        parser.add_argument('table-name', type=str, help="the name of the table to be deleted")

    def parse_and_execute(self, parsed_args: Dict):
        client = create_client_from_parsed_args(parsed_args)
        table_name = parsed_args['table-name']
        if prompt_confirmation(f'Confirm delete table `{table_name}`? The action is irreversible', False):
            client.delete_table(table_name)
        return 0


class CopyTableSubCommand(SubCommand):
    def __init__(self):
        super().__init__('copy-table', 'copy a table')

    def build_parser(self, parser):
        parser.add_argument('table-name', type=str, help="the name of the table to be copied")
        parser.add_argument('copy-table-name', type=str, help="the name of the new copy table")
        parser.add_argument(
            '--replace', action='store_true',
            help="replace an existing table of which name is the name of the copy table"
        )

    def parse_and_execute(self, parsed_args: Dict):
        client = create_client_from_parsed_args(parsed_args)
        client.copy_table(parsed_args['table-name'], parsed_args['copy-table-name'], parsed_args['replace'])
        return 0


class RenameTableSubCommand(SubCommand):
    def __init__(self):
        super().__init__('rename-table', 'rename a table')

    def build_parser(self, parser):
        parser.add_argument('old-name', type=str, help="the name of the table to be renamed")
        parser.add_argument('new-name', type=str, help="the new name of the table")
        parser.add_argument(
            '--replace', action='store_true',
            help="replace an existing table of which name is the new name"
        )

    def parse_and_execute(self, parsed_args: Dict):
        client = create_client_from_parsed_args(parsed_args)
        client.rename_table(parsed_args['old-name'], parsed_args['new-name'], parsed_args['replace'])
        return 0


class ShowTablesSubCommand(SubCommand):
    def __init__(self):
        super().__init__('show-tables', 'show the existing tables in the Aito instance')

    def build_parser(self, parser):
        pass

    def parse_and_execute(self, parsed_args: Dict):
        client = create_client_from_parsed_args(parsed_args)
        tables = client.get_existing_tables()
        print(*sorted(tables), sep='\n')
        pass


class DeleteDatabaseSubCommand(SubCommand):
    def __init__(self):
        super().__init__('delete-database', 'delete the whole database')

    def build_parser(self, parser):
        pass

    def parse_and_execute(self, parsed_args: Dict):
        client = create_client_from_parsed_args(parsed_args)
        if prompt_confirmation('Confirm delete the whole database? The action is irreversible', False):
            client.delete_database()


class UploadEntriesSubCommand(SubCommand):
    def __init__(self):
        super().__init__('upload-entries', 'upload a list of table entries to an existing table')

    def build_parser(self, parser):
        parser.epilog = 'With no input, or when input is -, read table content from standard input'
        parser.add_argument('table-name', type=str, help='name of the table to be added data to')
        parser.add_argument(
            'input', default='-', type=InputArgType(), nargs='?',
            help="path to the entries file (when no file is given or when input is -, read from the standard input)")

    def parse_and_execute(self, parsed_args: Dict):
        client = create_client_from_parsed_args(parsed_args)
        table_name = parsed_args['table-name']
        table_entries = load_json_from_parsed_input_arg(parsed_args['input'])
        client.upload_entries(table_name=table_name, entries=table_entries)
        return 0


class UploadBatchSubCommand(UploadEntriesSubCommand):
    def __init__(self):
        super().__init__()
        self.name = 'upload-batch'

    def parse_and_execute(self, parsed_args: Dict):
        raise DeprecationWarning('This feature is deprecated. Use upload-entries instead.')


class UploadFileSubCommand(SubCommand):
    def __init__(self):
        super().__init__('upload-file', 'upload a file to an existing table')

    def build_parser(self, parser):
        parser.add_argument('table-name', type=str, help='name of the table to be added data to')
        parser.add_argument('input-file', type=PathArgType(must_exist=True), help="path to the input file")
        file_format_choices = ['infer'] + DataFrameHandler.allowed_format
        parser.add_argument(
            '-f', '--file-format', type=str, choices=file_format_choices,
            default='infer', help='specify input file format (default: infer from the file extension)')

    def parse_and_execute(self, parsed_args: Dict):
        client = create_client_from_parsed_args(parsed_args)
        df_handler = DataFrameHandler()
        table_name = parsed_args['table-name']
        in_file_path = parsed_args['input-file']
        in_format = in_file_path.suffixes[0].replace('.', '') if parsed_args['file_format'] == 'infer' \
            else parsed_args['file_format']
        if in_format not in df_handler.allowed_format:
            raise ParseError(f'failed to infer file {in_file_path} format. '
                             f'Please give the exact file format instead of `infer`')
        converted_tmp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson.gz', delete=False)
        convert_options = {
            'read_input': in_file_path,
            'write_output': converted_tmp_file.name,
            'in_format': in_format,
            'out_format': 'ndjson',
            'convert_options': {'compression': 'gzip'},
            'use_table_schema': client.get_table_schema(table_name)
        }
        df_handler = DataFrameHandler()
        df_handler.convert_file(**convert_options)
        converted_tmp_file.close()

        with open(converted_tmp_file.name, 'rb') as in_f:
            client.upload_binary_file(table_name, in_f)
        converted_tmp_file.close()
        unlink(converted_tmp_file.name)


class UploadDataFromSQLSubCommand(SubCommand):
    def __init__(self):
        super().__init__('upload-data-from-sql', 'populate the result of a SQL query to an existing table')

    def build_parser(self, parser):
        parser.add_sql_default_credentials_arguments(add_use_env_arg=False)
        parser.add_argument('table-name', type=str, help='name of the table to be added data to')
        parser.add_argument('query', type=str, help='query to get the data from your SQL database')

    def parse_and_execute(self, parsed_args: Dict):
        connection = create_sql_connecting_from_parsed_args(parsed_args)
        client = create_client_from_parsed_args(parsed_args)
        table_name = parsed_args['table-name']

        result_df = connection.execute_query_and_save_result(parsed_args['query'])
        converted_tmp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson.gz', delete=False)
        DataFrameHandler().df_to_format(result_df, 'ndjson', converted_tmp_file.name, {'compression': 'gzip'})
        converted_tmp_file.close()

        with open(converted_tmp_file.name, 'rb') as in_f:
            client.upload_binary_file(table_name, in_f)
        converted_tmp_file.close()
        unlink(converted_tmp_file.name)
        return 0


class QuickAddTableFromSQLSubCommand(SubCommand):
    def __init__(self):
        super().__init__(
            'quick-add-table-from-sql', 'infer schema, create table, and upload the result of a SQL query to Aito'
        )

    def build_parser(self, parser):
        parser.add_sql_default_credentials_arguments(add_use_env_arg=False)
        parser.add_argument('table-name', type=str, help='name of the table to be created and added data to')
        parser.add_argument('query', type=str, help='query to get the data from your SQL database')

    def parse_and_execute(self, parsed_args: Dict):
        table_name = parsed_args['table-name']
        client = create_client_from_parsed_args(parsed_args)
        connection = create_sql_connecting_from_parsed_args(parsed_args)

        result_df = connection.execute_query_and_save_result(parsed_args['query'])
        inferred_schema = AitoTableSchema.infer_from_pandas_dataframe(result_df)

        converted_tmp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson.gz', delete=False)
        DataFrameHandler().df_to_format(result_df, 'ndjson', converted_tmp_file.name, {'compression': 'gzip'})
        converted_tmp_file.close()

        client.create_table(table_name, inferred_schema.to_json_serializable())
        with open(converted_tmp_file.name, 'rb') as in_f:
            client.upload_binary_file(table_name, in_f)
        converted_tmp_file.close()
        unlink(converted_tmp_file.name)
        return 0


class DatabaseSubCommand(SubCommand):
    _default_sub_sub_commands = [
        LoginSubCommand(),
        QuickAddTableSubCommand(),
        CreateTableSubCommand(),
        DeleteTableSubCommand(),
        CopyTableSubCommand(),
        RenameTableSubCommand(),
        ShowTablesSubCommand(),
        DeleteDatabaseSubCommand(),
        UploadEntriesSubCommand(),
        UploadBatchSubCommand(),
        UploadFileSubCommand(),
        UploadDataFromSQLSubCommand(),
        QuickAddTableFromSQLSubCommand()
    ]

    def __init__(self, sub_sub_commands: List[SubCommand] = None):
        super().__init__('database', 'perform operations with your Aito database instance')
        if not sub_sub_commands:
            sub_sub_commands = self._default_sub_sub_commands
        self._sub_sub_commands_map = {cmd.name: cmd for cmd in sub_sub_commands}

    def build_parser(self, parser):
        parser.add_aito_default_credentials_arguments()
        parser.epilog = '''To see help for a specific operation:
  aitodb <operation> -h
'''
        sub_sub_commands_subparsers = parser.add_subparsers(
            title='operation',
            dest='operation',
            metavar='<operation>',
            parser_class=ArgParser
        )
        sub_sub_commands_subparsers.required = True

        for sub_cmd in self._sub_sub_commands_map.values():
            sub_sub_cmd_parser = sub_sub_commands_subparsers.add_parser(sub_cmd.name, help=sub_cmd.help_message)
            sub_cmd.build_parser(sub_sub_cmd_parser)

    def parse_and_execute(self, parsed_args: Dict):
        sub_sub_command_name = parsed_args['operation']
        self._sub_sub_commands_map[sub_sub_command_name].parse_and_execute(parsed_args)
        return 0
