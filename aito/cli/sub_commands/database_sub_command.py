import tempfile
from os import unlink, environ
from typing import Dict, List

from aito.utils.data_frame_handler import DataFrameHandler
from aito.schema import AitoTableSchema
from aito.client import AitoClient, BaseError
from .sub_command import SubCommand
from ..parser import PathArgType, InputArgType, ParseError, ArgParser, prompt_confirmation, \
    load_json_from_parsed_input_arg, create_client_from_parsed_args, create_sql_connecting_from_parsed_args, get_credentials_file_config, write_credentials_file_profile
import getpass


class ConfigureSubCommand(SubCommand):
    def __init__(self):
        super().__init__('configure', 'configure your Aito instance')

    def build_parser(self, parser):
        parser.add_argument(
            '--profile', type=str, default="default",
            help='the named profile for the config (default: default)'
        )

    def parse_and_execute(self, parsed_args: Dict):
        def mask(string):
            if string is None:
                return None
            if len(string) <= 4:
                return '****'
            return (len(string) - 4) * '*' + string[-4:]

        def get_existing_credentials():
            config = get_credentials_file_config()
            profile = parsed_args['profile']
            if profile not in config.sections():
                return None, None
            else:
                return config.get(profile, 'instance_url', fallback=None), config.get(profile, 'api_key', fallback=None)

        def prompt(name, existing_value):
            masked_existing_value = mask(existing_value)
            new_value = getpass.getpass(prompt=f'{name} [{masked_existing_value}]: ')
            if not new_value:
                if not existing_value:
                    raise ParseError(f'{name} must not be empty')
                else:
                    new_value = existing_value
            return new_value

        existing_instance_url, existing_api_key = get_existing_credentials()
        new_instance_url = prompt('instance url', existing_instance_url)
        new_api_key = prompt('api key', existing_api_key)

        try:
            AitoClient(instance_url=new_instance_url, api_key=new_api_key, check_credentials=True)
        except BaseError:
            raise ParseError('invalid credentials, please try again')

        write_credentials_file_profile(parsed_args['profile'], new_instance_url, new_api_key)


class QuickAddTableSubCommand(SubCommand):
    def __init__(self):
        super().__init__('quick-add-table', 'infer schema, create table, and upload a file to Aito')

    def build_parser(self, parser):
        parser.add_aito_default_credentials_arguments()
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

        inferred_schema = AitoTableSchema.infer_from_pandas_data_frame(converted_df)
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
        parser.add_aito_default_credentials_arguments()
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
        parser.add_aito_default_credentials_arguments()
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
        parser.add_aito_default_credentials_arguments()
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
        parser.add_aito_default_credentials_arguments()
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
        parser.add_aito_default_credentials_arguments()

    def parse_and_execute(self, parsed_args: Dict):
        client = create_client_from_parsed_args(parsed_args)
        tables = client.get_existing_tables()
        print(*sorted(tables), sep='\n')
        pass


class DeleteDatabaseSubCommand(SubCommand):
    def __init__(self):
        super().__init__('delete-database', 'delete the whole database')

    def build_parser(self, parser):
        parser.add_aito_default_credentials_arguments()

    def parse_and_execute(self, parsed_args: Dict):
        client = create_client_from_parsed_args(parsed_args)
        if prompt_confirmation('Confirm delete the whole database? The action is irreversible', False):
            client.delete_database()


class UploadEntriesSubCommand(SubCommand):
    def __init__(self):
        super().__init__('upload-entries', 'upload a list of table entries to an existing table')

    def build_parser(self, parser):
        parser.add_aito_default_credentials_arguments()
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
        parser.add_aito_default_credentials_arguments()
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
        parser.add_aito_default_credentials_arguments()
        parser.add_sql_default_credentials_arguments()
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
        parser.add_aito_default_credentials_arguments()
        parser.add_sql_default_credentials_arguments()
        parser.add_argument('table-name', type=str, help='name of the table to be created and added data to')
        parser.add_argument('query', type=str, help='query to get the data from your SQL database')

    def parse_and_execute(self, parsed_args: Dict):
        table_name = parsed_args['table-name']
        client = create_client_from_parsed_args(parsed_args)
        connection = create_sql_connecting_from_parsed_args(parsed_args)

        result_df = connection.execute_query_and_save_result(parsed_args['query'])
        inferred_schema = AitoTableSchema.infer_from_pandas_data_frame(result_df)

        converted_tmp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson.gz', delete=False)
        DataFrameHandler().df_to_format(result_df, 'ndjson', converted_tmp_file.name, {'compression': 'gzip'})
        converted_tmp_file.close()

        client.create_table(table_name, inferred_schema.to_json_serializable())
        with open(converted_tmp_file.name, 'rb') as in_f:
            client.upload_binary_file(table_name, in_f)
        converted_tmp_file.close()
        unlink(converted_tmp_file.name)
        return 0
