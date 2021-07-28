import json
import tempfile
from abc import ABC, abstractmethod
from os import unlink
from typing import Dict

import aito.api as api
from aito.cli.parser import PathArgType, InputArgType, ParseError, prompt_confirmation, \
    load_json_from_parsed_input_arg, create_client_from_parsed_args, create_sql_connecting_from_parsed_args
from aito.cli.sub_commands.sub_command import SubCommand
from aito.client import AitoClient, Error
from aito.schema import AitoTableSchema
from aito.utils._credentials_file_utils import get_existing_credentials, write_credentials_file_profile, \
    mask_instance_url, mask_api_key
from aito.utils.data_frame_handler import DataFrameHandler


class ConfigureSubCommand(SubCommand):
    def __init__(self):
        super().__init__('configure', 'configure your Aito instance')

    def build_parser(self, parser):
        parser.add_argument(
            '--profile', type=str, default="default",
            help='the named profile for the config (default: default)'
        )

    def parse_and_execute(self, parsed_args: Dict):
        profile = parsed_args['profile']
        existing_instance_url, existing_api_key = get_existing_credentials(profile_name=profile)
        if existing_instance_url is not None or existing_api_key is not None:
            if not prompt_confirmation(f'Profile {profile} already exists. Do you want to replace?'):
                return 0

        input_ret_val = input(
            'instance url (i.e: https://instance_name.aito.app): ' if not existing_instance_url
            else f'instance url [{mask_instance_url(existing_instance_url)}]'
        )
        new_instance_url = input_ret_val if input_ret_val else existing_instance_url
        if not new_instance_url:
            raise ParseError('instance url must not be empty')

        input_ret_val = input(
            'api key [None]' if not existing_instance_url else f'api key [{mask_api_key(existing_api_key)}]'
        )
        new_api_key = input_ret_val if input_ret_val else existing_api_key
        if not new_api_key:
            raise ParseError('api_key must not be empty')

        try:
            AitoClient(instance_url=new_instance_url, api_key=new_api_key, check_credentials=True)
        except Error:
            raise ParseError('invalid credentials, please try again')

        write_credentials_file_profile(parsed_args['profile'], new_instance_url, new_api_key)


class QuickAddTableSubCommand(SubCommand):
    def __init__(self):
        super().__init__('quick-add-table', 'infer schema, create table, and upload a file to Aito')

    def build_parser(self, parser):
        parser.add_aito_default_credentials_arguments()
        parser.add_argument(
            '-n', '--table-name', type=str,
            help='name of the table to be created (default: the name of the input file)'
        )
        parser.add_argument(
            '-f', '--file-format', type=str, choices=DataFrameHandler.allowed_format,
            help='specify the input file format (default: the input file extension)'
        )
        parser.add_argument('input-file', type=PathArgType(must_exist=True), help="path to the input file")
        return parser

    def parse_and_execute(self, parsed_args: Dict):
        in_f_path = parsed_args['input-file']
        in_format = parsed_args.get('file_format')
        table_name = parsed_args.get('table_name')
        client = create_client_from_parsed_args(parsed_args)

        api.quick_add_table(client=client, input_file=in_f_path, input_format=in_format, table_name=table_name)
        return 0


class CreateDatabaseSubCommand(SubCommand):
    def __init__(self):
        super().__init__('create-database', 'create the database using the input database schema')

    def build_parser(self, parser):
        parser.add_aito_default_credentials_arguments()
        parser.add_argument(
            'input', default='-', type=InputArgType(), nargs='?',
            help="path to the schema file (when no file is given or when input is -, read from the standard input)")

    def parse_and_execute(self, parsed_args: Dict):
        client = create_client_from_parsed_args(parsed_args)
        database_schema = load_json_from_parsed_input_arg(parsed_args['input'], 'database schema')
        api.create_database(client=client, schema=database_schema)
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
        api.create_table(client=client, table_name=table_name, schema=table_schema)
        return 0


class GetTableSubCommand(SubCommand):
    def __init__(self):
        super().__init__('get-table', 'return the schema of the specified table')

    def build_parser(self, parser):
        parser.add_aito_default_credentials_arguments()
        parser.add_argument('table-name', type=str, help="name of the table")

    def parse_and_execute(self, parsed_args: Dict):
        client = create_client_from_parsed_args(parsed_args)
        table_name = parsed_args['table-name']
        print(api.get_table_schema(client, table_name).to_json_string(indent=2))
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
            api.delete_table(client, table_name)
        return 0


class CopyTableSubCommand(SubCommand):
    def __init__(self):
        super().__init__('copy-table', 'copy a table')

    def build_parser(self, parser):
        parser.add_aito_default_credentials_arguments()
        parser.add_argument('table-name', type=str, help="the name of the table to be copied")
        parser.add_argument('copy-table-name', type=str, help="the name of the new copy table")
        parser.add_argument('--replace', action='store_true', help="allow the replacement of an existing table")

    def parse_and_execute(self, parsed_args: Dict):
        client = create_client_from_parsed_args(parsed_args)
        api.copy_table(client, parsed_args['table-name'], parsed_args['copy-table-name'], parsed_args['replace'])
        return 0


class RenameTableSubCommand(SubCommand):
    def __init__(self):
        super().__init__('rename-table', 'rename a table')

    def build_parser(self, parser):
        parser.add_aito_default_credentials_arguments()
        parser.add_argument('old-name', type=str, help="the name of the table to be renamed")
        parser.add_argument('new-name', type=str, help="the new name of the table")
        parser.add_argument('--replace', action='store_true', help="allow the replacement of an existing table")

    def parse_and_execute(self, parsed_args: Dict):
        client = create_client_from_parsed_args(parsed_args)
        api.rename_table(client, parsed_args['old-name'], parsed_args['new-name'], parsed_args['replace'])
        return 0


class ShowTablesSubCommand(SubCommand):
    def __init__(self):
        super().__init__('show-tables', 'show the existing tables in the Aito instance')

    def build_parser(self, parser):
        parser.add_aito_default_credentials_arguments()

    def parse_and_execute(self, parsed_args: Dict):
        client = create_client_from_parsed_args(parsed_args)
        tables = api.get_existing_tables(client)
        print(*sorted(tables), sep='\n')
        pass


class GetDatabaseSubCommand(SubCommand):
    def __init__(self):
        super().__init__('get-database', 'return the schema of the database')

    def build_parser(self, parser):
        parser.add_aito_default_credentials_arguments()

    def parse_and_execute(self, parsed_args: Dict):
        client = create_client_from_parsed_args(parsed_args)
        print(api.get_database_schema(client).to_json_string(indent=2))
        return 0


class DeleteDatabaseSubCommand(SubCommand):
    def __init__(self):
        super().__init__('delete-database', 'delete the whole database')

    def build_parser(self, parser):
        parser.add_aito_default_credentials_arguments()

    def parse_and_execute(self, parsed_args: Dict):
        client = create_client_from_parsed_args(parsed_args)
        if prompt_confirmation('Confirm delete the whole database? The action is irreversible', False):
            api.delete_database(client)


class UploadEntriesSubCommand(SubCommand):
    def __init__(self):
        super().__init__('upload-entries', 'upload a list of table entries to an existing table')

    def build_parser(self, parser):
        parser.add_aito_default_credentials_arguments()
        parser.epilog = 'With no input, or when input is -, read table content from standard input'
        parser.add_argument('table-name', type=str, help='name of the table where to add data')
        parser.add_argument(
            'input', default='-', type=InputArgType(), nargs='?',
            help="path to the entries file (when no file is given or when input is -, read from the standard input)")

    def parse_and_execute(self, parsed_args: Dict):
        client = create_client_from_parsed_args(parsed_args)
        table_name = parsed_args['table-name']
        table_entries = load_json_from_parsed_input_arg(parsed_args['input'])
        api.upload_entries(client, table_name=table_name, entries=table_entries)
        return 0

class OptimizeTableSubCommand(SubCommand):
    def __init__(self):
        super().__init__('optimize-table', 'optimize an existing table')

    def build_parser(self, parser):
        parser.add_aito_default_credentials_arguments()
        parser.add_argument('table-name', type=str, help='name of the table to optimize')

    def parse_and_execute(self, parsed_args: Dict):
        client = create_client_from_parsed_args(parsed_args)
        table_name = parsed_args['table-name']
        api.optimize_table(client, table_name=table_name)
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
            'use_table_schema': api.get_table_schema(client, table_name)
        }
        df_handler = DataFrameHandler()
        df_handler.convert_file(**convert_options)
        converted_tmp_file.close()

        with open(converted_tmp_file.name, 'rb') as in_f:
            api.upload_binary_file(client=client, table_name=table_name, binary_file=in_f)
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
            api.upload_binary_file(client=client, table_name=table_name, binary_file=in_f)
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

        api.create_table(client, table_name, inferred_schema)
        with open(converted_tmp_file.name, 'rb') as in_f:
            api.upload_binary_file(client=client, table_name=table_name, binary_file=in_f)
        converted_tmp_file.close()
        unlink(converted_tmp_file.name)
        return 0


class QuickPredictSubCommand(SubCommand):
    def __init__(self):
        super().__init__(
            'quick-predict', 'generate an example predict query to predict a field'
        )

    def build_parser(self, parser):
        parser.add_aito_default_credentials_arguments()
        parser.add_argument(
            'from-table', type=str, help='the name of the table the will be use as context for prediction'
        )
        parser.add_argument('predicting-field', type=str, help='the name of the predicting field')
        parser.add_argument('--evaluate', action='store_true', help="get the accuracy of the example prediction query")

    def parse_and_execute(self, parsed_args: Dict):
        from_table = parsed_args['from-table']
        predicting_field = parsed_args['predicting-field']
        client = create_client_from_parsed_args(parsed_args)

        predict_query, evaluate_query = api.quick_predict_and_evaluate(
            client=client, from_table=from_table, predicting_field=predicting_field
        )
        print("[Predict Query Example]")
        print(json.dumps(predict_query, indent=2))

        if parsed_args['evaluate']:
            evaluate_result = api.evaluate(client=client, query=evaluate_query)
            print("[Evaluation Result]")
            print(f"- Train samples count: {evaluate_result.train_sample_count}")
            print(f"- Test samples count: {evaluate_result.test_sample_count}")
            print(f"- Accuracy: {evaluate_result.accuracy}")

        return 0


class QueryToEndpointSubCommand(SubCommand, ABC):
    @property
    @abstractmethod
    def api_method_name(self) -> str:
        pass

    @property
    def endpoint_name(self) -> str:
        return self.api_method_name.replace('_', '-')

    def __init__(self):
        super().__init__(self.endpoint_name, f'send a query to the {self.api_method_name.upper()} API')

    def build_parser(self, parser):
        parser.add_aito_default_credentials_arguments()
        parser.add_argument('query', type=str, help='the query to be sent')
        parser.add_argument(
            '--use-job', action='store_true',
            help='use job for query that takes longer than 30s (default: False)'
        )

    def parse_and_execute(self, parsed_args: Dict):
        client = create_client_from_parsed_args(parsed_args)

        query_str = parsed_args['query']
        query = json.loads(query_str)
        use_job = parsed_args['use_job']

        client_method = getattr(api, self.api_method_name)
        resp = client_method(client=client, query=query, use_job=use_job)

        print(resp.to_json_string(indent=2))
        return 0


SearchSubCommand = type('SearchSubCommand', (QueryToEndpointSubCommand,), {'api_method_name': 'search'})
PredictSubCommand = type('PredictSubCommand', (QueryToEndpointSubCommand,), {'api_method_name': 'predict'})
RecommendSubCommand = type('RecommendSubCommand', (QueryToEndpointSubCommand,), {'api_method_name': 'recommend'})
EvaluateSubCommand = type('EvaluateSubCommand', (QueryToEndpointSubCommand,), {'api_method_name': 'evaluate'})
SimilaritySubCommand = type('SimilaritySubCommand', (QueryToEndpointSubCommand,), {'api_method_name': 'similarity'})
MatchSubCommand = type('MatchSubCommand', (QueryToEndpointSubCommand,), {'api_method_name': 'match'})
RelateSubCommand = type('RelateSubCommand', (QueryToEndpointSubCommand,), {'api_method_name': 'relate'})
GenericQuerySubCommand = type(
    'GenericQuerySubCommand', (QueryToEndpointSubCommand,), {'api_method_name': 'generic_query'}
)
