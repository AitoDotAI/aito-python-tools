import argparse
import json
import os
from abc import abstractmethod
import sys
from dotenv import load_dotenv

from aito.cli.parser import AitoParser
from aito.client.aito_client import AitoClient
from aito.convert.data_frame_handler import DataFrameHandler
import datetime


class ClientParser:
    def __init__(self):
        self.client = None
        self.parser = AitoParser(formatter_class=argparse.RawTextHelpFormatter,
                                 description='setup and perform a task with the aito client',
                                 usage='''
        aito client [<client-options>] <task> [<task-options>]
        
        To see help for a specific task, you can run:
        aito client <task> -h
        ''',
                                 epilog='''example:
        aito client -u MY_AITO_INSTANCE_URL -r MY_RO_KEY -w MY_RW_KEY upload-batch myTable < myTableEntries.json
        aito client -e myAitoCredentials.env upload-file myTable myFile.csv
        aito client upload-file -c newTable myFile.xlsx
        ''',
                                 add_help=False)
        self.parser.add_argument('-e', '--use-env-file', type=str, metavar='env-file-path',
                                 help='set up the client using a .env file containing the required env variables')
        self.parser.add_argument('-r', '--read-only-key', type=str, default='.env',
                                 help='aito read-only API key (if not defined or when value is .env, '
                                      'use the AITO_RO_KEY env variable value)')
        self.parser.add_argument('-u', '--url', type=str, default='.env',
                                 help='aito instance url (if not defined or when value is .env, '
                                      'use the AITO_INSTANCE_URL env variable value)')
        self.parser.add_argument('-w', '--read-write-key', type=str, default='.env',
                                 help='aito read-write API key (if not defined or when value is .env, '
                                      'use the AITO_RW_KEY env variable value')
        self.parser.add_argument('task', choices=['upload-batch', 'upload-file'],
                                 help='perform a task with the client')
        self.client_task_parsers = {
            'upload-batch': UploadBatchParser(self.parser),
            'upload-file': UploadFileParser(self.parser)
        }

    def parse_and_execute(self, parsing_args) -> int:
        parsed_args, unknown = self.parser.parse_known_args(parsing_args)
        parsed_args = vars(parsed_args)
        client_action_parser = self.client_task_parsers[parsed_args['task']]
        client_action_parser.parse_and_execute(parsing_args)
        return 0


class ClientTaskParser:
    def __init__(self, client_parser: AitoParser, task: str):
        self.parser = AitoParser(formatter_class=argparse.RawTextHelpFormatter,
                                 parents=[client_parser])
        self.task = task
        self.usage_prefix = f"python aito client <client-options> {task} [<{task}-options>]"
        self.optional_args = self.parser.add_argument_group(f"optional {task} arguments")

    def parse_client_args(self, parsed_args):
        if parsed_args['use_env_file']:
            env_file_path = self.parser.check_valid_path(parsed_args['use_env_file'], True)
            load_dotenv(env_file_path)

        env_var = os.environ

        def get_env_variable(variable_name):
            if variable_name not in env_var:
                self.parser.error(f"{variable_name} env variable not found")
            return env_var[variable_name]

        client_args = {
            'url': get_env_variable('AITO_INSTANCE_URL') if parsed_args['url'] == '.env' else parsed_args['url'],
            'rw_key': get_env_variable('AITO_RW_KEY') if parsed_args['read_write_key'] == '.env'
            else parsed_args['read_write_key'],
            'ro_key': get_env_variable('AITO_RO_KEY') if parsed_args['read_only_key'] == '.env'
            else parsed_args['read_only_key']
        }
        return client_args

    @abstractmethod
    def parse_and_execute(self, parsing_args):
        pass


class UploadBatchParser(ClientTaskParser):
    def __init__(self, client_parser: AitoParser):
        super().__init__(client_parser, 'upload-batch')
        self.parser.description = '''populating contents to a table by batch.
        The content must be a json array of table entries.
        '''
        self.parser.usage = f'''
        {self.usage_prefix} <table-name> [input]
        With no input, or when input is -, read table content from standard input
        '''
        self.parser.epilog = '''example:
        aito client upload-batch myTable myTableEntries.json
        aito client upload-batch myTable < myTableEntries.json
        '''
        self.parser.add_argument('table-name', type=str, help="name of the table to be populated")
        self.optional_args.add_argument('input', default='-', type=str, nargs='?', help="input file or stream")

    def parse_and_execute(self, parsing_args) -> int:
        parsed_args = vars(self.parser.parse_args(parsing_args))
        client_args = self.parse_client_args(parsed_args)
        client = AitoClient(**client_args)
        table_name = parsed_args['table-name']
        if parsed_args['input'] == '-':
            table_content = json.load(sys.stdin)
        else:
            input_path = self.parser.check_valid_path(parsed_args['input'])
            with input_path.open() as f:
                table_content = json.load(f)
        client.populate_table_entries(table_name, table_content)
        return 0


class UploadFileParser(ClientTaskParser):
    def __init__(self, client_parser: AitoParser):
        super().__init__(client_parser, 'upload-file')
        self.parser.description = 'populating a file content to a table. If the file is not in gzip compressed ' \
                                  'ndjson format, a converted ndjson.gz file will be created at the same location.'
        self.parser.usage = f"{self.usage_prefix} <table-name> <file-path>"
        self.parser.epilog = '''example:
        aito client upload-file myExistingTable myFile.csv
        aito client upload-file -ck newTable myFile.json
        aito client upload-file -s correctSchema.json newTable myFile.json.gz
        '''
        self.parser.add_argument('table-name', type=str, help="name of the table to be populated")
        self.parser.add_argument('file-path', type=str, help="path to the input file")
        self.optional_args.add_argument('-c', '--create-table-schema', action='store_true',
                                        help='create an infered table schema at the same location of the input file '
                                             'and use it as the schema of the uploading table '
                                             '(table must not exist in the instance)')
        self.optional_args.add_argument('-f', '--file-format',
                                        type=str, choices=['csv', 'excel', 'json', 'ndjson', 'infer'], default='infer',
                                        help='input file format (default: infer file format from file-path extension)')
        self.optional_args.add_argument('-k', '--keep-generated-files', action='store_true',
                                        help='keep the converted ndjson.gz file and generated schema if applicable')
        self.optional_args.add_argument('-s', '--use-table-schema', metavar='schema-input-file', type=str,
                                        help='convert the file content according to the input schema and use it as the '
                                             'schema of the uploading table (table must not exist in the instance)')

    def parse_and_execute(self, parsing_args) -> int:
        parsed_args = vars(self.parser.parse_args(parsing_args))
        client_args = self.parse_client_args(parsed_args)
        client = AitoClient(**client_args)

        table_name = parsed_args['table-name']

        if table_name not in client.get_existing_tables() \
                and not parsed_args['use_table_schema'] and not parsed_args['create_table_schema']:
            self.parser.error(f"Table '{table_name}' does not exist. Please upload the table schema first, or use the "
                              f"--use-table-schema or --create-table-schema option")

        input_file_path = self.parser.check_valid_path(parsed_args['file-path'])

        in_format = input_file_path.suffixes[0].replace('.', '') if parsed_args['file_format'] == 'infer' \
            else parsed_args['file_format']
        if in_format not in DataFrameHandler.allowed_format:
            self.parser.error(f"Invalid input format {in_format}. Must be one of {DataFrameHandler.allowed_format}")

        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        converted_file_path = input_file_path.parent / f"{input_file_path.stem.split('.')[0]}_{now}.ndjson.gz"
        schema_file_path = input_file_path.parent / f"{table_name}_schema_{now}.json"
        convert_options = {
            'read_input': input_file_path,
            'write_output': converted_file_path,
            'in_format': in_format,
            'out_format': 'ndjson',
            'convert_options': {'compression': 'gzip'},
            'create_table_schema': schema_file_path if parsed_args['create_table_schema'] else None,
            'use_table_schema': self.parser.check_valid_path((parsed_args['use_table_schema']))
            if parsed_args['use_table_schema'] else None
        }
        df_handler = DataFrameHandler()

        if input_file_path.suffixes[:-2] != ['.ndjson', '.gz'] or parsed_args['create_table_schema'] or \
                parsed_args['use_table_schema']:
            df_handler.convert_file(**convert_options)
        else:
            converted_file_path = input_file_path

        if parsed_args['use_table_schema']:
            with self.parser.check_valid_path((parsed_args['use_table_schema'])).open() as f:
                table_schema = json.load(f)
            client.put_table_schema(table_name, table_schema)
        elif parsed_args['create_table_schema']:
            with schema_file_path.open() as f:
                table_schema = json.load(f)
            client.put_table_schema(table_name, table_schema)
        client.populate_table_by_file_upload(table_name, converted_file_path)

        if not parsed_args['keep_generated_files']:
            if schema_file_path.exists():
                schema_file_path.unlink()
            if converted_file_path.exists() and converted_file_path != input_file_path:
                converted_file_path.unlink()
        return 0
