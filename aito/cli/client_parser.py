import argparse
import json
import os
from abc import abstractmethod
import sys
from dotenv import load_dotenv

from aito.cli.parser import AitoParser
from aito.client.aito_client import AitoClient
from aito.schema.data_frame_converter import DataFrameConverter


class ClientParser:
    def __init__(self):
        self.client = None
        self.parser = AitoParser(formatter_class=argparse.RawTextHelpFormatter,
                                 description='setup and perform a task with the aito client',
                                 usage='''
        aito.py client [<client-options>] <task> [<task-options>]
        
        To see help for a specific task, you can run:
        aito.py client <task> -h
        ''',
                                 epilog='''example:
        python aito.py client upload-batch myTable < myFile.json
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

        client_action_parser = self.client_task_parsers[parsed_args['task']]
        client_action_parser.parse_and_execute(parsing_args, client_args)
        return 0


class ClientTaskParser:
    def __init__(self, client_parser: AitoParser, task: str):
        self.parser = AitoParser(formatter_class=argparse.RawTextHelpFormatter,
                                 parents=[client_parser])
        self.task = task
        self.usage_prefix = f"python aito.py client <client-options> {task} [<{task}-options>]"
        self.optional_args = self.parser.add_argument_group(f"optional {task} arguments")

    @abstractmethod
    def parse_and_execute(self, parsing_args, client_args):
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
        self.parser.add_argument('table-name', type=str, help="name of the table to be populated")
        self.optional_args.add_argument('input', default='-', type=str, nargs='?', help="input file or stream")

    def parse_and_execute(self, parsing_args, client_args) -> int:
        client = AitoClient(**client_args)
        parsed_args = vars(self.parser.parse_args(parsing_args))
        table_name = parsed_args['table-name']
        if parsed_args['input'] == '-':
            table_content = json.load(sys.stdin)
        else:
            input_path = self.parser.check_valid_path(parsed_args['input'])
            print(input_path)
            with input_path.open() as f:
                table_content = json.load(f)
        client.populate_table_entries(table_name, table_content)
        return 0


class UploadFileParser(ClientTaskParser):
    def __init__(self, client_parser: AitoParser):
        super().__init__(client_parser, 'upload-file')
        self.parser.description = 'populating a file content to a table'
        self.parser.usage = f"{self.usage_prefix} <table-name> <file-path"
        self.parser.add_argument('table-name', type=str, help="name of the table to be populated")
        self.parser.add_argument('file-path', type=str, help="path to the input file")
        self.optional_args.add_argument('-i', '--infer-table-schema', action='store_true',
                                        help='infer a table schema from file content and use it as the schema of the '
                                             'uploading table (table must not exist in the instance)')
        self.optional_args.add_argument('-f', '--file-format',
                                        type=str, choices=['csv', 'xlsx', 'json', 'ndjson', 'infer'], default='infer',
                                        help=''''input file format (default: infer file format from file-path extension)
                                        The file is converted to ndjson.gz with default convert options.
                                        ''')
        self.optional_args.add_argument('-s', '--use-table-schema', metavar='schema-input-file', type=str,
                                        help='convert the file content according to the input schema and use it as the '
                                             'schema of the uploading table (table must not exist in the instance)')

    def parse_and_execute(self, parsing_args, client_args) -> int:
        client = AitoClient(**client_args)
        parsed_args = vars(self.parser.parse_args(parsing_args))
        table_name = parsed_args['table-name']
        input_file_path = self.parser.check_valid_path(parsed_args['file-path'])
        in_format = input_file_path.suffix.replace('.', '') if parsed_args['file_format'] == 'infer' \
            else parsed_args['file_format']
        if in_format not in DataFrameConverter.allowed_format:
            self.parser.error(f"Invalid input format {in_format}. Must be one of {DataFrameConverter.allowed_format}")

        out_file_path = input_file_path.parent / f"{input_file_path.stem}.ndjson.gz"
        schema_file_path = input_file_path.parent / f"{table_name}_schema.json"
        convert_options = {
            'read_input': input_file_path,
            'write_output': out_file_path,
            'in_format': in_format,
            'out_format': 'ndjson',
            'convert_options': {'compression': 'gzip'},
            'create_table_schema': schema_file_path if parsed_args['create_table_schema'] else None,
            'use_table_schema': self.parser.check_valid_path((parsed_args['use_table_schema']))
            if parsed_args['use_table_schema'] else None
        }
        converter = DataFrameConverter()
        converter.logger.info("Converting input file to ndjson.gz...")
        converter.convert_file(**convert_options)

        if parsed_args['use_table_schema']:
            with self.parser.check_valid_path((parsed_args['use_table_schema'])).open() as f:
                table_schema = json.load(f)
            client.put_table_schema(table_name, table_schema)
        elif parsed_args['infer_table_schema']:
            with (input_file_path.parent / f"{table_name}_schema.json").open() as f:
                table_schema = json.load(f)
            client.put_table_schema(table_name, table_schema)
        client.upload_file(input_file_path)
        return 0