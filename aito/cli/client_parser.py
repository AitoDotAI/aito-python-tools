import argparse
import json
import os
from abc import abstractmethod
import sys
from dotenv import load_dotenv

from aito.cli.parser import AitoArgParser, ParserWrapper
from aito.client.aito_client import AitoClient
from aito.convert.data_frame_handler import DataFrameHandler
import datetime


class ClientParserWrapper(ParserWrapper):
    def __init__(self):
        super().__init__(add_help=False)
        parser = self.parser
        parser.description = 'setup and perform a task with the aito client'
        parser.usage = '''
        aito client [<client-options>] <task> [<task-options>]
        
        To see help for a specific task, you can run:
        aito client <task> -h
        '''
        parser.epilog = '''the AITO_INSTANCE_URL should look similar to https://my-instance.api.aito.ai
        example:
        aito client -u MY_AITO_INSTANCE_URL -r MY_RO_KEY -w MY_RW_KEY upload-batch myTable < myTableEntries.json
        aito client -e myAitoCredentials.env upload-file myTable myFile.csv
        aito client upload-file myFile.xlsx
        '''
        parser.add_argument('-e', '--use-env-file', type=str, metavar='env-file-path',
                            help='set up the client using a .env file containing the required env variables')
        parser.add_argument('-r', '--read-only-key', type=str, default='.env',
                            help='aito read-only API key (if not defined or when value is .env, '
                                 'use the AITO_RO_KEY env variable value)')
        parser.add_argument('-u', '--url', type=str, default='.env',
                            help='aito instance url (if not defined or when value is .env, '
                                 'use the AITO_INSTANCE_URL env variable value)')
        parser.add_argument('-w', '--read-write-key', type=str, default='.env',
                            help='aito read-write API key (if not defined or when value is .env, '
                                 'use the AITO_RW_KEY env variable value')
        parser.add_argument('task', choices=['upload-batch', 'upload-file'], help='perform a task with the client')
        self.client_task_parsers = {
            'upload-batch': UploadBatchParserWrapper(self.parser),
            'upload-file': UploadFileParserWrapper(self.parser)
        }

    def parse_and_execute(self, parsing_args) -> int:
        parsed_args, unknown = self.parser.parse_known_args(parsing_args)
        parsed_args = vars(parsed_args)
        client_action_parser = self.client_task_parsers[parsed_args['task']]
        client_action_parser.parse_and_execute(parsing_args)
        return 0


class ClientTaskParserWrapper(ParserWrapper):
    def __init__(self, parent_parser: AitoArgParser, task: str):
        super().__init__()
        self.parser = AitoArgParser(formatter_class=argparse.RawTextHelpFormatter,
                                    parents=[parent_parser])
        self.task = task
        self.usage_prefix = f"python aito client <client-options> {task} [<{task}-options>]"
        self.optional_args = self.parser.add_argument_group(f"optional {task} arguments")

    def get_client_args_from_parsed_args(self, parsed_args):
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


class UploadBatchParserWrapper(ClientTaskParserWrapper):
    def __init__(self, parent_parser: AitoArgParser):
        super().__init__(parent_parser, 'upload-batch')
        parser = self.parser
        parser.description = '''populating contents to a table by batch.
        The content must be a JSON array of table entries.
        '''
        parser.usage = f'''
        {self.usage_prefix} <table-name> [input]
        With no input, or when input is -, read table content from standard input
        '''
        parser.epilog = '''example:
        aito client upload-batch myTable myTableEntries.json
        aito client upload-batch myTable < myTableEntries.json
        '''
        parser.add_argument('table-name', type=str, help="name of the table to be populated")
        self.optional_args.add_argument('input', default='-', type=str, nargs='?',
                                        help="input file or stream in JSON array format")

    def parse_and_execute(self, parsing_args) -> int:
        parsed_args = vars(self.parser.parse_args(parsing_args))
        client_args = self.get_client_args_from_parsed_args(parsed_args)
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


class UploadFileParserWrapper(ClientTaskParserWrapper):
    def __init__(self, parent_parser: AitoArgParser):
        super().__init__(parent_parser, 'upload-file')
        parser = self.parser
        parser.description = 'populating a file content to a table. If the file is not in gzip compressed' \
                             'ndjson format, a converted ndjson.gz file will be created at the same location.'
        parser.usage = f"{self.usage_prefix} <table-name> <file-path>"
        parser.epilog = '''example:
        aito client upload-file tableName myFile.csv
        aito client upload-file -k tableName myFile.json
        '''
        parser.add_argument('table-name', type=str, help="name of the table to be populated")
        parser.add_argument('file-path', type=str, help="path to the input file")
        self.optional_args.add_argument('-f', '--file-format',
                                        type=str, choices=['infer', 'csv', 'excel', 'json', 'ndjson'], default='infer',
                                        help='specify input file format if it is not ndjson.gzip '
                                             '(default: infer file format from file-path extension)')
        self.optional_args.add_argument('-k', '--keep-generated-files', action='store_true',
                                        help='keep the converted ndjson.gz file if applicable')

    def parse_and_execute(self, parsing_args) -> int:
        parsed_args = vars(self.parser.parse_args(parsing_args))
        client_args = self.get_client_args_from_parsed_args(parsed_args)
        client = AitoClient(**client_args)

        table_name = parsed_args['table-name']
        input_file_path = self.parser.check_valid_path(parsed_args['file-path'])

        in_format = input_file_path.suffixes[0].replace('.', '') if parsed_args['file_format'] == 'infer' \
            else parsed_args['file_format']
        if in_format not in DataFrameHandler.allowed_format:
            self.parser.error(f"Invalid input format {in_format}. Must be one of {DataFrameHandler.allowed_format}")

        is_gzipped = True if (input_file_path.suffixes[-1] == 'gz') else False

        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if input_file_path != 'ndjson':
            converted_file_path = input_file_path.parent / f"{input_file_path.stem}_{now}.ndjson.gz"
            convert_options = {
                'read_input': input_file_path,
                'write_output': converted_file_path,
                'in_format': in_format,
                'out_format': 'ndjson',
                'convert_options': {'compression': 'gzip'}
            }
            df_handler = DataFrameHandler()
            df_handler.convert_file(**convert_options)
        elif not is_gzipped:
            converted_file_path = input_file_path.parent / f"{input_file_path.name}.gz"
            os.system(f"gzip -k {input_file_path}")
        else:
            converted_file_path = input_file_path
        client.populate_table_by_file_upload(table_name, converted_file_path)

        if not parsed_args['keep_generated_files']:
            if converted_file_path.exists() and converted_file_path != input_file_path:
                converted_file_path.unlink()
        return 0
