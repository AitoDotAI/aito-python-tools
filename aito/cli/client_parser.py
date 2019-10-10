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

from aito.schema.schema_handler import SchemaHandler


class ClientParserWrapper(ParserWrapper):
    def __init__(self):
        super().__init__(add_help=False)
        parser = self.parser
        parser.description = 'set up a client and perform CRUD operations'
        parser.usage = ''' aito client [-h] [<client-options>] <operation> [<operation-options>]
        To list operations:
            aito client list
        To see help for a specific operation:
            aito client <operation> -h
        '''
        parser.epilog = '''the AITO_INSTANCE_URL should look similar to https://my-instance.api.aito.ai
example:
  aito client create-table tableName < path/to/tableSchema.json
  aito client -e path/to/myDotEnvFile.env upload-file myTable path/to/myFile.csv
  aito client -u MY_AITO_INSTANCE_URL -r MY_RO_KEY -w MY_RW_KEY upload-batch myTable < path/to/myTableEntries.json
        '''
        client_args = parser.add_argument_group("client optional arguments")
        client_args.add_argument('-e', '--use-env-file', type=str, metavar='env-file-path',
                                 help='set up the client using a .env file containing the required env variables')
        client_args.add_argument('-r', '--read-only-key', type=str, default='.env',
                                 help='specify aito read-only API key')
        client_args.add_argument('-u', '--url', type=str, default='.env',
                                 help='specify aito instance url')
        client_args.add_argument('-w', '--read-write-key', type=str, default='.env',
                                 help='specify aito  read-write API key')
        self.client_operation_parsers = {
            'create-table': CreateTableParserWrapper,
            'delete-table': DeleteTableParserWrapper,
            'delete-database': DeleteDatabaseParserWrapper,
            'upload-batch': UploadBatchParserWrapper,
            'upload-file': UploadFileParserWrapper
        }
        operation_choices = ['list'] + list(self.client_operation_parsers.keys())
        self.operation_argument = parser.add_argument('operation', choices=operation_choices,
                                                      metavar='operation', help='perform an operation')

    def parse_and_execute(self, parsing_args) -> int:
        parsed_args, unknown = self.parser.parse_known_args(parsing_args)
        parsed_args = vars(parsed_args)
        if parsed_args['operation'] == 'list':
            all_operations = '\n'.join(self.client_operation_parsers.keys()) + '\n'
            sys.stdout.write(all_operations)
        else:
            client_action_parser = self.client_operation_parsers[parsed_args['operation']](self.parser,
                                                                                           self.operation_argument)
            client_action_parser.parse_and_execute(parsing_args)
        return 0


class ClientOperationParserWrapper(ParserWrapper):
    def __init__(self, parent_parser: AitoArgParser, operation_argument, operation: str):
        super().__init__()
        operation_argument.help = argparse.SUPPRESS
        self.parser = AitoArgParser(formatter_class=argparse.RawTextHelpFormatter,
                                    parents=[parent_parser])
        self.operation = operation
        self.usage_prefix = f"python aito client <client-options> {operation}"

    def create_client_from_parsed_args(self, parsed_args):
        def get_env_variable(variable_name):
            if variable_name not in env_var:
                self.parser.error(f"{variable_name} env variable not found")
            return env_var[variable_name]

        if parsed_args['use_env_file']:
            env_file_path = self.parser.check_valid_path(parsed_args['use_env_file'], True)
            load_dotenv(env_file_path)

        env_var = os.environ

        client_args = {
            'url': get_env_variable('AITO_INSTANCE_URL') if parsed_args['url'] == '.env' else parsed_args['url'],
            'rw_key': get_env_variable('AITO_RW_KEY') if parsed_args['read_write_key'] == '.env'
            else parsed_args['read_write_key'],
            'ro_key': get_env_variable('AITO_RO_KEY') if parsed_args['read_only_key'] == '.env'
            else parsed_args['read_only_key']
        }
        return AitoClient(**client_args)

    @abstractmethod
    def parse_and_execute(self, parsing_args):
        pass


class CreateTableParserWrapper(ClientOperationParserWrapper):
    def __init__(self, parent_parser: AitoArgParser, operation_argument):
        super().__init__(parent_parser, operation_argument, 'create-table')
        self.schema_handler = SchemaHandler()
        parser = self.parser
        parser.description = 'create a table with a given table schema'
        parser.usage = f''' {self.usage_prefix} <table-name> [table-schema-input]
        With no table-schema-input or when input is -, read table schema from standard input
        '''
        parser.epilog = '''example:
  aito client create-table myTable < path/to/myTableSchema.json
        '''
        parser.add_argument('table-name', type=str, help="name of the table to be populated")
        parser.add_argument('schema-input', default='-', type=str, nargs='?', help="table schema input")

    def parse_and_execute(self, parsing_args):
        parsed_args = vars(self.parser.parse_args(parsing_args))
        client = self.create_client_from_parsed_args(parsed_args)
        table_name = parsed_args['table-name']
        if parsed_args['schema-input'] == '-':
            table_schema = json.load(sys.stdin)
        else:
            input_path = self.parser.check_valid_path(parsed_args['schema-input'])
            with input_path.open() as f:
                table_schema = json.load(f)
        client.put_table_schema(table_name, table_schema)
        return 0


class DeleteTableParserWrapper(ClientOperationParserWrapper):
    def __init__(self, parent_parser: AitoArgParser, task_argument):
        super().__init__(parent_parser, task_argument, 'delete-table')
        parser = self.parser
        parser.description = "delete a table schema and all content inside the table"
        parser.usage = f"{self.usage_prefix} <table-name>"
        parser.add_argument('table-name', type=str, help="name of the table to be populated")

    def parse_and_execute(self, parsing_args) -> int:
        parsed_args = vars(self.parser.parse_args(parsing_args))
        client = self.create_client_from_parsed_args(parsed_args)
        table_name = parsed_args['table-name']
        if self.parser.ask_confirmation(f"Confirm delete table '{table_name}'? The action is irreversible", False):
            client.delete_table(table_name)
        return 0


class DeleteDatabaseParserWrapper(ClientOperationParserWrapper):
    def __init__(self, parent_parser: AitoArgParser, operation_argument):
        super().__init__(parent_parser, operation_argument, 'delete-database')
        parser = self.parser
        parser.description = "delete the whole database"

    def parse_and_execute(self, parsing_args) -> int:
        parsed_args = vars(self.parser.parse_args(parsing_args))
        client = self.create_client_from_parsed_args(parsed_args)
        if self.parser.ask_confirmation(f"Confirm delete the whole database? The action is irreversible", False):
            client.delete_database()
        return 0


class UploadBatchParserWrapper(ClientOperationParserWrapper):
    def __init__(self, parent_parser: AitoArgParser, operation_argument):
        super().__init__(parent_parser, operation_argument, 'upload-batch')
        parser = self.parser
        parser.description = "populating contents to a table by batch. " \
                             "The content must be a JSON array of table entries."
        parser.usage = f''' {self.usage_prefix} <table-name> [input]
        With no input, or when input is -, read table content from standard input
        '''
        parser.epilog = '''example:
  aito client upload-batch myTable path/to/myTableEntries.json
  aito client upload-batch myTable < path/to/myTableEntries.json
        '''
        parser.add_argument('table-name', type=str, help="name of the table to be populated")
        parser.add_argument('input', default='-', type=str, nargs='?', help="input file or stream in JSON array format")

    def parse_and_execute(self, parsing_args) -> int:
        parsed_args = vars(self.parser.parse_args(parsing_args))
        client = self.create_client_from_parsed_args(parsed_args)
        table_name = parsed_args['table-name']
        if parsed_args['input'] == '-':
            table_content = json.load(sys.stdin)
        else:
            input_path = self.parser.check_valid_path(parsed_args['input'])
            with input_path.open() as f:
                table_content = json.load(f)
        client.populate_table_entries(table_name, table_content)
        return 0


class UploadFileParserWrapper(ClientOperationParserWrapper):
    def __init__(self, parent_parser: AitoArgParser, operation_argument):
        super().__init__(parent_parser, operation_argument, 'upload-file')
        parser = self.parser
        parser.description = 'populating a file content to a table. If the file is not in gzip compressed ' \
                             'ndjson format, a converted ndjson.gz file will be created at the same location.'
        parser.usage = f"{self.usage_prefix} [<upload-file-options>] <table-name> <file-path>"
        parser.epilog = '''example:
  aito client upload-file tableName path/to/myFile.csv
  aito client upload-file -k tableName path/to/myFile.json
        '''
        parser.add_argument('table-name', type=str, help="name of the table to be populated")
        parser.add_argument('file-path', type=str, help="path to the input file")
        opts = parser.add_argument_group('upload file optional arguments')
        opts.add_argument('-f', '--file-format', type=str, choices=['infer', 'csv', 'excel', 'json', 'ndjson'],
                          default='infer', help='specify input file format if it is not ndjson.gzip '
                                                '(default: infer file format from file-path extension)')
        opts.add_argument('-k', '--keep-generated-files', action='store_true',
                         help='keep the converted ndjson.gz file if applicable')

    def parse_and_execute(self, parsing_args) -> int:
        parsed_args = vars(self.parser.parse_args(parsing_args))
        client = self.create_client_from_parsed_args(parsed_args)

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
