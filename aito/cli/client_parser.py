import argparse
import datetime
import json
import os
import sys
from abc import abstractmethod

from dotenv import load_dotenv

from aito.cli.parser import AitoArgParser, ParserWrapper
from aito.aito_client import AitoClient
from aito.data_frame_handler import DataFrameHandler
from aito.schema_handler import SchemaHandler


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
  aito client quick-add-table myTable.csv
  aito client create-table tableName < path/to/tableSchema.json
  aito client -e path/to/myDotEnvFile.env upload-file myTable path/to/myFile.csv
  aito client -u MY_AITO_INSTANCE_URL -r MY_RO_KEY -w MY_RW_KEY upload-batch myTable < path/to/myTableEntries.json
        '''
        client_args = parser.add_argument_group("client optional arguments")
        client_args.add_argument('-e', '--use-env-file', type=str, metavar='env-input-file',
                                 help='set up the client using a .env file containing the required env variables')
        client_args.add_argument('-r', '--read-only-key', type=str, default='.env',
                                 help='specify aito read-only API key')
        client_args.add_argument('-u', '--url', type=str, default='.env',
                                 help='specify aito instance url')
        client_args.add_argument('-w', '--read-write-key', type=str, default='.env',
                                 help='specify aito  read-write API key')
        self.operation_to_parser_wrapper = {
            'quick-add-table': QuickAddTableParserWrapper,
            'create-table': CreateTableParserWrapper,
            'delete-table': DeleteTableParserWrapper,
            'delete-database': DeleteDatabaseParserWrapper,
            'upload-batch': UploadBatchParserWrapper,
            'upload-file': UploadFileParserWrapper
        }
        self.operation_to_description = {
            'quick-add-table': "infer schema, create table, and upload file",
            'create-table': "create a table with a given table schema",
            'delete-table': "delete a table schema and all content inside the table",
            'delete-database': "delete the whole database",
            'upload-batch': "populating table entries to a table. Tables entries must be in JSON array format",
            'upload-file': "populating a file content to a table. "
                           "Automatically convert the file to match the existing schema"
        }

        operation_choices = ['list'] + list(self.operation_to_parser_wrapper.keys())
        self.operation_argument = parser.add_argument('operation', choices=operation_choices,
                                                      metavar='operation', help='perform an operation')

    def parse_and_execute(self, parsing_args) -> int:
        parsed_args, unknown = self.parser.parse_known_args(parsing_args)
        parsed_args = vars(parsed_args)
        if parsed_args['operation'] == 'list':
            for op, op_desc in self.operation_to_description.items():
                sys.stdout.write(f"{op:20}{op_desc}\n")
        else:
            client_action_parser = self.operation_to_parser_wrapper[parsed_args['operation']](self)
            client_action_parser.parse_and_execute(parsing_args)
        return 0


class ClientOperationParserWrapper():
    def __init__(self, client_parser_wrapper: ClientParserWrapper, operation: str):
        client_parser_wrapper.operation_argument.help = argparse.SUPPRESS
        self.parser = AitoArgParser(formatter_class=argparse.RawTextHelpFormatter,
                                    parents=[client_parser_wrapper.parser],
                                    description=client_parser_wrapper.operation_to_description[operation])
        self.operation = operation
        self.usage_prefix = f"aito client <client-options> {operation} [-h]"

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


class QuickAddTableParserWrapper(ClientOperationParserWrapper):
    def __init__(self, client_parser_wrapper: ClientParserWrapper):
        super().__init__(client_parser_wrapper, 'quick-add-table')
        parser = self.parser
        parser.usage = f"{self.usage_prefix} [-h] [<options>] <input-file>"
        parser.epilog = '''example:
  aito add-table myTable.json
  aito add-table --table-name myTable myFile.csv
  aito add-table -n myTable -f csv myFile
  '''
        parser.add_argument('-n', '--table-name', type=str,
                            help='create a table with the given name (default: use file name)')
        file_format_choices = ['infer'] + DataFrameHandler.allowed_format
        parser.add_argument('-f', '--file-format', type=str, choices=file_format_choices,
                            default='infer', help='specify input file format (default: infer from the file extension)')
        parser.add_argument('input-file', type=str, help="path to the input file")

    def parse_and_execute(self, parsing_args):
        parsed_args = vars(self.parser.parse_args(parsing_args))

        input_file_path = self.parser.check_valid_path(parsed_args['input-file'])
        table_name = parsed_args['table_name'] if parsed_args['table_name'] else input_file_path.stem
        in_format = input_file_path.suffixes[0].replace('.', '') if parsed_args['file_format'] == 'infer' \
            else parsed_args['file_format']

        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        converted_file_path = input_file_path.parent / f"{input_file_path.stem}_{now}.ndjson.gz"
        schema_path = input_file_path.parent / f"{table_name}_schema_{now}.json"
        convert_options = {
            'read_input': input_file_path,
            'write_output': converted_file_path,
            'in_format': in_format,
            'out_format': 'ndjson',
            'convert_options': {'compression': 'gzip'},
            'create_table_schema': schema_path
        }
        df_handler = DataFrameHandler()
        df_handler.convert_file(**convert_options)

        client = self.create_client_from_parsed_args(parsed_args)
        with schema_path.open() as f:
            inferred_schema = json.load(f)
        client.put_table_schema(table_name, inferred_schema)
        client.populate_table_by_file_upload(table_name, converted_file_path)

        if converted_file_path.exists():
            converted_file_path.unlink()
        schema_path.unlink()
        return 0


class CreateTableParserWrapper(ClientOperationParserWrapper):
    def __init__(self, client_parser_wrapper: ClientParserWrapper):
        super().__init__(client_parser_wrapper, 'create-table')
        self.schema_handler = SchemaHandler()
        parser = self.parser
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
    def __init__(self, client_parser_wrapper: ClientParserWrapper):
        super().__init__(client_parser_wrapper, 'delete-table')
        parser = self.parser
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
    def __init__(self, client_parser_wrapper: ClientParserWrapper):
        super().__init__(client_parser_wrapper, 'delete-database')
        parser = self.parser

    def parse_and_execute(self, parsing_args) -> int:
        parsed_args = vars(self.parser.parse_args(parsing_args))
        client = self.create_client_from_parsed_args(parsed_args)
        if self.parser.ask_confirmation(f"Confirm delete the whole database? The action is irreversible", False):
            client.delete_database()
        return 0


class UploadBatchParserWrapper(ClientOperationParserWrapper):
    def __init__(self, client_parser_wrapper: ClientParserWrapper):
        super().__init__(client_parser_wrapper, 'upload-batch')
        parser = self.parser
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
    def __init__(self, client_parser_wrapper: ClientParserWrapper):
        super().__init__(client_parser_wrapper, 'upload-file')
        parser = self.parser
        parser.usage = f"{self.usage_prefix} [<upload-file-options>] <table-name> <input-file>"
        parser.epilog = '''example:
  aito client upload-file tableName path/to/myFile.csv
        '''
        parser.add_argument('table-name', type=str, help="name of the table to be populated")
        parser.add_argument('input-file', type=str, help="path to the input file")
        opts = parser.add_argument_group('upload file optional arguments')

        file_format_choices = ['infer'] + DataFrameHandler.allowed_format
        opts.add_argument('-f', '--file-format', type=str, choices=file_format_choices,
                          default='infer', help='specify input file format (default: infer from the file extension)')

    def parse_and_execute(self, parsing_args) -> int:
        parsed_args = vars(self.parser.parse_args(parsing_args))
        client = self.create_client_from_parsed_args(parsed_args)
        table_name = parsed_args['table-name']

        if not client.check_table_existed(table_name):
            self.parser.error(f"Table '{table_name}' does not exist. Please create table first.")

        input_file_path = self.parser.check_valid_path(parsed_args['input-file'])

        in_format = input_file_path.suffixes[0].replace('.', '') if parsed_args['file_format'] == 'infer' \
            else parsed_args['file_format']

        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        converted_file_path = input_file_path.parent / f"{input_file_path.stem}_{now}.ndjson.gz"
        convert_options = {
            'read_input': input_file_path,
            'write_output': converted_file_path,
            'in_format': in_format,
            'out_format': 'ndjson',
            'convert_options': {'compression': 'gzip'},
            'use_table_schema': client.get_table_schema(table_name)
        }
        df_handler = DataFrameHandler()
        df_handler.convert_file(**convert_options)
        client.populate_table_by_file_upload(table_name, converted_file_path)

        if converted_file_path.exists():
            converted_file_path.unlink()
        return 0
