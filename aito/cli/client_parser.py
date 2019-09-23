import argparse
import os
import json

from dotenv import load_dotenv

from aito.cli.parser import AitoParser
from aito.client.aito_client import AitoClient


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
        self.parser.add_argument('-e', '--use-env-file', type=str,
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

        self.client_action_parsers = {
            'upload-batch': UploadBatchParser
        }
        self.parser.add_argument('action', choices=list(self.client_action_parsers.keys()),
                                 help='perform an action with the client')

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

        url = get_env_variable('AITO_INSTANCE_URL') if parsed_args['url'] == '.env' else parsed_args['url']
        rw_key = get_env_variable('AITO_RW_KEY') if parsed_args['read_write_key'] == '.env' \
            else parsed_args['read_write_key']
        ro_key = get_env_variable('AITO_RO_KEY') if parsed_args['read_only_key'] == '.env' \
            else parsed_args['read_only_key']
        self.client = AitoClient(url, rw_key, ro_key)

        client_action_parser = self.client_action_parsers[parsed_args['action']](self.client)
        client_action_parser.parse_and_execute(parsing_args)
        return 0


class UploadBatchParser:
    def __init__(self, client: AitoClient):
        self.client = client
        self.parser = AitoParser(formatter_class=argparse.RawTextHelpFormatter,
                                 description='populating contents to a table',
                                 usage='''
                                 aito.py client <client-options> upload-batch <table-name> [input]
                                 With no input, or when input is -, read table content from standard input
                                 ''')
        self.parser.add_argument('table-name', type=str, help="name of the table to be populated")
        self.parser.add_argument('input', default='-', type=str, nargs='?', help="input file")

    def parse_and_execute(self, parsing_args) -> int:
        parsed_args = vars(self.parser.parse_args(parsing_args))
        table_name = parsed_args['table_name']
        parsed_input = self.parser.parse_input_arg_value(parsed_args['input'])
        try:
            table_content = json.load(parsed_input)
        except json.JSONDecodeError as e:
            self.parser.error(f"invalid json: {e}")
        self.client.populate_table_entries(table_name, table_content)
        return 0