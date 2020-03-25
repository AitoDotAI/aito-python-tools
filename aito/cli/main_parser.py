import sys

import argcomplete

from aito.cli.convert_parser import add_convert_parser, execute_convert
from aito.cli.database_parser import add_database_parser, execute_database_operation
from aito.cli.infer_table_schema_parser import add_infer_table_schema_parser, execute_infer_table_schema
from aito.utils.generic_utils import logging_config
from aito.cli.parser import AitoArgParser
from aito import __version__


class MainParser(AitoArgParser):
    def __init__(self):
        super().__init__()
        self.prog = 'aito'
        self.add_argument('-V', '--version', action='store_true', help='display the version of this tool')
        self.add_argument('-v', '--verbose', action='store_true', help='display verbose messages')
        self.add_argument('-q', '--quiet', action='store_true', help='display only error messages')
        action_subparsers = self.add_subparsers(
            title='action',
            description='action to perform',
            dest='action',
            parser_class=AitoArgParser,
            metavar="<action>"
        )
        enable_sql_functions = True
        try:
            import pyodbc
        except ImportError:
            enable_sql_functions = False

        add_infer_table_schema_parser(action_subparsers, enable_sql_functions)
        add_convert_parser(action_subparsers)
        add_database_parser(action_subparsers, enable_sql_functions)
        argcomplete.autocomplete(self)

    def parse_and_execute(self):
        parsed_args = vars(self.parse_args())
        if parsed_args['version']:
            print(__version__)
            return 0
        logging_level = 10 if parsed_args['verbose'] else 40 if parsed_args['quiet'] else 20
        logging_config(level=logging_level)
        action = parsed_args['action']
        if not action:
            self.error('the following arguments are required: <action>')
        if action == 'infer-table-schema':
            execute_infer_table_schema(self, parsed_args)
        elif action == 'convert':
            execute_convert(self, parsed_args)
        elif action == 'database':
            execute_database_operation(self, parsed_args)
        return 0
