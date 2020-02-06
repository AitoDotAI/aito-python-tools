import sys

import argcomplete

from aito.cli.convert_parser import add_convert_parser, execute_convert
from aito.cli.database_parser import add_database_parser, execute_database_operation
from aito.cli.infer_table_schema_parser import add_infer_table_schema_parser, execute_infer_table_schema
from aito.utils.generic_utils import set_up_logger
from aito.utils.parser import AitoArgParser, ParserWrapper


class MainParserWrapper(ParserWrapper):
    def __init__(self, add_help=True):
        super().__init__(add_help)
        self.parser.prog = 'aito'
        action_subparsers = self.parser.add_subparsers(title='action',
                                                       description='action to perform',
                                                       dest='action',
                                                       parser_class=AitoArgParser,
                                                       metavar="<action>")
        action_subparsers.required = True
        enable_sql_functions = True
        try:
            import pyodbc
        except ImportError:
            enable_sql_functions = False

        add_infer_table_schema_parser(action_subparsers, enable_sql_functions)
        add_convert_parser(action_subparsers)
        add_database_parser(action_subparsers, enable_sql_functions)
        argcomplete.autocomplete(self.parser)

    def parse_and_execute(self, parsing_args):
        parsed_args = vars(self.parser.parse_args(parsing_args))
        action = parsed_args['action']
        if action == 'infer-table-schema':
            execute_infer_table_schema(self.parser, parsed_args)
        elif action == 'convert':
            execute_convert(self.parser, parsed_args)
        elif action == 'database':
            execute_database_operation(self.parser, parsed_args)
        return 0


def main():
    set_up_logger()
    main_parser = MainParserWrapper()
    main_parser.parse_and_execute(sys.argv[1:])


if __name__ == '__main__':
    main()
