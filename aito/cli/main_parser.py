from typing import List

import argcomplete

from aito import __version__
from aito.utils._generic_utils import logging_config
from .parser import ArgParser, ParseError
from .sub_commands import *
from typing import Dict, Optional
import sys


class MainParser(ArgParser):
    _default_sub_commands = [
        InferTableSchemaSubCommand(),
        ConvertSubCommand(),
        DatabaseSubCommand()
    ]

    def __init__(self, sub_commands: List[SubCommand] = None):
        super().__init__()
        self.prog = 'aito'
        self.add_argument('-V', '--version', action='store_true', help='display the version of this tool')
        self.add_argument('-v', '--verbose', action='store_true', help='display verbose messages')
        self.add_argument('-q', '--quiet', action='store_true', help='display only error messages')
        if not sub_commands:
            sub_commands = self._default_sub_commands
        self._sub_commands_map = {cmd.name: cmd for cmd in sub_commands}
        sub_commands_subparsers = self.add_subparsers(
            title='command',
            dest='command',
            parser_class=ArgParser,
            metavar="<command>"
        )
        for sub_cmd in sub_commands:
            sub_cmd_parser = sub_commands_subparsers.add_parser(sub_cmd.name, help=sub_cmd.help_message)
            sub_cmd.build_parser(sub_cmd_parser)
        argcomplete.autocomplete(self)

    def parse_and_execute(self, parsed_args: Optional[Dict] = None):
        if parsed_args is None:
            parsed_args = vars(self.parse_args())
        if parsed_args['version']:
            print(__version__)
            return 0
        logging_level = 10 if parsed_args['verbose'] else 40 if parsed_args['quiet'] else 20
        logging_config(level=logging_level)

        sub_command_name = parsed_args['command']
        if not sub_command_name:
            self.error('the following arguments are required: <command>')

        try:
            self._sub_commands_map[sub_command_name].parse_and_execute(parsed_args)
        except ParseError as e:
            self.error(e.message)
        return 0


def main():
    parser = MainParser()
    sys.exit(parser.parse_and_execute())