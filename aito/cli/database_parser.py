from typing import List

import argcomplete

from aito import __version__
from aito.utils._generic_utils import logging_config
from .parser import ArgParser, ParseError
from .sub_commands import *
from typing import Dict, Optional
import sys


class DatabaseParser(ArgParser):
    def __init__(self):
        super().__init__()
        self.prog = 'aitodb'
        self.add_argument('-V', '--version', action='store_true', help='display the version of this tool')
        self.add_argument('-v', '--verbose', action='store_true', help='display verbose messages')
        self.add_argument('-q', '--quiet', action='store_true', help='display only error messages')
        DatabaseSubCommand().build_parser(self)
        argcomplete.autocomplete(self)

    def parse_and_execute(self, parsed_args: Optional[Dict] = None):
        if parsed_args is None:
            parsed_args = vars(self.parse_args())
        if parsed_args['version']:
            print(__version__)
            return 0
        logging_level = 10 if parsed_args['verbose'] else 40 if parsed_args['quiet'] else 20
        logging_config(level=logging_level)

        try:
            DatabaseSubCommand().parse_and_execute(parsed_args)
        except ParseError as e:
            self.error(e.message)
        return 0


def main():
    parser = DatabaseParser()
    sys.exit(parser.parse_and_execute())


if __name__ == "__main__":
    main()