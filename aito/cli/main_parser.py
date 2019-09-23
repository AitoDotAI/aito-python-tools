import argparse

from aito.cli.client_parser import ClientParser
from aito.cli.convert_parser import ConvertParser
from aito.cli.parser import AitoParser


class MainParser:
    def __init__(self):
        usage = ''' python aito.py [-h] <action> [<args>]
        To see help text, you can run:
            python aito.py -h
            python aito.py <action> -h
            
        The most commonly actions are:
            convert     convert data into ndjson format
            client      set up and do task with an aito client
        '''
        self.parser = AitoParser(formatter_class=argparse.RawTextHelpFormatter, usage=usage)
        self.parser.add_argument('action', help='action to perform')
        self.actions_parser = {
            'convert': ConvertParser(),
            'client': ClientParser()
        }

    def parse_and_execute(self, parsing_args) -> int:
        args = self.parser.parse_args(parsing_args[0:1])
        if args.action not in self.actions_parser:
            self.parser.error(f"unrecognized action {args.action}")
        self.actions_parser[args.action].parse_and_execute(parsing_args[1:])
        return 0
