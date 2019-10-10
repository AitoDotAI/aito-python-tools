import logging
import sys

from aito.cli.client_parser import ClientParserWrapper
from aito.cli.convert_parser import ConvertParserWrapper
from aito.cli.infer_schema_parser import InferTableSchemaParserWrapper
from aito.cli.parser import ParserWrapper


class MainParserWrapper(ParserWrapper):
    def __init__(self):
        super().__init__()
        self.parser.usage = ''' aito [-h] <action> [<args>]
        To see help text, you can run:
            aito -h
            aito <action> -h

        The most commonly actions are:
            infer-table-schema  infer Aito table schema from a file
            convert             convert data of table entries into ndjson (for file-upload) or json (for batch-upload)
            client              set up a client and perform CRUD operations
        '''
        self.parser.add_argument('action', help='action to perform')
        self.actions_parser = {
            'infer-table-schema': InferTableSchemaParserWrapper(),
            'convert': ConvertParserWrapper(),
            'client': ClientParserWrapper()
        }

    def parse_and_execute(self, parsing_args):
        args = self.parser.parse_args(parsing_args[0:1])
        if args.action not in self.actions_parser:
            self.parser.error(f"unrecognized action {args.action}")
        self.actions_parser[args.action].parse_and_execute(parsing_args[1:])
        return 0


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)-5s %(name)-5s %(levelname)-10s %(message)s',
                        datefmt='%H:%M:%S')
    main_parser = MainParserWrapper()
    main_parser.parse_and_execute(sys.argv[1:])
