import argparse
import logging
import sys

from aito.cli.infer_table_schema_parser import add_infer_table_schema_parser, execute_infer_table_schema
from aito.cli.parser import ParserWrapper, AitoArgParser
import argcomplete


class MainParser:
    def __init__(self):
        self.parser = AitoArgParser(formatter_class=argparse.RawTextHelpFormatter)
        action_subparsers = self.parser.add_subparsers(title='action', description='action to perform', dest='action',
                                                       parser_class=AitoArgParser)
        action_subparsers.required = True

        add_infer_table_schema_parser(action_subparsers)

    def parse_and_execute(self, parsing_args):
        argcomplete.autocomplete(self.parser)
        parsed_args = vars(self.parser.parse_args(parsing_args))
        if parsed_args['action'] == 'infer-table-schema':
            execute_infer_table_schema(self.parser, parsed_args)
        return 0


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
        action_sub_parsers = self.parser.add_subparsers(title='action',
                                                        description='action to perform',
                                                        dest='action')
        action_sub_parsers.required = True

    def parse_and_execute(self, parsing_args):
        self.parser.parse_args(parsing_args)
        # args = self.parser.parse_args(parsing_args[0:1])
        # if args.action not in self.actions_parser:
        #     self.parser.error(f"unrecognized action {args.action}")
        # self.actions_parser[args.action].parse_and_execute(parsing_args[1:])
        return 0


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)-5s %(name)-5s %(levelname)-10s %(message)s',
                        datefmt='%H:%M:%S')
    main_parser = MainParser()
    main_parser.parse_and_execute(sys.argv[1:])


if __name__ == '__main__':
    main()
