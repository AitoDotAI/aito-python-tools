import argparse
import logging.config
import shutil
import sys
import textwrap
from typing import Dict, Optional
from typing import List

import argcomplete

from aito import __version__
from aito.exceptions import BaseError
from .parser import ArgParser, DEFAULT_CONFIG_DIR
from .sub_commands.convert_sub_command import ConvertSubCommand
from .sub_commands.database_sub_command import ConfigureSubCommand, QuickAddTableSubCommand, CreateTableSubCommand, \
    DeleteTableSubCommand, CopyTableSubCommand, RenameTableSubCommand, ShowTablesSubCommand, DeleteDatabaseSubCommand, \
    UploadEntriesSubCommand, UploadBatchSubCommand, UploadFileSubCommand, UploadDataFromSQLSubCommand, OptimizeTableSubCommand, \
    QuickAddTableFromSQLSubCommand, GetDatabaseSubCommand, GetTableSubCommand, QuickPredictSubCommand, SearchSubCommand, \
    PredictSubCommand, RecommendSubCommand, EvaluateSubCommand, SimilaritySubCommand, MatchSubCommand, RelateSubCommand, \
    GenericQuerySubCommand, CreateDatabaseSubCommand
from .sub_commands.infer_table_schema_sub_command import InferTableSchemaSubCommand
from .sub_commands.sub_command import SubCommand


class MainParser(ArgParser):
    _default_commands = [
        InferTableSchemaSubCommand(),
        ConvertSubCommand(),
        ConfigureSubCommand(),
        QuickAddTableSubCommand(),
        CreateDatabaseSubCommand(),
        CreateTableSubCommand(),
        GetTableSubCommand(),
        DeleteTableSubCommand(),
        CopyTableSubCommand(),
        RenameTableSubCommand(),
        ShowTablesSubCommand(),
        GetDatabaseSubCommand(),
        DeleteDatabaseSubCommand(),
        UploadEntriesSubCommand(),
        UploadBatchSubCommand(),
        UploadFileSubCommand(),
        UploadDataFromSQLSubCommand(),
        OptimizeTableSubCommand(),
        QuickAddTableFromSQLSubCommand(),
        QuickPredictSubCommand(),
        SearchSubCommand(),
        PredictSubCommand(),
        RecommendSubCommand(),
        EvaluateSubCommand(),
        SimilaritySubCommand(),
        MatchSubCommand(),
        RelateSubCommand(),
        GenericQuerySubCommand()
    ]

    def __init__(self, commands: List[SubCommand] = None):
        super().__init__()
        self.prog = 'aito'
        self.add_argument('-V', '--version', action='store_true', help='display the version of this tool')
        self.add_argument('-v', '--verbose', action='store_true', help='display verbose messages')
        self.add_argument('-q', '--quiet', action='store_true', help='display only error messages')
        if not commands:
            commands = self._default_commands
        self._commands_map = {cmd.name: cmd for cmd in commands}
        commands_subparsers = self.add_subparsers(
            title='command',
            dest='command',
            parser_class=ArgParser,
            metavar="<command>",
            help=argparse.SUPPRESS
        )
        for cmd in commands:
            cmd_parser = commands_subparsers.add_parser(
                cmd.name, help=cmd.help_message
            )
            cmd.build_parser(cmd_parser)
        commands_subparsers.add_parser('list', help='list all available commands')
        self.epilog = f"""To see all available commands, you can run:
  {self.prog} list

To see the help text, you can run:
  {self.prog} -h
  {self.prog} <command> -h
"""
        argcomplete.autocomplete(self)

    @staticmethod
    def config_logging(default_level='INFO'):
        if not DEFAULT_CONFIG_DIR.exists():
            DEFAULT_CONFIG_DIR.mkdir(parents=True)

        config_dict = {
            "version": 1,
            'disable_existing_loggers': False,
            "formatters": {
                'default': {
                    'class': 'logging.Formatter',
                    'format': '%(name)-20s %(message)s',
                },
                'detailed': {
                    'class': 'logging.Formatter',
                    'format': '%(asctime)s %(name)-20s %(levelname)-10s %(message)s',
                    'datefmt': '%Y-%m-%dT%H:%M:%S%z',
                    'converter': 'time.gmtime'
                }
            },
            "handlers": {
                'default': {
                    'class': 'logging.StreamHandler',
                    'formatter': 'default',
                    'level': default_level,
                    'stream': 'ext://sys.stdout'
                },
                'file': {
                    'class': 'logging.handlers.RotatingFileHandler',
                    'formatter': 'detailed',
                    'level': 'DEBUG',
                    'filename': DEFAULT_CONFIG_DIR / 'aito.log',
                    'maxBytes': 10 * 1024 * 1024,
                    'backupCount': 5,
                }
            },
            'loggers': {
                '': {
                    'handlers': ['default', 'file'],
                    'level': 'DEBUG'
                }
            }
        }
        logging.config.dictConfig(config_dict)

    def list_commands(self, max_command_name_width=24):
        terminal_width = shutil.get_terminal_size().columns
        terminal_width -= 2

        cmd_name_max_length = max(len(sub_command_name) for sub_command_name in self._commands_map)
        max_command_name_width = min(cmd_name_max_length, max_command_name_width)

        text_wrapper = textwrap.TextWrapper(width=terminal_width)

        formatted_text_items = []
        for cmd_name, cmd in self._commands_map.items():
            # overflow command name
            if len(cmd_name) + 2 >= max_command_name_width:
                formatted_text_items.append(f'{cmd_name}\n')
                formatted_text_items += text_wrapper.wrap(' ' * max_command_name_width + cmd.help_message)
            else:
                remaining_spaces = ' ' * (max_command_name_width - len(cmd_name))
                formatted_text_items += text_wrapper.wrap(f"{cmd_name}{remaining_spaces}{cmd.help_message}")

        formatted_message = '\n'.join(formatted_text_items) + '\n'
        self.exit(status=0, message=formatted_message)

    def parse_and_execute(self, parsed_args: Optional[Dict] = None):
        if parsed_args is None:
            parsed_args = vars(self.parse_args())
        if parsed_args['version']:
            print(__version__)
            return 0
        default_level = 'DEBUG' if parsed_args['verbose'] else 'WARNING' if parsed_args['quiet'] else 'INFO'
        self.config_logging(default_level=default_level)

        command_name = parsed_args['command']
        if not command_name:
            self.error_and_print_help('the following arguments are required: <command>')
        elif command_name == 'list':
            self.list_commands()
        else:
            try:
                self._commands_map[command_name].parse_and_execute(parsed_args)
            except BaseError:
                self.exit(2)
            except Exception as e:
                self.exit(2, f"{self.prog}: error: {e}\n")
        return 0


def main():
    parser = MainParser()
    sys.exit(parser.parse_and_execute())
