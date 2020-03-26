from abc import ABC, abstractmethod
from typing import Dict


class SubCommand(ABC):
    def __init__(self, name: str, help_message: str):
        self.name = name
        self.help_message = help_message

    @abstractmethod
    def build_parser(self, parser):
        """Take a vanilla parser from main parser's action sub parser and add arguments

        :param parser: a vanilla parser from main parser's action sub parser
        """

    @abstractmethod
    def parse_and_execute(self, parsed_args: Dict):
        """Use the parsed args from main parser and execute the action

        :param parsed_args:
        """
