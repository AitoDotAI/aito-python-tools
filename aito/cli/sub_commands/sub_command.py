from abc import ABC, abstractmethod
from typing import Dict


class SubCommand(ABC):
    def __init__(self, name: str, help_message: str):
        self.name = name
        self.help_message = help_message

    @abstractmethod
    def build_parser(self, parser):
        """Take a vanilla argparser from main argparser's action sub argparser and add arguments

        :param parser: a vanilla argparser from main argparser's action sub argparser
        """

    @abstractmethod
    def parse_and_execute(self, parsed_args: Dict):
        """Use the parsed args from main argparser and execute the action

        :param parsed_args:
        """
