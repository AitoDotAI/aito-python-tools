import argparse
import sys
from abc import abstractmethod
import logging
from pathlib import Path
from aito.utils._typing import FilePathOrBuffer
import os


class AitoArgParser(argparse.ArgumentParser):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = logging.getLogger('Parser')

    def error(self, message):
        sys.stderr.write(f"error: {message}\n")
        self.print_help()
        sys.exit(2)

    def parse_path_value(self, path, check_exists=False) -> Path:
        try:
            path = Path(path)
        except Exception as e:
            self.error(f"invalid path {path}")
            raise e
        if check_exists and not path.exists():
            self.error(f"path {path} does not exist")
        return path

    def parse_input_arg_value(self, input_arg: str) -> FilePathOrBuffer:
        return sys.stdin if input_arg == '-' else self.parse_path_value(input_arg, True)

    def parse_output_arg_value(self, output_arg: str) -> FilePathOrBuffer:
        return sys.stdout if output_arg == '-' else self.parse_path_value(output_arg)

    def parse_env_variable(self, var_name):
        if var_name not in os.environ:
            self.logger.warning(f"{var_name} environment variable not found")
            return None
        return os.environ[var_name]

    @staticmethod
    def ask_confirmation(content, default: bool = None) -> bool:
        valid_responses = {
            'yes': True,
            'y': True,
            'no': False,
            'n': False
        }
        if not default:
            prompt = '[y/n]'
        elif default:
            prompt = '[Y/n]'
        else:
            prompt = '[y/N]'
        while True:
            sys.stdout.write(f"{content} {prompt}")
            response = input().lower()
            if default and response == '':
                return default
            elif response in valid_responses:
                return valid_responses[response]
            else:
                sys.stdout.write("Please respond with yes(y) or no(n)'\n")


class ParserWrapper:
    def __init__(self, add_help=True):
        if add_help:
            self.parser = AitoArgParser(formatter_class=argparse.RawTextHelpFormatter)
        else:
            self.parser = AitoArgParser(formatter_class=argparse.RawTextHelpFormatter, add_help=False)

    @abstractmethod
    def parse_and_execute(self, parsing_args):
        pass
