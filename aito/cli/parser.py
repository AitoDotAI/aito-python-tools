import argparse
import sys
from pathlib import Path
from abc import abstractmethod


class AitoArgParser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write(f"error: {message}\n")
        self.print_help()
        sys.exit(2)

    def check_valid_path(self, str_path, check_exists=False):
        try:
            path = Path(str_path)
        except Exception as e:
            self.error(f"invalid path {str_path}")
            raise e
        if check_exists and not path.exists():
            self.error(f"path {str_path} does not exist")
        return path

    def parse_input_arg_value(self, input_arg: str):
        return sys.stdin if input_arg == '-' else self.check_valid_path(input_arg, True)

    def parse_output_arg_value(self, output_arg: str):
        return sys.stdout if output_arg == '-' else self.check_valid_path(output_arg)

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
            prompt= '[y/N]'
        while True:
            sys.stdout.write(f"{content} {prompt}")
            response = input().lower()
            if default and response == '':
                return default
            elif response in valid_responses:
                return valid_responses[response]
            else:
                sys.stdout.write("Please respond with yes(y) or no(n)'\n")



class ParserWrapper():
    def __init__(self, add_help=True):
        if add_help:
            self.parser = AitoArgParser(formatter_class=argparse.RawTextHelpFormatter)
        else:
            self.parser = AitoArgParser(formatter_class=argparse.RawTextHelpFormatter, add_help=False)

    @abstractmethod
    def parse_and_execute(self, parsing_argsw):
        pass