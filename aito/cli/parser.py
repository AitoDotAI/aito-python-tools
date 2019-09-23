import argparse
import sys
from pathlib import Path


class AitoParser(argparse.ArgumentParser):
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
