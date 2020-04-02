from aito.cli.main_parser import MainParser
from tests.cases import CompareTestCase
import os
import subprocess


class ParserAndCLITestCase(CompareTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.parser = MainParser()

    def parse_and_execute(
            self, parsing_args, expected_args, stub_stdin=None, stub_stdout=None, execute_exception=None
    ):
        if os.getenv('TEST_BUILT_PACKAGE'):
            if execute_exception:
                with self.assertRaises(subprocess.CalledProcessError):
                    subprocess.run(['aito'] + parsing_args, stdin=stub_stdin, stdout=stub_stdout, check=True)
            else:
                subprocess.run(['aito'] + parsing_args, stdin=stub_stdin, stdout=stub_stdout, check=True)
        else:
            self.assertDictEqual(vars(self.parser.parse_args(parsing_args)), expected_args)
            if stub_stdin:
                self.stub_stdin(stub_stdin)
            if stub_stdout:
                self.stub_stdout(stub_stdout)
            # re run parse_args to use the new stubbed stdio
            if execute_exception:
                with self.assertRaises(execute_exception):
                    self.parser.parse_and_execute(vars(self.parser.parse_args(parsing_args)))
            else:
                self.parser.parse_and_execute(vars(self.parser.parse_args(parsing_args)))