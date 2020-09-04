import argparse
import logging
import logging.handlers
import os
import sys
import time
import unittest
import doctest
from pathlib import Path
from typing import Dict

from tests.results import TestResultLogMetrics, TestResultCompareFileMeld

ROOT_PATH = Path(__file__).parent.parent


class TestParser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write(f"error: {message}\n")
        self.print_help()
        sys.exit(2)

    @staticmethod
    def is_not_suite(test: unittest.suite.TestSuite):
        try:
            iter(test)
        except TypeError:
            return True
        return False

    @staticmethod
    def discover_tests_in_dir(dir: Path, silent=False) -> unittest.TestSuite:
        """ Discover tests in a directory with TestLoader
        print error if test loader failed to load tests
        """
        test_loader = unittest.TestLoader()
        tests = test_loader.discover(str(dir))
        if test_loader.errors and not silent:
            print(f"failed to load {dir} with {len(test_loader.errors)} errors:")
            for err in test_loader.errors:
                print(err)
        return tests

    def discover_suites(self, starting_dir: Path, silent=False) -> Dict:
        """ Iterating recursively through the test directory and
        returns the name of child directories which contain tests as test suites
        """
        checking_dirs = {starting_dir}
        suites_dir = set()
        while checking_dirs:
            checking_d = checking_dirs.pop()
            sub_dirs = {d for d in checking_d.iterdir() if d.is_dir() and d.stem != '__pycache__'}
            if not sub_dirs:
                suites_dir.add(checking_d)
            else:
                checking_dirs = checking_dirs.union(sub_dirs)
        test_suites = {}
        for d in suites_dir:
            t_suite = self.discover_tests_in_dir(d, silent)

            if t_suite.countTestCases() > 0:
                test_suites['.'.join(d.relative_to(starting_dir).parts)] = t_suite
        return test_suites

    def unpack_test_suite(self, ret_val, test_suite, level):
        """ Recursively unpack a test suite into cases or methods
        """
        if level not in ("case", "method"):
            raise ValueError("level must be either case or method")

        for test in test_suite:
            if self.is_not_suite(test):
                # Case should be suite if it has multiple methods
                t_name = '.'.join(test.id().split('.')[:-1]) if level == "case" else test.id()
                if isinstance(test, doctest.DocTestCase):
                    t_name = f'doctest.{t_name}'
                if level == "case":
                    if t_name not in ret_val:
                        suite = unittest.TestSuite()
                        ret_val[t_name] = suite
                    ret_val[t_name].addTest(test)
                else:
                    ret_val[t_name] = test
            else:
                ret_val = self.unpack_test_suite(ret_val, test, level)
        return ret_val

    def discover_cases(self, starting_dir: Path, silent=False):
        t_suite = self.discover_tests_in_dir(starting_dir, silent)
        return self.unpack_test_suite({}, t_suite, "case")

    def discover_test_methods(self, starting_dir: Path, silent=False):
        t_suite = self.discover_tests_in_dir(starting_dir, silent)
        return self.unpack_test_suite({}, t_suite, "method")

    def list_command(self, level: str, test_dir):
        if level == 'suite':
            test_suites = self.discover_suites(test_dir)
            names = list(test_suites.keys())
        elif level == 'case':
            names = list(self.discover_cases(test_dir).keys())
        else:
            names = list(self.discover_test_methods(test_dir).keys())
        if not names:
            sys.stdout.write(f"No {level} found in {test_dir}")
        else:
            sys.stdout.write('\n'.join(names))
            sys.stdout.write('\n')

    def run_by_level(self, test_runner, test_dir, level, name):

        if level == "suite":
            all_choices = self.discover_suites(test_dir, silent=True)
        if level == "case":
            all_choices = self.discover_cases(test_dir, silent=True)


    @staticmethod
    def config_log(log_dir_path, log_to_std_out, verbose):
        log_dir = ROOT_PATH.joinpath(log_dir_path)
        log_dir.mkdir(exist_ok=True)
        os.environ['METRICS_LOG_PATH'] = str(log_dir / 'test_metrics.log')
        file_handler = logging.handlers.RotatingFileHandler(
            filename=str(log_dir / 'test.log'), maxBytes=10 * 1024 * 1024, backupCount=5
        )
        log_config_kwargs = {
            'format': '%(asctime)s %(name)-20s %(levelname)-10s %(message)s',
            'datefmt': "%Y-%m-%dT%H:%M:%S%z",
            'handlers': [file_handler, logging.StreamHandler()] if log_to_std_out else [file_handler]
        }
        if verbose:
            log_config_kwargs['level'] = logging.DEBUG
        else:
            log_config_kwargs['level'] = logging.INFO
        logging.basicConfig(**log_config_kwargs)
        logging.Formatter.converter = time.gmtime

    def __init__(self):
        super().__init__()
        self.prog = 'tests'
        self.formatter_class = argparse.RawTextHelpFormatter
        self.epilog = """example:
    tests all
    tests all -d path/to/testDir
    tests list suite | case | method
    tests -verbose suite suitName
    tests case suiteName.caseName
    tests case suiteName.caseName.caseClass.methodName
        """

        self.add_argument(
            '-d', '--testDirPath', type=str, default='tests',
            help=f"Path to test dir containing tests to be discovered by TestLoader (default: tests)"
        )
        self.add_argument(
            '-l', '--logDirPath', type=str, default='.logs',
            help=f"Path to log dir containing debug log (default: .logs)"
        )
        self.add_argument('-s', '--logStdout', action='store_true', help='log to stdout as well as to a log file')
        self.add_argument(
            '-v', '--verbose', action='store_true',
            help=f"Make the test verbose (default False)"
        )
        self.add_argument('--meld', action='store_true', help='Use meld to compare out and exp file (default False)')
        sub_parser = self.add_subparsers(
            title='command', dest='command', metavar='<command>', parser_class=argparse.ArgumentParser
        )
        sub_parser.required = True
        suite_parser = sub_parser.add_parser('suite', help='Run a test suite')
        suite_parser.add_argument('suiteName', type=str, help=f"Name of the suite to be run")
        case_parser = sub_parser.add_parser('case', help='Run a test case or method')
        case_parser.add_argument('caseName', type=str,
                                 help="Path to TestCase or method from the testDir separated by dot")
        sub_parser.add_parser('all', help='Test all cases discovered from the testDir ')
        list_parser = sub_parser.add_parser(
            'list', help='List discovered test suites or cases or methods from the testDir'
        )
        list_parser.add_argument('level', type=str, choices=['suite', 'case', 'method'])

    def __call__(self):
        args = self.parse_args()
        test_dir = ROOT_PATH.joinpath(args.testDirPath)
        if not test_dir.exists():
            self.error(f"Test dir {test_dir} does not exist")
        os.environ['TEST_DIR_PATH'] = str(test_dir)

        self.config_log(args.logDirPath, args.logStdout, args.verbose)

        runner_verbosity = 2 if args.verbose else 1
        result_class = TestResultCompareFileMeld if args.meld else TestResultLogMetrics
        runner = unittest.TextTestRunner(verbosity=runner_verbosity, resultclass=result_class)

        all_succeed = True
        if args.command == 'all':
            test_suites = self.discover_suites(test_dir)
            results = [runner.run(test_suites[s_name]).wasSuccessful() for s_name in test_suites]
            all_succeed = all(results)
        elif args.command == 'suite':
            test_suites = self.discover_suites(test_dir)
            if args.suiteName in list(test_suites.keys()):
                all_succeed = runner.run(test_suites[args.suiteName]).wasSuccessful()
            else:
                self.error(f"Suite {args.suiteName} not found in {test_dir}. Use `list suite` command to list suite")
        elif args.command == 'case':
            all_cases = self.discover_cases(test_dir, silent=True)
            case_name = args.caseName
            all_succeed = runner.run(all_cases[case_name]).wasSuccessful()

        elif args.command == 'list':
            self.list_command(args.level, test_dir)

        if not all_succeed:
            sys.exit("Some tests failed")
