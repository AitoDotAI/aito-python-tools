import argparse
import logging
import os
import sys
import unittest
from pathlib import Path

from aito.utils.generic_utils import logging_config, ROOT_PATH
from tests.results import TestResultLogMetrics, TestResultCompareFileMeld


class ArgParser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write(f"error: {message}\n")
        self.print_help()
        sys.exit(2)


def generate_parser() -> ArgParser:
    example_text = """example:
    test all
    test all --testDirPath tests/testDir
    test suite tests.testSuite --verbose --meld
    test case testDir.testFile.testClass.testMethod
    test list suite
    test list case
    """
    parser = ArgParser()
    parser.prog = 'test'
    parser.epilog = example_text

    parser.add_argument(
        '-d', '--testDirPath', type=str, default='tests',
        help=f"Path to test dir containing tests to be discovered by TestLoader (default: tests)"
    )
    parser.add_argument(
        '-l', '--logDirPath', type=str, default='logs',
        help=f"Path to log dir containing debug log (default: logs)"
    )
    parser.add_argument(
        '-v', '--verbosity', action='store_true',
        help=f"Make the test verbose (default False)"
    )
    parser.add_argument('--meld', action='store_true', help='Use meld to compare out and exp file (default False)')
    sub_parser = parser.add_subparsers(
        title='action',
        description='action to perform ',
        dest='action',
        parser_class=ArgParser,
        metavar='<action>'
    )
    sub_parser.required = True
    suite_parser = sub_parser.add_parser('suite', help='Test a suite')
    suite_parser.add_argument('suiteName', type=str, help=f"Name of the suite")
    case_parser = sub_parser.add_parser('case', help='Test a test case or method')
    case_parser.add_argument('caseName', type=str,
                             help="Path to TestCase or method from project root separated by dot. "
                                  "E.g: testFile.TestCaseClass.testMethod")
    sub_parser.add_parser('all', help='Test all cases discovered from the testDir ')
    list_parser = sub_parser.add_parser('list', help='List discovered test suites or cases from the testDir')
    list_parser.add_argument('level', type=str, choices=['suite', 'case', 'method'])
    return parser


def execute_parser(parser):
    args = parser.parse_args()
    test_dir = ROOT_PATH.joinpath(args.testDirPath)
    if not test_dir.exists():
        parser.error(f"Test dir {test_dir} does not exist")

    log_dir = ROOT_PATH.joinpath(args.logDirPath)
    log_dir.mkdir(exist_ok=True)
    os.environ['METRICS_LOG_PATH'] = str(log_dir / 'test_metrics.log')
    if args.verbosity:
        logging_config(filename=str(log_dir/'test.log'), level=logging.DEBUG)
        verbosity = 2
    else:
        logging_config(filename=str(log_dir / 'test.log'))
        verbosity = 1
    result_class = TestResultCompareFileMeld if args.meld else TestResultLogMetrics
    runner = unittest.TextTestRunner(verbosity=verbosity, resultclass=result_class)

    all_succeed = True
    test_suites = discover_test_suites(test_dir)
    if args.action == 'all':
        results = [runner.run(test_suites[s_name]).wasSuccessful() for s_name in test_suites]
        all_succeed = all(results)
    elif args.action == 'suite':
        if args.suiteName in list(test_suites.keys()):
            all_succeed = runner.run(test_suites[args.suiteName]).wasSuccessful()
        else:
            parser.error(f"Suite {args.suiteName} not found in {test_dir}. Use `list` option to list suite")
    elif args.action == 'case':
        relative_to_root = '.'.join(test_dir.relative_to(ROOT_PATH).parts)
        suite = unittest.defaultTestLoader.loadTestsFromName(f"{relative_to_root}.{args.caseName}")
        all_succeed = runner.run(suite).wasSuccessful()
    elif args.action == 'list':
        if args.level == 'suite':
            names = list(test_suites.keys())
        elif args.level == 'case':
            names = discover_test_cases(test_dir)
        else:
            names = discover_test_methods(test_dir)
        if not names:
            sys.stdout.write(f"No {args.level} found in {test_dir}")
        else:
            sys.stdout.write('\n'.join(names))
            sys.stdout.write('\n')

    if not all_succeed:
        sys.exit("Some tests failed")


def discover_test_suites(starting_dir: Path):
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
        tests = unittest.TestLoader().discover(str(d))
        if tests.countTestCases() > 0:
            test_suites['.'.join(d.relative_to(starting_dir).parts)] = tests
    return test_suites


def is_not_suite(test: unittest.suite.TestSuite):
    try:
        iter(test)
    except TypeError:
        return True
    return False


def discover_test_methods(starting_dir: Path):
    discovered_tests = unittest.TestLoader().discover(str(starting_dir))

    def test_case_gen(t_suite):
        for test in t_suite:
            if is_not_suite(test):
                yield test.id()
            else:
                for t in test_case_gen(test):
                    yield t

    return sorted(list(test_case_gen(discovered_tests)))


def discover_test_cases(starting_dir: Path):
    discovered_tests = unittest.TestLoader().discover(str(starting_dir))

    def test_case_gen(t_suite):
        for test in t_suite:
            if is_not_suite(test):
                case = '.'.join(test.id().split('.')[:-1])
                yield case
            else:
                for t in test_case_gen(test):
                    yield t

    return sorted(list(set(test_case_gen(discovered_tests))))


if __name__ == '__main__':
    execute_parser(generate_parser())
