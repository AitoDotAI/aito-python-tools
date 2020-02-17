import argparse
import sys

from aito.utils.generic_utils import root_path, set_up_logger
from tests.test_result import *


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

    parser.add_argument('-d', '--testDirPath',
                        help=f"Path to test dir containing tests to be discovered by TestLoader (default: tests)",
                        type=str,
                        default='tests')
    parser.add_argument('-l', '--metricsLogPath',
                        help=f"Path to metrics log file (default: metrics_timestamp.log)",
                        type=str)
    parser.add_argument('-v', '--verbosity', help=f"Make the test verbose (default False)", action='store_true')
    parser.add_argument('--meld',
                        help='Use meld to compare out and exp file (default False)',
                        action='store_true')
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
    test_dir = root_path().joinpath(args.testDirPath)
    relative_to_root = '.'.join(test_dir.relative_to(root_path()).parts)
    if not test_dir.exists():
        parser.error(f"Test dir {test_dir} does not exist")

    metric_log_path = Path(args.metricsLogPath) if args.metricsLogPath else test_dir / f"metrics.log"
    if not metric_log_path.parent.exists():
        parser.error(f"Metric log file dir {metric_log_path.parent} does not exist")
    os.environ['METRICS_LOG_PATH'] = str(metric_log_path)
    if args.verbosity:
        set_up_logger(logging_level=5)
        verbosity = 2
    else:
        set_up_logger()
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


def discover_test_methods(starting_dir: Path):
    discovered_tests = unittest.TestLoader().discover(str(starting_dir))

    def test_case_gen(t_suite):
        for test in t_suite:
            if unittest.suite._isnotsuite(test):
                yield test.id()
            else:
                for t in test_case_gen(test):
                    yield t

    return sorted(list(test_case_gen(discovered_tests)))


def discover_test_cases(starting_dir: Path):
    discovered_tests = unittest.TestLoader().discover(str(starting_dir))

    def test_case_gen(t_suite):
        for test in t_suite:
            if unittest.suite._isnotsuite(test):
                case = '.'.join(test.id().split('.')[:-1])
                yield case
            else:
                for t in test_case_gen(test):
                    yield t

    return sorted(list(set(test_case_gen(discovered_tests))))


if __name__ == '__main__':
    execute_parser(generate_parser())
