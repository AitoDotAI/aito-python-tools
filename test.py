import argparse
import sys

from config import set_up_logger
from tests.test_case import *
from tests.test_result import *


class ArgParser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write(f"error: {message}\n")
        self.print_help()
        sys.exit(2)


def generate_parser():
    example_text = """example:
    python test.py all
    python test.py all --testDirPath tests/testDir
    python test.py suite tests.testSuite --verbose -- meld
    python test.py case tests.testDir.testFile.testClass
    python test.py list tests
    """
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter, epilog=example_text)
    parser.add_argument('--testDirPath',
                        help=f"Specify path to test dir containing tests from project root (default: tests)",
                        type=str,
                        default='tests')
    parser.add_argument('-v', '--verbosity', choices=[1, 2], help=f"Test verbosity (default 2)", type=int, default=2)
    parser.add_argument('-m', '--meld',
                        help='Use meld to compare out and exp file (default False)',
                        action='store_true')
    sub_parser = parser.add_subparsers(help="test option", dest='testOption')
    suite_parser = sub_parser.add_parser('suite', help='Test an existing suite')
    suite_parser.add_argument('suiteName', type=str, help=f"Name of the suite. Use list to see existing suite")
    case_parser = sub_parser.add_parser('case', help='Test an existing test case using')
    case_parser.add_argument('caseName', type=str,
                             help="Path to TestCase or method from project root separated by dot. "
                                  "E.g: test.testDir.testFile.TestCaseClass.testMethod")
    all_parser = sub_parser.add_parser('all', help='Test all existing cases from a testDir')
    list_parser = sub_parser.add_parser('listSuite', help='List test suites in a dir')
    list_parser.add_argument('dirPath', help='path to dir from project root')
    return parser


def get_test_suites(starting_dir: Path):
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
        tests = unittest.TestLoader().discover(d)
        if tests.countTestCases() > 0:
            parent = d.parent.stem
            test_suites[f"{parent}.{d.stem}"] = tests
    return test_suites


if __name__ == '__main__':
    test_parser = generate_parser()
    set_up_logger()

    args = test_parser.parse_args()
    if args.meld:
        result_class = TestResultCompareFileMeld
    else:
        result_class = unittest.TextTestResult
    runner = unittest.TextTestRunner(verbosity=args.verbosity, resultclass=result_class)
    result = False
    if args.testOption:
        if args.testOption == 'all':
            results = set()
            t_suites = get_test_suites(root_path().joinpath(args.testDirPath))
            for s in t_suites:
                results.add(runner.run(t_suites[s]).wasSuccessful())
            result = all(results)
        elif args.testOption == 'suite':
            t_suites = get_test_suites(root_path().joinpath(args.testDirPath))
            if args.suiteName in list(t_suites.keys()):
                result = runner.run(t_suites[args.suiteName]).wasSuccessful()
            else:
                sys.stderr.write(f"error: Suite {args.suiteName} not found in test dir {args.testDirPath}.\n"
                                 f"Try another test suite or test dir.\n")
                test_parser.print_help()
        elif args.testOption == 'case':
            suite = unittest.defaultTestLoader.loadTestsFromName(name=args.caseName)
            result = runner.run(suite).wasSuccessful()
        elif args.testOption == 'listSuite':
            t_suites = get_test_suites(root_path().joinpath(args.dirPath))
            if not t_suites:
                sys.stdout.write(f"No test suite found in {args.dirPath}")
            else:
                sys.stdout.write(f"{' '.join(t_suites.keys())}")
            result = True
        if not result:
            sys.exit("Some tests failed")
    else:
        sys.stderr.write(f"error: Testing option is required\n")
        test_parser.print_help()
        sys.exit(2)
