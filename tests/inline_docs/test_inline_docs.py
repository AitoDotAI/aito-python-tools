import doctest

import aito.schema


def load_tests(loader, tests, ignore):
    tests.addTests(doctest.DocTestSuite(aito.schema))
    return tests