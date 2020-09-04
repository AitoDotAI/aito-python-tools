import doctest

import aito.schema


def load_tests(loader, tests, ignore):
    suite = doctest.DocTestSuite(aito.schema)
    tests.addTests(suite)
    return tests