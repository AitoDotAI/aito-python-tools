import doctest
from os import environ
from pprint import pprint

import aito.v1.client


def load_tests(loader, tests, ignore):
    grocery_demo_client = aito.v1.client.AitoClient(
        environ['AITO_GROCERY_DEMO_INSTANCE_URL'],
        environ['AITO_GROCERY_DEMO_API_KEY']
    )

    suite = doctest.DocTestSuite(module=aito.v1.client, extraglobs={'client': grocery_demo_client, 'pprint': pprint})
    tests.addTests(suite)
    return tests