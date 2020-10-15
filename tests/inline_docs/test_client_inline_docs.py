import doctest
from os import environ
from pprint import pprint

import aito.client


def load_tests(loader, tests, ignore):
    grocery_demo_client = aito.client.aito_client.AitoClient(
        environ['AITO_GROCERY_DEMO_INSTANCE_URL'],
        environ['AITO_GROCERY_DEMO_API_KEY']
    )

    suite = doctest.DocTestSuite(module=aito.client.aito_client, extraglobs={'client': grocery_demo_client, 'pprint': pprint})
    tests.addTests(suite)
    return tests