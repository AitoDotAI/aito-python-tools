"""Run the doctests embedded in the v2 client's docstrings

The examples that reach an instance are marked ``# doctest: +SKIP`` in the source
— they are there to be read, and the live suite in ``tests/sdk/test_v2_live.py``
is what proves they work. What runs here are the pure ones, chiefly
:func:`~aito.client.v2.responses.unwrap_payload`, whose whole job is to document
the two shapes a non-rows v2 endpoint can return.
"""

import doctest

import aito.client.v2.client
import aito.client.v2.errors
import aito.client.v2.responses


def load_tests(loader, tests, ignore):
    for module in (aito.client.v2.responses, aito.client.v2.errors, aito.client.v2.client):
        tests.addTests(doctest.DocTestSuite(module=module))
    return tests
