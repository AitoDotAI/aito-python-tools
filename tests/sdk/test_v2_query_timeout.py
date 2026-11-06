"""Every v2 query method takes a per-call timeout

``query``, ``evaluate`` and ``batch`` always did; ``search``, ``predict``, ``recommend``,
``relate``, ``match``, ``estimate`` and ``aggregate`` now do too, with the same meaning:
``timeout=`` overrides the client's for that one call, and leaving it out keeps the client's.
Offline: a recording session plays the server.
"""

from tests.cases import BaseTestCase
from tests.sdk.test_v2_client import FakeResponse, make_client

CALLS = {
    'search': dict(from_table='t'),
    'predict': dict(from_table='t', predict='x'),
    'recommend': dict(from_table='t', recommend='x', goal={'y': 1}),
    'relate': dict(from_table='t', relate='x'),
    'match': dict(from_table='t', match='x'),
    'estimate': dict(from_table='t', estimate='x'),
    'aggregate': dict(from_table='t', aggregate=['x.$sum']),
}


def answer(method):
    if method == 'estimate':
        return FakeResponse(200, {'kind': 'estimate', 'data': {'estimate': 1.0}})
    if method == 'aggregate':
        return FakeResponse(200, {'kind': 'aggregate', 'data': {'x.$sum': 1}})
    return FakeResponse(200, {'offset': 0, 'total': 0, 'hits': []})


class TestPerCallTimeout(BaseTestCase):
    def test_a_timeout_given_to_the_call_is_sent(self):
        for method, kwargs in CALLS.items():
            with self.subTest(method):
                client = make_client([answer(method)], timeout=30.0)
                getattr(client, method)(**kwargs, timeout=2.5)
                self.assertEqual(client._session.last['timeout'], 2.5)
                self.assertNotIn('timeout', client._session.last['json'])

    def test_without_one_the_clients_is_sent(self):
        for method, kwargs in CALLS.items():
            with self.subTest(method):
                client = make_client([answer(method)], timeout=12.0)
                getattr(client, method)(**kwargs)
                self.assertEqual(client._session.last['timeout'], 12.0)
