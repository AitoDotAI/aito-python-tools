"""The v1 sync client gives up on a call after 300 s by default

Before 1.3.1 it had no timeout: a hung connection blocked the caller for ever. The async path
already stopped at aiohttp's 300 s, so the sync one now matches it. ``AitoClient(...,
timeout=s)`` sets another, and ``timeout=None`` restores no limit. The credential check sends
it too. Offline: ``requests.request`` is replaced.
"""

from unittest import mock

import requests

from aito.v1.client import AitoClient, RequestError
from tests.cases import BaseTestCase
from tests.sdk.test_v1_write_contention_retry import URL, ok, response


class TimeoutCase(BaseTestCase):
    def client(self, responses=None, **kwargs):
        self.calls = []
        queue = list(responses or [])

        def fake_request(*args, **kw):
            self.calls.append(kw)
            item = queue.pop(0) if queue else ok()
            if isinstance(item, Exception):
                raise item
            return item
        patch = mock.patch('aito.v1.client.requestslib.request', fake_request)
        patch.start()
        self.addCleanup(patch.stop)
        kwargs.setdefault('check_credentials', False)
        return AitoClient(URL, 'key', **kwargs)


class TestTheDefaultIs300Seconds(TimeoutCase):
    def test_a_request_is_sent_with_300(self):
        client = self.client()
        client.request(method='GET', endpoint='/api/v1/schema')
        self.assertEqual(self.calls[-1]['timeout'], 300.0)
        self.assertEqual(client.timeout, 300.0)

    def test_the_credential_check_sends_it_too(self):
        self.client([response(200, {'version': '2.12.0'}), ok()], check_credentials=True, timeout=12.0)
        self.assertEqual([c['timeout'] for c in self.calls], [12.0, 12.0])


class TestItCanBeChanged(TimeoutCase):
    def test_another_timeout(self):
        client = self.client(timeout=5.0)
        client.request(method='GET', endpoint='/api/v1/schema')
        self.assertEqual(self.calls[-1]['timeout'], 5.0)

    def test_none_is_no_limit_as_before(self):
        client = self.client(timeout=None)
        client.request(method='GET', endpoint='/api/v1/schema')
        self.assertIsNone(self.calls[-1]['timeout'])

    def test_it_must_be_positive(self):
        for bad in (0, -1):
            with self.subTest(bad), self.assertRaises(ValueError):
                self.client(timeout=bad)


class TestATimeoutIsARequestError(TimeoutCase):
    def test_it_is_raised_as_before(self):
        client = self.client([requests.ReadTimeout('read timed out')])
        with self.assertRaises(RequestError) as raised:
            client.request(method='GET', endpoint='/api/v1/schema')
        self.assertIn('timed out', str(raised.exception))
        self.assertEqual(len(self.calls), 1)
