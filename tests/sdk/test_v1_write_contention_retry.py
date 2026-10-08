"""The v1 client retries 409 ``write.contention`` too, sync and async (aito-core #1619)

Most traffic is still on v1, so a large v1 delete under same-table churn is the likelier
real case. Same policy as v2 (aito/_write_contention.py): keyed on the body's ``error``
code, never on 409 alone; the Retry-After floor plus exponential full jitter; capped.
Offline: the HTTP layer is replaced by real ``requests.Response`` objects (sync) and a
small aiohttp stand-in (async).
"""

import asyncio
import json
from unittest import mock

import requests

from aito.v1.client import AitoClient, RequestError
from tests.cases import BaseTestCase

URL = 'https://shared.aito.ai/db/test-db'


def response(status, body, retry_after=None):
    r = requests.Response()
    r.status_code = status
    r._content = json.dumps(body).encode()
    r.url = URL + '/api/v1/data/t/delete'
    r.reason = 'Conflict' if status == 409 else 'OK'
    if retry_after is not None:
        r.headers['Retry-After'] = retry_after
    return r


CONTENTION = {'error': 'write.contention',
              'message': 'a positional write re-anchored 64 times without committing; retry the request'}


def contention(retry_after='1'):
    return response(409, CONTENTION, retry_after)


def ok():
    return response(200, {'total': 3})


class V1Case(BaseTestCase):
    def client(self, responses, **kwargs):
        self.sleeps, self.calls = [], []
        queue = list(responses)

        def fake_request(**kw):
            self.calls.append(kw)
            return queue.pop(0)
        for target, value in (('aito.v1.client.requestslib.request', fake_request),
                              ('aito.v1.client.time.sleep', self.sleeps.append)):
            patch = mock.patch(target, value)
            patch.start()
            self.addCleanup(patch.stop)
        return AitoClient(URL, 'key', check_credentials=False, **kwargs)

    def delete(self, client, **kwargs):
        return client.request(method='POST', endpoint='/api/v1/data/t/delete',
                              query={'from': 't', 'where': {'a': 1}}, **kwargs)


class TestV1Sync(V1Case):
    def test_a_starved_write_is_retried_until_it_lands(self):
        client = self.client([contention(), contention(), ok()])
        res = self.delete(client)
        self.assertEqual(res.json, {'total': 3})
        self.assertEqual((len(self.calls), len(self.sleeps)), (3, 2))
        self.assertEqual(self.calls[0], self.calls[2])          # the same request again

    def test_retry_after_floor_plus_jitter(self):
        client = self.client([contention('2'), contention('2'), ok()])
        with mock.patch('aito._write_contention.random.uniform', side_effect=lambda lo, hi: hi):
            self.delete(client)
        self.assertEqual(self.sleeps, [2.5, 3.0])

    def test_capped_and_the_error_says_so(self):
        client = self.client([contention()] * 10)
        with self.assertRaises(RequestError) as ctx:
            self.delete(client)
        self.assertIn('retried 3 times', str(ctx.exception))
        self.assertIn('retry the request', str(ctx.exception))
        self.assertEqual(len(self.calls), 4)

    def test_zero_turns_it_off_and_raise_for_status_false_returns_the_error(self):
        client = self.client([contention(), ok()], write_contention_retries=0)
        result = self.delete(client, raise_for_status=False)
        self.assertIsInstance(result, RequestError)
        self.assertEqual((len(self.calls), self.sleeps), (1, []))

    def test_other_409s_and_other_statuses_are_not_retried(self):
        for status, body in ((409, {'error': 'env.migrating', 'message': 'migrating'}),
                             (409, {'message': 'release v1 already exists'}),
                             (500, CONTENTION), (503, {'message': 'busy'})):
            with self.subTest(status=status, body=body):
                client = self.client([response(status, body, '1'), ok()])
                with self.assertRaises(RequestError):
                    self.delete(client)
                self.assertEqual((len(self.calls), self.sleeps), (1, []))

    def test_a_negative_setting_is_refused(self):
        with self.assertRaises(ValueError):
            AitoClient(URL, 'key', check_credentials=False, write_contention_retries=-1)


class FakeAiohttpResponse:
    def __init__(self, status, body, retry_after=None):
        self.status, self._body = status, body
        self.headers = {'Retry-After': retry_after} if retry_after else {}

    async def json(self, content_type='application/json'):
        return self._body

    def raise_for_status(self):
        if self.status >= 400:
            from aiohttp import ClientResponseError
            raise ClientResponseError(request_info=mock.Mock(real_url=URL), history=(),
                                      status=self.status, message=json.dumps(self._body))

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class FakeSession:
    def __init__(self, responses):
        self.responses, self.calls = list(responses), []

    def request(self, **kw):
        self.calls.append(kw)
        return self.responses.pop(0)


class TestV1Async(BaseTestCase):
    def run_delete(self, responses, **kwargs):
        self.sleeps = []

        async def fake_sleep(seconds):
            self.sleeps.append(seconds)
        session = FakeSession(responses)
        client = AitoClient(URL, 'key', check_credentials=False, **kwargs)
        with mock.patch('aito.v1.client.asyncio.sleep', fake_sleep):
            result = asyncio.run(client.async_request(
                session, method='POST', endpoint='/api/v1/data/t/delete', query={'from': 't'},
                raise_for_status=False))
        return session, result

    def test_a_starved_write_is_retried_until_it_lands(self):
        session, result = self.run_delete([FakeAiohttpResponse(409, CONTENTION, '1'),
                                           FakeAiohttpResponse(200, {'total': 3})])
        self.assertEqual(result.json, {'total': 3})
        self.assertEqual((len(session.calls), len(self.sleeps)), (2, 1))
        self.assertGreaterEqual(self.sleeps[0], 1.0)

    def test_other_409s_are_not_retried(self):
        session, result = self.run_delete([FakeAiohttpResponse(409, {'error': 'env.migrating'}, '1'),
                                           FakeAiohttpResponse(200, {'total': 3})])
        self.assertIsInstance(result, RequestError)
        self.assertEqual((len(session.calls), self.sleeps), (1, []))

    def test_capped(self):
        session, result = self.run_delete([FakeAiohttpResponse(409, CONTENTION, '1')] * 5,
                                          write_contention_retries=2)
        self.assertIsInstance(result, RequestError)
        self.assertIn('retried 2 times', str(result))
        self.assertEqual(len(session.calls), 3)
