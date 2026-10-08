"""The v2 client retries a starved write: 409 ``write.contention`` (aito-core #1619)

Under heavy same-table write churn a large write can give up without committing anything
and answer 409 with the code ``write.contention`` and ``Retry-After``. Nothing was written,
so the request is safe to repeat. The client retries it, keyed on the CODE: the server's
other 409s (an env being migrated, an old binary format that needs a repair, a release or
branch conflict) are not this condition and must not be retried as it. The delay is the
Retry-After floor plus exponential full jitter, so clients starved together do not retry
in step; the attempts are capped. Offline: a recording session plays the server.
"""

from unittest import mock

from aito.v2 import AitoV2Error
from tests.cases import BaseTestCase
from tests.sdk.test_v2_client import FakeResponse, make_client

OK = FakeResponse(200, {'offset': 0, 'total': 0, 'hits': []})


def contention(retry_after='1'):
    return FakeResponse(409, {'kind': 'error', 'data': {
        'code': 'write.contention',
        'message': 'a positional write re-anchored 64 times without committing: concurrent '
                   'writes kept moving the table; retry the request'}},
        headers={'Retry-After': retry_after} if retry_after is not None else {})


def other_409(code, message):
    return FakeResponse(409, {'kind': 'error', 'data': {'code': code, 'message': message}})


class RetryCase(BaseTestCase):
    def client(self, responses, **kwargs):
        self.sleeps = []
        client = make_client(responses, **kwargs)
        patch = mock.patch('aito.v2.client.time.sleep', self.sleeps.append)
        patch.start()
        self.addCleanup(patch.stop)
        return client

    def sent(self, client):
        return len(client._session.calls)


class TestWriteContentionIsRetried(RetryCase):
    def test_a_starved_write_is_retried_until_it_lands(self):
        client = self.client([contention(), contention(), OK])
        client.request('POST', '/data/invoices/batch', [{'id': 'a'}])
        self.assertEqual(self.sent(client), 3)
        self.assertEqual(len(self.sleeps), 2)

    def test_the_same_request_is_sent_again(self):
        client = self.client([contention(), OK])
        client.request('POST', '/data/invoices/batch', [{'id': 'a'}])
        first, second = client._session.calls
        self.assertEqual((first['method'], first['url'], first['json']),
                         (second['method'], second['url'], second['json']))

    def test_retry_after_is_the_floor_and_jitter_spreads_the_retries(self):
        client = self.client([contention('2'), contention('2'), contention('2'), OK])
        with mock.patch('aito._write_contention.random.uniform', side_effect=lambda lo, hi: hi):
            client.request('POST', '/_delete', {'from': 't', 'where': {'a': 1}})
        # floor 2 s + full jitter up to 0.5 * 2**n (here its top: 0.5, 1, 2)
        self.assertEqual(self.sleeps, [2.5, 3.0, 4.0])
        self.sleeps.clear()
        client = self.client([contention('2'), OK])
        with mock.patch('aito._write_contention.random.uniform', side_effect=lambda lo, hi: lo):
            client.request('POST', '/_delete', {'from': 't'})
        self.assertEqual(self.sleeps, [2.0])

    def test_a_missing_or_unreadable_retry_after_means_one_second(self):
        for header in (None, 'soon', 'Wed, 21 Oct 2026 07:28:00 GMT'):
            with self.subTest(header):
                client = self.client([contention(header), OK])
                with mock.patch('aito._write_contention.random.uniform', side_effect=lambda lo, hi: lo):
                    client.request('POST', '/_delete', {'from': 't'})
                self.assertEqual(self.sleeps, [1.0])

    def test_the_jitter_is_capped(self):
        client = self.client([contention()] * 9 + [OK], write_contention_retries=9)
        with mock.patch('aito._write_contention.random.uniform', side_effect=lambda lo, hi: hi):
            client.request('POST', '/_delete', {'from': 't'})
        self.assertEqual(max(self.sleeps), 1.0 + 8.0)


class TestTheRetriesAreCapped(RetryCase):
    def test_after_the_last_attempt_the_error_is_raised_and_says_so(self):
        client = self.client([contention()] * 10)
        with self.assertRaises(AitoV2Error) as ctx:
            client.request('POST', '/_delete', {'from': 't'})
        self.assertEqual(ctx.exception.code, 'write.contention')
        self.assertEqual(ctx.exception.status_code, 409)
        self.assertIn('retried 3 times', str(ctx.exception))
        self.assertEqual(self.sent(client), 4)       # the request + 3 retries (the default)

    def test_zero_turns_the_retry_off(self):
        client = self.client([contention(), OK], write_contention_retries=0)
        with self.assertRaises(AitoV2Error) as ctx:
            client.request('POST', '/_delete', {'from': 't'})
        self.assertEqual(ctx.exception.code, 'write.contention')
        self.assertEqual((self.sent(client), self.sleeps), (1, []))


class TestOnlyThisConditionIsRetried(RetryCase):
    def test_other_409s_are_raised_at_once(self):
        others = [
            other_409('env.migrating', 'env main is being migrated; retry later'),
            other_409('database.incompatible', 'collection is in an older binary format: repair it'),
            other_409('ref.exists', "release 'v1' already exists"),
            FakeResponse(409, {'kind': 'error', 'data': {'message': 'conflict'}}),   # no code at all
        ]
        for response in others:
            with self.subTest(response._body):
                client = self.client([response, OK])
                with self.assertRaises(AitoV2Error):
                    client.request('POST', '/_delete', {'from': 't'})
                self.assertEqual((self.sent(client), self.sleeps), (1, []))

    def test_the_code_alone_decides_not_the_status(self):
        # a 500 or 503 is not retried as contention, even with a Retry-After
        for status in (500, 503, 429):
            with self.subTest(status):
                client = self.client([FakeResponse(status, {'kind': 'error', 'data': {'message': 'x'}},
                                                   headers={'Retry-After': '1'}), OK])
                with self.assertRaises(AitoV2Error):
                    client.request('POST', '/_delete', {'from': 't'})
                self.assertEqual(self.sent(client), 1)

    def test_a_negative_setting_is_refused(self):
        with self.assertRaises(ValueError):
            make_client(write_contention_retries=-1)
