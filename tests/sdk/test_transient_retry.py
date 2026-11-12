"""The v2 client can retry a transient failure, opt-in, and only where repeating is safe

``AitoClientV2(retries=n, backoff=s)`` repeats a request that met a 502, 503 or 504, a
dropped connection or a timeout, up to n times, waiting up to ``s * 2**attempt`` seconds
(full jitter, at most 8 s; a 503's ``Retry-After`` is the floor). The default is 2 retries
with a 0.5 s backoff; ``retries=0`` turns them off. A read timeout is retried only with
``retry_read_timeouts=True``: repeating a request a busy server didn't answer in time adds
to its load, and turns a 30 s timeout into about 90 s. A connect timeout never reached the
server and is retried like a dropped connection.

Only requests that change nothing are repeated: GETs, the query endpoints (``_query``,
``_search``, ``_predict``, ``_recommend``, ``_relate``, ``_match``, ``_estimate``,
``_aggregate``, ``_evaluate``, ``_batch``), ``optimize``, and a batch sent with
``on_conflict=update`` or ``ignore``, which lands once however often it is sent. Never a plain
batch insert (into a table without a primary key, a batch sent twice stores its rows twice), a
delete, a schema write,
``_modify`` or a job submission: a write that failed in transit may have landed. Offline: a
recording session plays the server.
"""

from unittest import mock

import requests

from aito.v2 import AitoV2Error
from tests.sdk.test_v2_client import FakeResponse, RecordingSession
from tests.sdk.test_write_contention_retry import OK, RetryCase


def status(code, retry_after=None):
    return FakeResponse(code, {'kind': 'error', 'data': {'code': 'unavailable', 'message': f'HTTP {code}'}},
                        headers={'Retry-After': retry_after} if retry_after is not None else {})


class RaisingSession(RecordingSession):
    """a queued exception is raised, as requests raises a dropped connection or a timeout"""

    def request(self, *args, **kwargs):
        response = super().request(*args, **kwargs)
        if isinstance(response, Exception):
            raise response
        return response


def jitter_at_most():
    return mock.patch('aito._write_contention.random.uniform', side_effect=lambda lo, hi: hi)


class TransientCase(RetryCase):
    def client(self, responses, **kwargs):
        client = super().client([], **kwargs)
        client._session = RaisingSession(responses)
        return client


class TestOnByDefault(TransientCase):
    def test_a_503_on_a_query_is_retried_twice(self):
        client = self.client([status(503), status(503), OK])
        client.request('POST', '/_predict', {'from': 't', 'predict': 'x'})
        self.assertEqual((self.sent(client), len(self.sleeps)), (3, 2))

    def test_the_third_failure_is_raised(self):
        client = self.client([status(503)] * 3 + [OK])
        with self.assertRaises(AitoV2Error) as raised:
            client.request('POST', '/_query', {'from': 't'})
        self.assertEqual(self.sent(client), 3)
        self.assertEqual(raised.exception.status_code, 503)

    def test_the_default_backoff_is_half_a_second(self):
        client = self.client([status(503), status(503), OK])
        with jitter_at_most():
            client.request('POST', '/_query', {'from': 't'})
        self.assertEqual(self.sleeps, [0.5, 1.0])

    def test_retries_0_turns_them_off(self):
        client = self.client([status(503), OK], retries=0)
        with self.assertRaises(AitoV2Error) as raised:
            client.request('POST', '/_predict', {'from': 't', 'predict': 'x'})
        self.assertEqual((self.sent(client), self.sleeps), (1, []))
        self.assertEqual(raised.exception.status_code, 503)

    def test_a_dropped_connection_is_raised_at_once_with_retries_0(self):
        client = self.client([requests.ConnectionError('reset'), OK], retries=0)
        with self.assertRaises(AitoV2Error):
            client.request('POST', '/_query', {'from': 't'})
        self.assertEqual(self.sent(client), 1)

    def test_a_plain_batch_insert_is_never_retried_by_default(self):
        # a batch into a collection without a key appends: sent twice, its rows land twice
        for failure in (status(502), status(503), status(504), requests.ConnectionError('reset'),
                        requests.ReadTimeout('slow')):
            with self.subTest(repr(failure)):
                client = self.client([failure, OK, OK])
                with self.assertRaises(AitoV2Error):
                    client.upload_entries('invoices', [{'id': 'a'}])
                self.assertEqual((self.sent(client), self.sleeps), (1, []))

    def test_a_read_timeout_is_not_retried_by_default(self):
        client = self.client([requests.ReadTimeout('read timed out'), OK])
        with self.assertRaises(AitoV2Error) as raised:
            client.request('POST', '/_predict', {'from': 't', 'predict': 'x'})
        self.assertEqual((self.sent(client), self.sleeps), (1, []))
        self.assertIn('read timed out', str(raised.exception))

    def test_a_connect_timeout_is_retried_by_default(self):
        # nothing reached the server: no load added by trying again
        client = self.client([requests.ConnectTimeout('connect timed out'), OK])
        client.request('POST', '/_query', {'from': 't'})
        self.assertEqual(self.sent(client), 2)

    def test_retry_read_timeouts_opts_in(self):
        client = self.client([requests.ReadTimeout('read timed out'), OK], retry_read_timeouts=True)
        client.request('POST', '/_query', {'from': 't'})
        self.assertEqual(self.sent(client), 2)

    def test_retry_read_timeouts_never_reaches_a_write(self):
        client = self.client([requests.ReadTimeout('read timed out'), OK], retry_read_timeouts=True)
        with self.assertRaises(AitoV2Error):
            client.upload_entries('invoices', [{'id': 'a'}])
        self.assertEqual(self.sent(client), 1)

    def test_retries_must_not_be_negative(self):
        with self.assertRaises(ValueError):
            self.client([], retries=-1)
        with self.assertRaises(ValueError):
            self.client([], backoff=-1)


class TestATransientFailureIsRetried(TransientCase):
    def test_502_503_and_504_on_a_query(self):
        for code in (502, 503, 504):
            with self.subTest(code):
                client = self.client([status(code), status(code), OK], retries=2)
                client.request('POST', '/_predict', {'from': 't', 'predict': 'x'})
                self.assertEqual((self.sent(client), len(self.sleeps)), (3, 2))

    def test_a_dropped_connection_and_a_timeout(self):
        for error in (requests.ConnectionError('Connection reset by peer'), requests.ReadTimeout('read timed out'),
                      requests.ConnectTimeout('connect timed out')):
            with self.subTest(type(error).__name__):
                client = self.client([error, OK], retries=1, retry_read_timeouts=True)
                client.request('POST', '/_query', {'from': 't'})
                self.assertEqual(self.sent(client), 2)

    def test_every_request_that_changes_nothing(self):
        safe = [('GET', '/schema'), ('GET', '/_version'), ('GET', '/jobs/job-1'),
                ('POST', '/data/invoices/optimize')]
        safe += [('POST', f'/_{q}') for q in ('query', 'search', 'predict', 'recommend', 'relate', 'match',
                                                'estimate', 'aggregate', 'evaluate', 'batch')]
        for method, path in safe:
            with self.subTest(f'{method} {path}'):
                client = self.client([status(503), OK], retries=1)
                client.request(method, path, {} if method == 'POST' else None)
                self.assertEqual(self.sent(client), 2)

    def test_the_same_request_is_sent_again(self):
        client = self.client([status(503), OK], retries=1)
        client.request('POST', '/_predict', {'from': 't', 'predict': 'x'})
        first, second = client._session.calls
        self.assertEqual((first['method'], first['url'], first['json']),
                         (second['method'], second['url'], second['json']))


class TestAWriteIsNeverRetried(TransientCase):
    UNSAFE = [('POST', '/data/invoices/batch'), ('POST', '/data/invoices/stream'), ('POST', '/data/invoices/import'),
              ('POST', '/data/_delete'), ('POST', '/data/_modify'), ('PUT', '/schema/invoices'),
              ('DELETE', '/schema/invoices'), ('POST', '/schema/_copy'), ('POST', '/_envs'),
              ('DELETE', '/_envs/b'), ('POST', '/jobs/_query')]

    def test_not_on_a_503(self):
        for method, path in self.UNSAFE:
            with self.subTest(f'{method} {path}'):
                client = self.client([status(503), OK], retries=3)
                with self.assertRaises(AitoV2Error):
                    client.request(method, path, [{'id': 'a'}])
                self.assertEqual(self.sent(client), 1)

    def test_not_on_a_dropped_connection_or_a_timeout(self):
        # the rows may have landed before the connection dropped
        for error in (requests.ConnectionError('Connection reset by peer'), requests.ReadTimeout('read timed out')):
            with self.subTest(type(error).__name__):
                client = self.client([error, OK], retries=3)
                with self.assertRaises(AitoV2Error):
                    client.upload_entries('invoices', [{'id': 'a'}])
                self.assertEqual(self.sent(client), 1)


class TestOtherFailuresAreNotRetried(TransientCase):
    def test_a_500_or_a_4xx(self):
        for code in (500, 400, 401, 404):
            with self.subTest(code):
                client = self.client([status(code), OK], retries=3)
                with self.assertRaises(AitoV2Error):
                    client.request('POST', '/_query', {'from': 't'})
                self.assertEqual(self.sent(client), 1)


class TestTheRetriesAreCappedAndSpread(TransientCase):
    def test_after_the_last_attempt_the_error_names_the_retries(self):
        client = self.client([status(503)] * 4 + [OK], retries=3)
        with self.assertRaises(AitoV2Error) as raised:
            client.request('POST', '/_query', {'from': 't'})
        self.assertEqual(self.sent(client), 4)
        self.assertEqual(raised.exception.status_code, 503)
        self.assertIn('retried 3 times', str(raised.exception))

    def test_a_connection_that_keeps_dropping_names_the_retries(self):
        client = self.client([requests.ConnectionError('reset')] * 3, retries=2)
        with self.assertRaises(AitoV2Error) as raised:
            client.request('POST', '/_query', {'from': 't'})
        self.assertEqual(self.sent(client), 3)
        self.assertIn('retried 2 times', str(raised.exception))

    def test_the_wait_doubles_from_the_backoff(self):
        client = self.client([status(503), status(503), status(503), OK], retries=3, backoff=1.0)
        with jitter_at_most():
            client.request('POST', '/_query', {'from': 't'})
        self.assertEqual(self.sleeps, [1.0, 2.0, 4.0])

    def test_the_wait_is_capped(self):
        client = self.client([status(503)] * 2 + [OK], retries=2, backoff=10.0)
        with jitter_at_most():
            client.request('POST', '/_query', {'from': 't'})
        self.assertEqual(self.sleeps, [8.0, 8.0])

    def test_a_503s_retry_after_is_the_floor(self):
        client = self.client([status(503, retry_after='3'), OK], retries=1, backoff=1.0)
        with jitter_at_most():
            client.request('POST', '/_query', {'from': 't'})
        self.assertEqual(self.sleeps, [4.0])


class TestAnUpsertBatchIsRetried(TransientCase):
    """A batch sent with on_conflict=update or ignore leaves the collection the same however often
    it lands, and the engine refuses either on a collection without a key: safe to repeat"""

    def test_on_conflict_update_and_ignore(self):
        for value in ('update', 'ignore'):
            with self.subTest(value):
                client = self.client([status(503), requests.ConnectionError('reset'), OK], retries=2)
                client.upload_entries('invoices', [{'id': 'a'}], on_conflict=value)
                self.assertEqual(self.sent(client), 3)

    def test_a_plain_batch_or_on_conflict_error_is_still_never_retried(self):
        for path in ('/data/invoices/batch', '/data/invoices/batch?on_conflict=error',
                     '/data/invoices/batch?on_conflict=update&on_conflict=error',
                     '/data/invoices/batch?on_conflict=', '/data/invoices/stream?on_conflict=update',
                     '/data/invoices/import?on_conflict=update'):
            with self.subTest(path):
                client = self.client([status(503), OK], retries=3)
                with self.assertRaises(AitoV2Error):
                    client.request('POST', path, [{'id': 'a'}])
                self.assertEqual(self.sent(client), 1)
