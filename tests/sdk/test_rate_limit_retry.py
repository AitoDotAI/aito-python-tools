"""The v2 client retries a 429, and no client retries a job's stored contention result

aito-core's admission control turns a v2 request away with 429 and ``Retry-After`` when it
is busy; it is off until clients retry (decision 3zzn). The client waits ``Retry-After``
(seconds, or an HTTP date) plus jitter and repeats the request, a capped number of times
and for at most 60 s in all for one request: a longer wait is raised at once, naming 429.

A job that loses to concurrent writes stores a 409 ``write.contention`` result. Fetching
it again returns the same result, so the job's status and result are never retried; the
error says to resubmit the job. Offline: a recording session plays the server.
"""

from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from unittest import mock

from aito.v2 import AitoV2Error
from tests.cases import BaseTestCase
from tests.sdk.test_v1_write_contention_retry import V1Case, contention as v1_contention, ok as v1_ok
from tests.sdk.test_v2_client import FakeResponse, make_client
from tests.sdk.test_write_contention_retry import OK, RetryCase, contention


def busy(retry_after='1'):
    return FakeResponse(429, {'kind': 'error', 'data': {
        'code': 'rate_limited', 'message': 'the server is busy: retry after the Retry-After delay'}},
        headers={'Retry-After': retry_after} if retry_after is not None else {})


def no_jitter():
    return mock.patch('aito._write_contention.random.uniform', side_effect=lambda lo, hi: lo)


class TestA429IsRetried(RetryCase):
    def test_a_query_turned_away_is_sent_again_until_it_is_let_in(self):
        client = self.client([busy(), busy(), OK])
        client.request('POST', '/_query', {'from': 'invoices'})
        self.assertEqual(self.sent(client), 3)
        self.assertEqual(len(self.sleeps), 2)

    def test_so_is_a_job_submission(self):
        client = self.client([busy(), FakeResponse(200, {'id': 'job-1'})])
        client.request('POST', '/jobs/_query', {'from': 'invoices'})
        self.assertEqual(self.sent(client), 2)

    def test_retry_after_in_seconds_is_the_floor(self):
        client = self.client([busy('4'), OK])
        with no_jitter():
            client.request('POST', '/_query', {'from': 't'})
        self.assertEqual(self.sleeps, [4.0])

    def test_retry_after_as_an_http_date_is_honoured(self):
        when = format_datetime(datetime.now(timezone.utc) + timedelta(seconds=30), usegmt=True)
        client = self.client([busy(when), OK])
        with no_jitter():
            client.request('POST', '/_query', {'from': 't'})
        self.assertAlmostEqual(self.sleeps[0], 30, delta=2)

    def test_jitter_spreads_the_retries(self):
        client = self.client([busy('2'), busy('2'), OK])
        with mock.patch('aito._write_contention.random.uniform', side_effect=lambda lo, hi: hi):
            client.request('POST', '/_query', {'from': 't'})
        self.assertEqual(self.sleeps, [2.5, 3.0])


class TestThe429RetriesAreCapped(RetryCase):
    def test_after_the_last_attempt_the_error_names_429_and_the_retries(self):
        client = self.client([busy(), busy(), busy(), busy(), OK])
        with self.assertRaises(AitoV2Error) as raised:
            client.request('POST', '/_query', {'from': 't'})
        self.assertEqual(self.sent(client), 4)
        self.assertEqual(raised.exception.status_code, 429)
        self.assertIn('429', str(raised.exception))
        self.assertIn('retried 3 times', str(raised.exception))

    def test_a_wait_beyond_a_minute_is_raised_at_once(self):
        client = self.client([busy('120'), OK])
        with self.assertRaises(AitoV2Error) as raised:
            client.request('POST', '/_query', {'from': 't'})
        self.assertEqual((self.sent(client), self.sleeps), (1, []))
        self.assertIn('asked to wait 120s', str(raised.exception))

    def test_the_waits_for_one_request_add_up_to_a_minute_at_most(self):
        client = self.client([busy('25'), busy('25'), busy('25'), OK])
        with no_jitter(), self.assertRaises(AitoV2Error):
            client.request('POST', '/_query', {'from': 't'})
        self.assertEqual(self.sleeps, [25.0, 25.0])

    def test_zero_turns_it_off(self):
        client = self.client([busy(), OK], rate_limit_retries=0)
        with self.assertRaises(AitoV2Error):
            client.request('POST', '/_query', {'from': 't'})
        self.assertEqual((self.sent(client), self.sleeps), (1, []))

    def test_a_negative_setting_is_refused(self):
        with self.assertRaises(ValueError):
            make_client(rate_limit_retries=-1)

    def test_contention_keeps_its_own_count(self):
        client = self.client([busy(), contention(), busy(), contention(), OK],
                             rate_limit_retries=2, write_contention_retries=2)
        client.request('POST', '/_delete', {'from': 't'})
        self.assertEqual(self.sent(client), 5)


class TestAJobsStoredContentionIsNotRefetched(RetryCase):
    def test_a_job_result_is_raised_at_once_saying_to_resubmit(self):
        for path in ('/jobs/1f2e/result', '/jobs/1f2e'):
            with self.subTest(path):
                client = self.client([contention(), OK])
                with self.assertRaises(AitoV2Error) as raised:
                    client.request('GET', path)
                self.assertEqual((self.sent(client), self.sleeps), (1, []))
                self.assertIn('submit the job again', str(raised.exception))

    def test_submitting_a_job_is_still_retried(self):
        client = self.client([contention(), FakeResponse(200, {'id': 'job-1'})])
        client.request('POST', '/jobs/data/_modify', {'from': 't'})
        self.assertEqual(self.sent(client), 2)


class TestV1JobsStoredContentionIsNotRefetched(V1Case):
    def test_a_job_result_is_not_retried(self):
        from aito.v1.client import RequestError
        client = self.client([v1_contention(), v1_ok()])
        with self.assertRaises(RequestError):
            client.request(method='GET', endpoint='/api/v1/jobs/1f2e/result')
        self.assertEqual((len(self.calls), self.sleeps), (1, []))

    def test_a_job_submission_still_is(self):
        client = self.client([v1_contention(), v1_ok()])
        client.request(method='POST', endpoint='/api/v1/jobs/data/_delete', query={'from': 't'})
        self.assertEqual(len(self.calls), 2)
