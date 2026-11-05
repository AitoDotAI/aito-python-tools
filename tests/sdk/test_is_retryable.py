"""`AitoV2Error.is_retryable`: whether repeating the same request later is safe and can succeed

For a caller that turns the client's retries off, or runs out of them, and decides itself.
True for what the client retries: 409 ``write.contention`` (nothing was committed) and 429
(turned away before it ran). Not a job's stored contention result: fetching it again returns
the same result; the job is resubmitted. Not a 503: the engine's write 503 "may or may not
have been applied", and its others are a timed-out query or a missing extension.
"""

from aito.v2 import AitoV2Error
from tests.sdk.test_rate_limit_retry import busy
from tests.sdk.test_v2_client import FakeResponse
from tests.sdk.test_write_contention_retry import RetryCase, contention


def error(status, code, message='x'):
    return FakeResponse(status, {'kind': 'error', 'data': {'code': code, 'message': message}})


class TestIsRetryable(RetryCase):
    def raised(self, response, method='POST', path='/_delete'):
        client = self.client([response], write_contention_retries=0, rate_limit_retries=0)
        with self.assertRaises(AitoV2Error) as ctx:
            client.request(method, path, {'from': 't'} if method == 'POST' else None)
        return ctx.exception

    def test_a_starved_write_is_retryable(self):
        self.assertTrue(self.raised(contention()).is_retryable)

    def test_a_request_turned_away_is_retryable(self):
        self.assertTrue(self.raised(busy()).is_retryable)

    def test_a_jobs_stored_contention_result_is_not(self):
        for path in ('/jobs/1f2e/result', '/jobs/1f2e'):
            with self.subTest(path):
                self.assertFalse(self.raised(contention(), 'GET', path).is_retryable)

    def test_other_errors_are_not(self):
        for status, code in ((409, 'env.migrating'), (503, 'store.not_writable'), (503, 'unavailable'),
                             (400, 'data.bad_request'), (404, 'not_found'), (500, 'internal')):
            with self.subTest(status=status, code=code):
                self.assertFalse(self.raised(error(status, code)).is_retryable)

    def test_an_error_built_without_a_response_is_not(self):
        self.assertFalse(AitoV2Error('the API key is empty').is_retryable)
