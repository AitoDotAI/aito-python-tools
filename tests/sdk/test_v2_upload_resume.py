"""A v2 upload that fails at a batch says what had landed, so the caller can resume

``upload_entries`` sends the rows in batches and raises at the first that fails. The error is
the same ``AitoV2Error`` as before (a caller that catches it is unaffected), and it now carries
``batches_landed``, ``rows_landed`` (the rows of the batches before it: ``entries[rows_landed:]``
is what is left) and ``failed_batch`` (its 0-based index). A batch is all or nothing in the
engine, but one that failed in transit (a reset, a timeout, a 503) may have landed. Resending
from ``rows_landed`` is safe with ``on_conflict='update'`` into a keyed collection; without a
key, no resend is (the batch can land after any check). Other errors leave the three None. Offline: a recording
session plays the server.
"""

import requests

from aito.v2 import AitoV2Error
from tests.cases import BaseTestCase
from tests.sdk.test_transient_retry import RaisingSession, status
from tests.sdk.test_v2_client import FakeResponse, make_client


def inserted(count):
    return FakeResponse(200, {'status': 'inserted', 'table': 'invoices', 'count': count})


ROWS = [{'id': str(i)} for i in range(5)]


def client_with(responses):
    client = make_client(rate_limit_retries=0)  # a 429 is raised, not slept through
    client._session = RaisingSession(responses)
    return client


class TestAFailedBatchSaysWhatLanded(BaseTestCase):
    def test_a_refused_batch(self):
        client = client_with([inserted(2), status(400)])
        with self.assertRaises(AitoV2Error) as raised:
            client.upload_entries('invoices', ROWS, batch_size=2)
        error = raised.exception
        self.assertEqual((error.batches_landed, error.rows_landed, error.failed_batch), (1, 2, 1))
        self.assertEqual(error.status_code, 400)

    def test_a_dropped_connection(self):
        client = client_with([inserted(2), inserted(2), requests.ConnectionError('reset')])
        with self.assertRaises(AitoV2Error) as raised:
            client.upload_entries('invoices', ROWS, batch_size=2)
        error = raised.exception
        self.assertEqual((error.batches_landed, error.rows_landed, error.failed_batch), (2, 4, 2))

    def test_the_first_batch(self):
        client = client_with([status(503)])
        with self.assertRaises(AitoV2Error) as raised:
            client.upload_entries('invoices', ROWS, batch_size=2)
        error = raised.exception
        self.assertEqual((error.batches_landed, error.rows_landed, error.failed_batch), (0, 0, 0))

    def test_the_message_says_where_to_resume(self):
        client = client_with([inserted(2), status(400)])
        with self.assertRaises(AitoV2Error) as raised:
            client.upload_entries('invoices', ROWS, batch_size=2)
        self.assertIn('batch 1 failed and refused', str(raised.exception))
        self.assertIn('2 rows in 1 batch landed before it', str(raised.exception))

    def test_resuming_from_rows_landed_sends_only_the_rest(self):
        client = client_with([inserted(2), status(503), inserted(2), inserted(1)])
        try:
            client.upload_entries('invoices', ROWS, batch_size=2, on_conflict='update')
        except AitoV2Error as error:
            client.upload_entries('invoices', ROWS[error.rows_landed:], batch_size=2, on_conflict='update')
        sent = [row for call in client._session.calls for row in call['json']]
        self.assertEqual(sent, ROWS[:2] + ROWS[2:4] + ROWS[2:])


class TestTheFailedBatchsOutcome(BaseTestCase):
    """a 4xx refused the batch: none of it landed and rows_landed is exact. A 5xx, a reset or
    a timeout leaves it unknown: it may have landed, so rows_landed is a lower bound"""

    def test_a_4xx_is_refused(self):
        for code in (400, 401, 404, 409, 429):
            with self.subTest(code):
                client = client_with([inserted(2), status(code)])
                with self.assertRaises(AitoV2Error) as raised:
                    client.upload_entries('invoices', ROWS, batch_size=2)
                self.assertEqual(raised.exception.failed_batch_outcome, 'refused')

    def test_a_5xx_a_reset_or_a_timeout_is_unknown(self):
        for failure in (status(500), status(503), requests.ConnectionError('reset'), requests.ReadTimeout('slow')):
            with self.subTest(repr(failure)):
                client = client_with([inserted(2), failure])
                with self.assertRaises(AitoV2Error) as raised:
                    client.upload_entries('invoices', ROWS, batch_size=2)
                self.assertEqual(raised.exception.failed_batch_outcome, 'unknown')
                self.assertIn('may have landed', str(raised.exception))


class TestOtherErrorsCarryNone(BaseTestCase):
    def test_a_query_error(self):
        client = client_with([status(400)])
        with self.assertRaises(AitoV2Error) as raised:
            client.request('POST', '/_query', {'from': 't'})
        error = raised.exception
        self.assertEqual((error.batches_landed, error.rows_landed, error.failed_batch), (None, None, None))
        self.assertIsNone(error.failed_batch_outcome)
