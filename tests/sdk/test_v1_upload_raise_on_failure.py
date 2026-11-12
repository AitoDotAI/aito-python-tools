"""v1 ``api.upload_entries``: a failed batch is logged, and can stop the upload

By default the upload goes on past a failed batch, as it always has, and raises only when no
batch landed. Each failed batch is logged at ERROR with its row range, and an upload that
finished with some batches failed ends with an ERROR naming them all, not an INFO.

``raise_on_failure=True`` raises at the first failed batch instead: an
``UploadEntriesError`` carrying ``rows_uploaded`` (the rows of the batches before it, so
``entries[rows_uploaded:]`` is what is left) and ``failed_range``, with the request's error
as its cause. No later batch is sent and the table isn't optimized. Offline: the client's
``request`` is replaced.
"""

from unittest import mock

from aito.v1 import api
from aito.v1.api import UploadEntriesError
from aito.v1.client import AitoClient, RequestError
from aito.v1.requests import UploadEntriesRequest
from tests.cases import BaseTestCase

ROWS = [{'id': i} for i in range(5)]


class UploadCase(BaseTestCase):
    def client(self, outcomes):
        """outcomes: one per request, True for a batch that lands, False for one refused"""
        client = AitoClient('https://shared.aito.ai/db/test-db', 'key', check_credentials=False)
        self.sent = []
        queue = list(outcomes)

        def fake_request(*, request_obj, **kwargs):
            self.sent.append(request_obj)
            if isinstance(request_obj, UploadEntriesRequest) and not queue.pop(0):
                raise RequestError(request_obj, Exception('400 bad row'))
            return mock.Mock()
        client.request = fake_request
        return client

    def batches(self):
        return [r.query for r in self.sent if isinstance(r, UploadEntriesRequest)]


class TestByDefaultItGoesOn(UploadCase):
    def test_the_other_batches_are_sent(self):
        client = self.client([True, False, True])
        api.upload_entries(client, 't', ROWS, batch_size=2, optimize_on_finished=False)
        self.assertEqual(self.batches(), [ROWS[0:2], ROWS[2:4], ROWS[4:5]])

    def test_the_failed_batches_are_named_at_error_at_the_end(self):
        client = self.client([False, True, False])
        with self.assertLogs('AitoAPI', 'INFO') as logs:
            api.upload_entries(client, 't', ROWS, batch_size=2, optimize_on_finished=False)
        errors = [r.getMessage() for r in logs.records if r.levelname == 'ERROR']
        self.assertIn('batch 0:2 failed', errors[0])
        self.assertIn('batch 4:5 failed', errors[1])
        self.assertIn('uploaded 2/5 entries', errors[-1])
        self.assertIn('2 batches failed: rows 0:2, 4:5', errors[-1])

    def test_a_complete_upload_ends_at_info(self):
        client = self.client([True, True, True])
        with self.assertLogs('AitoAPI', 'INFO') as logs:
            api.upload_entries(client, 't', ROWS, batch_size=2, optimize_on_finished=False)
        self.assertFalse([r for r in logs.records if r.levelname == 'ERROR'])

    def test_nothing_landed_still_raises(self):
        client = self.client([False, False, False])
        with self.assertLogs('AitoAPI', 'ERROR'), self.assertRaises(Exception) as raised:
            api.upload_entries(client, 't', ROWS, batch_size=2, optimize_on_finished=False)
        self.assertIn('failed to upload any data', str(raised.exception))


class TestRaiseOnFailure(UploadCase):
    def test_it_stops_at_the_first_failed_batch(self):
        client = self.client([True, False, True])
        with self.assertLogs('AitoAPI', 'ERROR'), self.assertRaises(UploadEntriesError) as raised:
            api.upload_entries(client, 't', ROWS, batch_size=2, raise_on_failure=True)
        error = raised.exception
        self.assertEqual((error.rows_uploaded, error.failed_range), (2, (2, 4)))
        self.assertIsInstance(error.__cause__, RequestError)
        self.assertEqual(self.batches(), [ROWS[0:2], ROWS[2:4]])
        self.assertEqual(len(self.sent), 2)  # no optimize

    def test_the_message_says_where_to_resume(self):
        client = self.client([True, False])
        with self.assertLogs('AitoAPI', 'ERROR'), self.assertRaises(UploadEntriesError) as raised:
            api.upload_entries(client, 't', ROWS, batch_size=2, raise_on_failure=True)
        self.assertIn('batch 2:4 failed', str(raised.exception))
        self.assertIn('2 rows uploaded before it', str(raised.exception))

    def test_the_first_batch(self):
        client = self.client([False])
        with self.assertLogs('AitoAPI', 'ERROR'), self.assertRaises(UploadEntriesError) as raised:
            api.upload_entries(client, 't', ROWS, batch_size=2, raise_on_failure=True)
        self.assertEqual((raised.exception.rows_uploaded, raised.exception.failed_range), (0, (0, 2)))

    def test_the_last_partial_batch(self):
        client = self.client([True, True, False])
        with self.assertLogs('AitoAPI', 'ERROR'), self.assertRaises(UploadEntriesError) as raised:
            api.upload_entries(client, 't', ROWS, batch_size=2, raise_on_failure=True)
        self.assertEqual((raised.exception.rows_uploaded, raised.exception.failed_range), (4, (4, 5)))

    def test_a_complete_upload_is_unchanged(self):
        client = self.client([True, True, True])
        api.upload_entries(client, 't', ROWS, batch_size=2, optimize_on_finished=False, raise_on_failure=True)
        self.assertEqual(self.batches(), [ROWS[0:2], ROWS[2:4], ROWS[4:5]])

    def test_it_is_an_exception_as_before(self):
        # a caller catching Exception for the all-failed case catches this too
        self.assertTrue(issubclass(UploadEntriesError, Exception))
