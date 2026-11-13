"""A v2 upload stays under the engine's 10 MB request-body limit

The engine refuses a body over 10,000,000 bytes on ``/data/{t}/batch`` and ``/data/{t}/import``
with 413 ``payload_too_large`` (measured on v2.12.0; through a proxy the client may see a reset or
a broken pipe instead). So ``upload_entries`` splits by serialized size as well as by
``batch_size``, aiming each request at 9,000,000 bytes. A single row over the limit raises
before anything is sent, naming the limit. ``upload_csv`` sends a CSV over the limit through the
client path (``via='auto'``) or refuses it (``via='server'``). The MCP ``upload_rows`` tool uses
the same splitter. A dropped connection on a body over the limit says so. Offline: a recording
session plays the server.
"""

import json
import os
import unittest
from unittest import mock

import requests

from aito.v2 import AitoV2Error
from aito.v2.limits import MAX_REQUEST_BYTES, TARGET_REQUEST_BYTES, batch_body_bytes
from tests.cases import BaseTestCase
from tests.sdk.test_transient_retry import RaisingSession
from tests.sdk.test_v2_client import FakeResponse, make_client

try:
    from aito.mcp import server as mcp_server
except ImportError:  # the mcp extra is not installed
    mcp_server = None

MB = 1_000_000


def inserted(count):
    return FakeResponse(200, {'status': 'inserted', 'table': 't', 'count': count})


def rows_of(n, size):
    """n rows, each serializing to about ``size`` bytes"""
    return [{'id': str(i), 'text': 'x' * (size - 30)} for i in range(n)]


def body_bytes(call):
    """the bytes the call sent, serialized as requests serializes ``json=``"""
    if call['data'] is not None:
        return len(call['data'])
    return len(json.dumps(call['json']).encode('utf-8'))


class SizeCase(BaseTestCase):
    def client(self, responses=None):
        client = make_client(rate_limit_retries=0)
        client._session = RaisingSession(responses or [])
        return client

    def batches(self, client):
        return [c for c in client._session.calls if '/batch' in c['url']]


class TestTheLimit(BaseTestCase):
    def test_it_is_the_engines_exact_figure(self):
        self.assertEqual(MAX_REQUEST_BYTES, 10_000_000)
        self.assertEqual(TARGET_REQUEST_BYTES, 9_000_000)

    def test_the_body_size_matches_what_requests_sends(self):
        rows = [{'id': 'a', 'n': 1.5, 'é': 'ü'}, {'id': 'b'}]
        sizes = [len(json.dumps(r).encode('utf-8')) for r in rows]
        self.assertEqual(batch_body_bytes(sizes), len(json.dumps(rows).encode('utf-8')))


class TestUploadEntriesSplitsBySize(SizeCase):
    def test_a_12_mb_batch_is_split_and_lands_whole(self):
        rows = rows_of(120, 100_000)   # 12 MB in one batch_size of 1000
        client = self.client([inserted(n) for n in (89, 31)])
        count = client.upload_entries('t', rows)
        sent = self.batches(client)
        self.assertGreater(len(sent), 1)
        self.assertTrue(all(body_bytes(c) <= TARGET_REQUEST_BYTES for c in sent))
        self.assertEqual([r for c in sent for r in c['json']], rows)
        self.assertEqual(count, 120)

    def test_under_the_target_nothing_changes(self):
        rows = rows_of(5, 1000)
        client = self.client([inserted(2), inserted(2), inserted(1)])
        client.upload_entries('t', rows, batch_size=2)
        self.assertEqual([c['json'] for c in self.batches(client)], [rows[0:2], rows[2:4], rows[4:5]])

    def test_a_row_between_the_target_and_the_limit_goes_alone(self):
        rows = rows_of(1, 1000) + rows_of(1, int(9.5 * MB)) + rows_of(1, 1000)
        client = self.client([inserted(1)] * 3)
        client.upload_entries('t', rows)
        self.assertEqual([len(c['json']) for c in self.batches(client)], [1, 1, 1])

    def test_a_failed_split_batch_says_what_landed(self):
        rows = rows_of(120, 100_000)
        client = self.client([inserted(89), FakeResponse(
            400, {'kind': 'error', 'data': {'code': 'data.bad_request', 'message': 'bad'}})])
        with self.assertRaises(AitoV2Error) as raised:
            client.upload_entries('t', rows)
        error = raised.exception
        first = len(self.batches(client)[0]['json'])
        self.assertEqual((error.batches_landed, error.rows_landed, error.failed_batch), (1, first, 1))
        self.assertEqual(error.failed_batch_outcome, 'refused')


class TestARowOverTheLimit(SizeCase):
    def test_a_single_11_mb_row_raises_naming_the_limit(self):
        rows = rows_of(1, 1000) + rows_of(1, 11 * MB)
        client = self.client()
        with self.assertRaises(AitoV2Error) as raised:
            client.upload_entries('t', rows)
        message = str(raised.exception)
        self.assertIn('10,000,000', message)
        self.assertIn('row 1', message)
        self.assertEqual(raised.exception.code, 'payload_too_large')

    def test_exactly_the_limit_is_sent(self):
        # the engine accepts a body of exactly 10,000,000 bytes and refuses 10,000,001
        pad = MAX_REQUEST_BYTES - len(json.dumps([{'t': ''}]).encode('utf-8'))
        exact = [{'t': 'x' * pad}]
        self.assertEqual(len(json.dumps(exact).encode('utf-8')), MAX_REQUEST_BYTES)
        client = self.client([inserted(1)])
        client.upload_entries('t', exact)
        self.assertEqual(len(self.batches(client)), 1)
        with self.assertRaises(AitoV2Error):
            client.upload_entries('t', [{'t': 'x' * (pad + 1)}])

    def test_no_rows_send_nothing(self):
        client = self.client()
        self.assertEqual(client.upload_entries('t', []), 0)
        self.assertEqual(client._session.calls, [])

    def test_batch_size_must_be_positive(self):
        with self.assertRaises(ValueError):
            self.client().upload_entries('t', rows_of(2, 100), batch_size=0)

    def test_nothing_is_sent(self):
        rows = rows_of(1, 1000) + rows_of(1, 11 * MB)
        client = self.client()
        with self.assertRaises(AitoV2Error):
            client.upload_entries('t', rows)
        self.assertEqual(client._session.calls, [])


class TestADroppedConnectionOnABigBody(SizeCase):
    def test_it_names_the_limit(self):
        client = self.client([requests.ConnectionError('Broken pipe')])
        with self.assertRaises(AitoV2Error) as raised:
            client.request('POST', '/data/t/batch', rows_of(110, 100_000))
        self.assertIn('10,000,000', str(raised.exception))

    def test_a_raw_body_too(self):
        client = self.client([requests.ConnectionError('Connection reset by peer')])
        with self.assertRaises(AitoV2Error) as raised:
            client.request('POST', '/data/t/import', data=b'a\n' + b'x' * (11 * MB),
                           content_type='text/csv')
        self.assertIn('10,000,000', str(raised.exception))

    def test_a_small_body_says_nothing_of_it(self):
        client = self.client([requests.ConnectionError('reset')])
        with self.assertRaises(AitoV2Error) as raised:
            client.request('POST', '/data/t/batch', rows_of(2, 100))
        self.assertNotIn('10,000,000', str(raised.exception))


NOT_FOUND = FakeResponse(404, {'kind': 'error', 'data': {'code': 'not_found', 'message': 'no table'}})


def big_csv():
    line = b'A1,' + b'x' * 99_000 + b'\n'
    return b'id,text\n' + line * 110   # about 11 MB


class TestUploadCsvOverTheLimit(SizeCase):
    def test_auto_takes_the_client_path(self):
        client = self.client([NOT_FOUND, FakeResponse(200, {'type': 'collection'})]
                             + [inserted(90), inserted(20)])
        res = client.upload_csv('t', big_csv())
        self.assertEqual(res.via, 'client')
        self.assertFalse([c for c in client._session.calls if '/import' in c['url']])
        self.assertTrue(all(body_bytes(c) <= TARGET_REQUEST_BYTES for c in self.batches(client)))

    def test_server_raises_before_sending(self):
        client = self.client([NOT_FOUND])
        with self.assertRaises(AitoV2Error) as raised:
            client.upload_csv('t', big_csv(), via='server')
        self.assertIn('10,000,000', str(raised.exception))
        self.assertFalse([c for c in client._session.calls if '/import' in c['url']])


@unittest.skipIf(mcp_server is None, 'the mcp extra is not installed')
class TestMcpUploadRows(SizeCase):
    def test_a_12_mb_upload_is_split(self):
        rows = rows_of(120, 100_000)
        client = self.client([inserted(89), inserted(31)])
        with mock.patch.dict(os.environ, {mcp_server.WRITES_ENV: '1'}):
            server = mcp_server.build_server(client)
        from tests.sdk.test_mcp_server import run
        out = run(server, 'upload_rows', {'table': 't', 'rows': rows})
        self.assertEqual(out, {'table': 't', 'uploaded': 120})
        sent = self.batches(client)
        self.assertGreater(len(sent), 1)
        self.assertEqual([r for c in sent for r in c['json']], rows)
