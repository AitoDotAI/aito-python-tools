"""A file upload an engine cannot take falls back to sending the rows in batches

The v1 file upload asks the engine for a presigned S3 URL (POST /api/v1/data/{table}/file).
A self-hosted engine has no S3: v2.12.0 and older answer a bare 500 ("There was an internal
server error.", nothing to tell it apart), newer ones a 501 ``file_upload.unavailable``. The
initiate call commits nothing, so the client then sends the rows through /batch, as it
already does for a multitenant instance. A 500 on Aito's own cloud, which has the storage,
is a real failure and still raises. Offline: a fake server answers.
"""

import gzip
import io
import json
from unittest import mock

import requests

import aito.v1.api as api
from aito.v1.client import AitoClient, RequestError
from tests.cases import BaseTestCase

ROWS = [{'a': 'x'}, {'a': 'y'}, {'a': 'z'}]
UNAVAILABLE = {'status': 501, 'error': 'file_upload.unavailable',
               'message': 'file upload is not available on this engine: it uploads through S3, '
                          'and this engine has no S3 storage configured',
               'alternatives': ['/api/v1/data/{t}/batch', '/api/v2/data/{t}/stream']}


def response(status, body, reason='OK'):
    r = requests.Response()
    r.status_code = status
    r._content = (body if isinstance(body, str) else json.dumps(body)).encode()
    r.reason = reason
    return r


def gzipped(rows):
    return io.BytesIO(gzip.compress(''.join(json.dumps(r) + '\n' for r in rows).encode()))


class FakeEngine:
    """answers /version, the file upload's initiate call with `initiate`, /batch and /optimize"""

    def __init__(self, initiate):
        self.initiate, self.batches, self.calls = initiate, [], []

    def __call__(self, **kw):
        method, url = kw['method'], kw['url']
        self.calls.append((method, url.split('/api/v1')[-1]))
        if url.endswith('/version'):
            return response(200, {'version': '2.12.0'})
        if url.endswith('/file'):
            return self.initiate
        if url.endswith('/batch'):
            self.batches.append(kw['json'])
            return response(200, {'entries': len(kw['json']), 'status': 'ok'})
        if url.endswith('/optimize'):
            return response(200, {})
        return response(404, {'status': 404, 'message': f'unexpected {method} {url}'})


class TestFileUploadFallback(BaseTestCase):
    def upload(self, url, initiate):
        engine = FakeEngine(initiate)
        with mock.patch('aito.v1.client.requestslib.request', engine):
            client = AitoClient(url, 'key', check_credentials=False)
            api.upload_binary_file(client, 't', gzipped(ROWS), batch_size=2)
        return engine

    def sent(self, engine):
        return [row for batch in engine.batches for row in batch]

    def test_a_self_hosted_engine_without_s3_gets_the_rows_in_batches(self):
        # today's engines: a bare 500, plain text
        bare_500 = response(500, 'There was an internal server error.', 'Internal Server Error')
        for url in ('http://localhost:9005', 'https://aito.customer.example', 'http://10.0.0.5:9005'):
            with self.subTest(url), self.assertLogs('AitoAPI', level='WARNING') as logs:
                engine = self.upload(url, bare_500)
                self.assertEqual(self.sent(engine), ROWS)
                warnings = [line for line in logs.output if 'WARNING' in line]
                self.assertEqual(len(warnings), 1, logs.output)
                self.assertIn('/batch', warnings[0])

    def test_the_engines_unavailable_code_falls_back_anywhere(self):
        # newer engines name it, so the host does not matter
        for url in ('http://localhost:9005', 'https://my-instance.aito.app'):
            with self.subTest(url), self.assertLogs('AitoAPI', level='WARNING'):
                engine = self.upload(url, response(501, UNAVAILABLE, 'Not Implemented'))
                self.assertEqual(self.sent(engine), ROWS)

    def test_a_500_on_aitos_cloud_still_raises(self):
        # the cloud has the storage: a 500 there is a failure, not a missing feature
        bare_500 = response(500, 'There was an internal server error.', 'Internal Server Error')
        for url in ('https://my-instance.aito.app', 'https://my-instance.api.aito.ai'):
            with self.subTest(url):
                with self.assertRaises(RequestError):
                    engine = self.upload(url, bare_500)
                    self.assertEqual(engine.batches, [])

    def test_another_refusal_of_the_initiate_call_still_raises(self):
        for status, body in ((401, {'status': 401, 'message': 'The API key you provided is not recognised'}),
                             (404, {'status': 404, 'message': 'no such table t'}),
                             (501, {'status': 501, 'error': 'something.else', 'message': 'x'})):
            with self.subTest(status=status), self.assertRaises(RequestError):
                self.upload('http://localhost:9005', response(status, body))

    def test_a_cloud_engine_with_storage_still_takes_the_file_path(self):
        initiated = response(200, {'id': '1f2e3d4c-0000-4000-8000-000000000000', 'url': 'https://s3.example/put',
                                   'method': 'PUT', 'expires': 'later'})
        with mock.patch.object(api, 'upload_binary_file_to_s3') as to_s3, \
                mock.patch.object(api, 'trigger_file_processing'), \
                mock.patch.object(api, 'poll_file_processing_status'):
            engine = self.upload('https://my-instance.aito.app', initiated)
        to_s3.assert_called_once()
        self.assertEqual(engine.batches, [])
