"""A refused query and a refused key reach the user with the server's reason

The engine now refuses, instead of answering 200 with the filter silently dropped:
a condition it cannot apply under ``$not``/``$or``/``$and`` (400 on v1, 501 on v2), an
operator in a v1 ``_estimate`` ``where`` (400), and an empty API key (401). Each body
names the cause. The client must carry that message into the exception text, not a
status line ("Bad Request") or a catch-all, and a key refused at startup must say how
to set the key. The bodies below are the engine's own (aito-core books
V1EstimateContractTest, RefsRefusalReachesClientTest; ApiSupport's key messages).
Offline: no instance is called.
"""

import asyncio
import io
import json
import os
from contextlib import redirect_stderr, redirect_stdout
from unittest import mock

import requests

from aito.local.profiles import NoCredentialsError
from aito.v1.client import AitoClient, Error, RequestError
from aito.v2 import AitoClientV2, AitoV2Error
from tests.cases import BaseTestCase

URL = 'https://shared.aito.ai/db/test-db'

ESTIMATE_REFUSED = (
    "1:64: _estimate: the condition on 'cat' is not supported; the estimate takes plain field "
    "values in 'where' (no $not, $or, $gt, ...)")
NOT_REFUSED = ("where: $match is not available on field 'product' here — it is not a Text "
               "column, so $match does not apply")
NOT_RECOGNISED = 'The API key you provided is not recognised'
KEY_MISSING = "The required header 'x-api-key' is missing"

V1_ESTIMATE_400 = {'status': 400, 'message': ESTIMATE_REFUSED}
V1_NOT_400 = {'status': 400, 'message': NOT_REFUSED}
V1_401 = {'status': 401, 'message': NOT_RECOGNISED}
V2_NOT_501 = {'kind': 'error', 'data': {'code': 'proposition.unsupported', 'message': NOT_REFUSED}}
V2_401 = {'kind': 'error', 'data': {'code': 'unauthorized', 'message': NOT_RECOGNISED}}
REASONS = {400: 'Bad Request', 401: 'Unauthorized', 501: 'Not Implemented'}


def response(status, body):
    r = requests.Response()
    r.status_code = status
    r._content = json.dumps(body).encode()
    r.url = URL
    r.reason = REASONS.get(status, 'OK')
    return r


class FakeSession:
    """a v2 session answering every request with one response"""

    def __init__(self, status, body):
        self.headers, self.calls = {}, []
        self.status, self.body = status, body

    def request(self, method, url, **kwargs):
        self.calls.append(url)
        return response(self.status, self.body)


def v1_server(status, body):
    """patch v1's HTTP: /version answers, everything else answers `status` with `body`"""
    def fake(**kw):
        if kw['url'].endswith('/version'):
            return response(200, {'version': '2.11.2'})
        return response(status, body)
    return mock.patch('aito.v1.client.requestslib.request', fake)


class TestV1(BaseTestCase):
    def test_a_refused_estimate_names_the_clause(self):
        with v1_server(400, V1_ESTIMATE_400):
            client = AitoClient(URL, 'key', check_credentials=False)
            with self.assertRaises(RequestError) as ctx:
                client.request(method='POST', endpoint='/api/v1/_estimate',
                               query={'from': 'items', 'where': {'cat': {'$not': 'A'}}})
        self.assertIn(ESTIMATE_REFUSED, str(ctx.exception))

    def test_a_refused_condition_under_not_names_the_field(self):
        with v1_server(400, V1_NOT_400):
            client = AitoClient(URL, 'key', check_credentials=False)
            with self.assertRaises(RequestError) as ctx:
                client.request(method='POST', endpoint='/api/v1/_search',
                               query={'from': 't', 'where': {'$not': {'product': {'$match': 'p1'}}}})
        self.assertIn(NOT_REFUSED, str(ctx.exception))

    def test_an_empty_key_is_refused_before_any_request(self):
        for key in ('', '   '):
            with self.subTest(repr(key)), v1_server(401, V1_401):
                with self.assertRaises(Error) as ctx:
                    AitoClient(URL, key)
                self.assertIn('the API key is empty', str(ctx.exception))
                self.assertIn('AITO_API_KEY', str(ctx.exception))

    def test_a_refused_key_at_startup_says_so_and_how_to_set_it(self):
        with v1_server(401, V1_401):
            with self.assertRaises(Error) as ctx:
                AitoClient(URL, 'a-wrong-key')
        message = str(ctx.exception)
        self.assertIn('the API key was refused', message)
        self.assertIn(NOT_RECOGNISED, message)
        self.assertIn('set AITO_API_KEY', message)

    def test_an_unreachable_instance_is_not_called_a_key_problem_without_its_cause(self):
        def down(**kw):
            raise requests.ConnectionError('connection refused')
        with mock.patch('aito.v1.client.requestslib.request', down):
            with self.assertRaises(Error) as ctx:
                AitoClient(URL, 'key')
        self.assertIn('connection refused', str(ctx.exception))


class FakeAiohttpResponse:
    """the parts of aiohttp's ClientResponse the async path reads, as aiohttp has them:
    ``raise_for_status`` would carry only the reason phrase, the body is in ``text()``"""

    def __init__(self, status, body):
        self.status, self._body = status, body
        self.reason = REASONS.get(status, 'OK')
        self.headers, self.history = {}, ()
        self.request_info = mock.Mock(real_url=URL)

    async def json(self, content_type='application/json'):
        return self._body

    async def text(self):
        return json.dumps(self._body)

    def raise_for_status(self):
        if self.status >= 400:
            from aiohttp import ClientResponseError
            raise ClientResponseError(self.request_info, self.history, status=self.status,
                                      message=self.reason, headers=self.headers)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class FakeAiohttpSession:
    def __init__(self, response):
        self.response = response

    def request(self, **kw):
        return self.response


class TestV1Async(BaseTestCase):
    def test_a_refused_estimate_names_the_clause_not_the_status_line(self):
        client = AitoClient(URL, 'key', check_credentials=False)
        session = FakeAiohttpSession(FakeAiohttpResponse(400, V1_ESTIMATE_400))
        with self.assertRaises(RequestError) as ctx:
            asyncio.run(client.async_request(session, method='POST', endpoint='/api/v1/_estimate',
                                             query={'from': 'items', 'where': {'cat': {'$not': 'A'}}}))
        self.assertIn(ESTIMATE_REFUSED, str(ctx.exception))
        self.assertEqual(ctx.exception.error.status, 400)

    def test_a_body_without_a_message_is_shown_as_it_is(self):
        client = AitoClient(URL, 'key', check_credentials=False)
        session = FakeAiohttpSession(FakeAiohttpResponse(400, {'error': 'something else'}))
        with self.assertRaises(RequestError) as ctx:
            asyncio.run(client.async_request(session, method='POST', endpoint='/api/v1/_search', query={}))
        self.assertIn('something else', str(ctx.exception))


class TestV2(BaseTestCase):
    def client(self, status, body, key='key', **kwargs):
        session = FakeSession(status, body)
        with mock.patch('aito.v2.client.requestslib.Session', lambda: session):
            return AitoClientV2(URL, key, **kwargs), session

    def test_a_refused_condition_under_not_names_the_field_and_code(self):
        client, _ = self.client(501, V2_NOT_501, check_credentials=False)
        with self.assertRaises(AitoV2Error) as ctx:
            client.search(from_table='t', where={'$not': {'product': {'$match': 'p1'}}})
        self.assertIn(NOT_REFUSED, str(ctx.exception))
        self.assertEqual(ctx.exception.code, 'proposition.unsupported')

    def test_an_empty_key_is_refused_before_any_request(self):
        for key in ('', '   '):
            with self.subTest(repr(key)):
                session = FakeSession(401, V2_401)
                with mock.patch('aito.v2.client.requestslib.Session', lambda: session):
                    with self.assertRaises(NoCredentialsError) as ctx:
                        AitoClientV2(URL, key)
                self.assertIn('the API key is empty', str(ctx.exception))
                self.assertIn('AITO_API_KEY', str(ctx.exception))
                self.assertEqual(session.calls, [])

    def test_a_refused_key_at_startup_says_so_and_how_to_set_it(self):
        for body in (V2_401, {'status': 401, 'message': KEY_MISSING}):  # v2 and v1-shaped 401s
            with self.subTest(body):
                with self.assertRaises(AitoV2Error) as ctx:
                    self.client(401, body)
                message = str(ctx.exception)
                self.assertIn(body.get('message') or body['data']['message'], message)
                self.assertIn('set AITO_API_KEY', message)
                self.assertEqual(ctx.exception.status_code, 401)

    def test_other_startup_errors_are_not_called_a_key_problem(self):
        with self.assertRaises(AitoV2Error) as ctx:
            self.client(503, {'kind': 'error', 'data': {'code': 'unavailable', 'message': 'starting up'}})
        self.assertNotIn('Check the key', str(ctx.exception))


class TestCli(BaseTestCase):
    """the CLI prints the reason: v2 errors are not logged when raised, so it must"""

    def run_cli(self, argv, status, body):
        from aito.cli.main_parser import MainParser
        session = FakeSession(status, body)
        err, out = io.StringIO(), io.StringIO()
        env = {'AITO_INSTANCE_URL': URL, 'AITO_API_KEY': 'key', 'AITO_API_VERSION': 'v2'}
        with mock.patch.dict(os.environ, env), \
                mock.patch('aito.v2.client.requestslib.Session', lambda: session), \
                mock.patch.object(MainParser, 'config_logging'), \
                redirect_stderr(err), redirect_stdout(out), self.assertRaises(SystemExit) as ctx:
            parser = MainParser()
            parser.parse_and_execute(vars(parser.parse_args(argv)))
        self.assertEqual(ctx.exception.code, 2)
        return err.getvalue()

    def test_a_refused_query_prints_the_reason(self):
        query = json.dumps({'from': 't', 'where': {'$not': {'product': {'$match': 'p1'}}}})
        self.assertIn(NOT_REFUSED, self.run_cli(['search', query], 501, V2_NOT_501))

    def test_a_refused_key_prints_how_to_set_it(self):
        stderr = self.run_cli(['search', json.dumps({'from': 't'})], 401, V2_401)
        self.assertIn(NOT_RECOGNISED, stderr)
        self.assertIn('set AITO_API_KEY', stderr)
