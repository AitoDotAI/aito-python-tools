"""Offline tests for :class:`~aito.client.v2.client.AitoClientV2`

No network: the session is replaced with a recorder that returns canned
responses, so these assert what the client *sends* and how it interprets what
comes back. The error bodies are verbatim captures from a live instance.
"""

import json
import logging

from aito.client.v2 import AitoClientV2, AitoV2Error, V2RowsResponse
from tests.cases import BaseTestCase


class FakeResponse:
    """The parts of a ``requests.Response`` the client reads"""

    def __init__(self, status_code=200, body=None, text=None):
        self.status_code = status_code
        self._body = body
        self.text = text if text is not None else json.dumps(body)

    def json(self):
        if self._body is None:
            raise ValueError('no json')
        return self._body


class RecordingSession:
    """Records the calls made and replays a queue of canned responses"""

    def __init__(self, responses=None):
        self.calls = []
        self.headers = {}
        self._responses = list(responses or [])

    def request(self, method, url, json=None, params=None, headers=None, timeout=None):
        self.calls.append({'method': method, 'url': url, 'json': json,
                           'params': params, 'headers': headers, 'timeout': timeout})
        if self._responses:
            return self._responses.pop(0)
        return FakeResponse(200, {'offset': 0, 'total': 0, 'hits': []})

    @property
    def last(self):
        return self.calls[-1]


def make_client(responses=None, **kwargs):
    """build a client with a recording session and no credential check"""
    kwargs.setdefault('check_credentials', False)
    client = AitoClientV2('https://shared.aito.ai/db/test-db', 'a-key', **kwargs)
    client._session = RecordingSession(responses)
    return client


class TestUrlAndEnv(BaseTestCase):
    def test_api_url_without_env(self):
        self.assertEqual(make_client().api_url, 'https://shared.aito.ai/db/test-db/api/v2')

    def test_api_url_with_env_uses_two_path_segments(self):
        # The single dotted segment (/db/<db>/env.<name>) does not match the
        # route at all; only /env/<name> does.
        client = make_client(env='v2-demo')
        self.assertEqual(client.api_url, 'https://shared.aito.ai/db/test-db/env/v2-demo/api/v2')

    def test_trailing_slash_on_the_instance_url_is_stripped(self):
        client = AitoClientV2('https://shared.aito.ai/db/test-db/', 'k', check_credentials=False)
        self.assertEqual(client.api_url, 'https://shared.aito.ai/db/test-db/api/v2')

    def test_reserved_env_prefix_is_rejected_locally(self):
        # `env.` became reserved after environments were created under it, so a
        # stored `env.v2-demo` now 400s at the server. Fail here, naming the fix.
        for name in ('env.v2-demo', '_internal', 'release.1'):
            with self.assertRaises(ValueError) as ctx:
                AitoClientV2('https://x/db/y', 'k', env=name, check_credentials=False)
            self.assertIn('may not start with', str(ctx.exception))

    def test_reserved_env_prefix_message_suggests_the_stripped_name(self):
        with self.assertRaises(ValueError) as ctx:
            AitoClientV2('https://x/db/y', 'k', env='env.v2-demo', check_credentials=False)
        self.assertIn("'v2-demo'", str(ctx.exception))

    def test_branch_env_validates_too(self):
        with self.assertRaises(ValueError):
            make_client().branch_env('env.nope')

    def test_invalid_on_warning_is_rejected(self):
        with self.assertRaises(ValueError):
            AitoClientV2('https://x/db/y', 'k', on_warning='explode', check_credentials=False)


class TestRequestBodies(BaseTestCase):
    """What the client actually puts on the wire"""

    def test_predict_addresses_the_named_endpoint(self):
        client = make_client()
        client.predict(from_table='invoices', where={'vendor': 'Elenia Oy'}, predict='gl_code')
        call = client._session.last
        self.assertEqual(call['method'], 'POST')
        self.assertTrue(call['url'].endswith('/api/v2/_predict'))
        self.assertEqual(call['json'], {
            'from': 'invoices', 'predict': 'gl_code',
            'where': {'vendor': 'Elenia Oy'}, 'select': ['$p', '$value'],
        })

    def test_predict_why_adds_the_explanation_to_the_default_select(self):
        client = make_client()
        client.predict(from_table='t', predict='c', why=True)
        self.assertEqual(client._session.last['json']['select'], ['$p', '$value', '$why'])

    def test_absent_clauses_are_omitted_not_sent_as_null(self):
        # v2 validates the grammar strictly; "where": null is not "no where".
        client = make_client()
        client.search(from_table='invoices')
        self.assertEqual(client._session.last['json'], {'from': 'invoices'})

    def test_relate_wraps_a_single_field_name_in_a_list(self):
        # v1 took a bare string here; v2 rejects it.
        client = make_client()
        client.relate(from_table='invoices', relate='vendor', where={'gl_code': '6110'})
        body = client._session.last['json']
        self.assertEqual(body['relate'], ['vendor'])
        self.assertEqual(body['select'], ['related', 'condition', 'lift', 'fs'])
        self.assertEqual(body['orderBy'], 'lift')

    def test_relate_passes_a_list_through(self):
        client = make_client()
        client.relate(from_table='t', relate=['a', 'b'])
        self.assertEqual(client._session.last['json']['relate'], ['a', 'b'])

    def test_relate_passes_a_patterns_spec_through(self):
        client = make_client()
        spec = {'$patterns': {'$related': {'relate': ['vendor'], 'k': 8}}}
        client.relate(from_table='t', relate=spec)
        self.assertEqual(client._session.last['json']['relate'], spec)

    def test_recommend_body(self):
        client = make_client()
        client.recommend(from_table='impressions', recommend='product',
                         goal={'purchase': True}, limit=8)
        self.assertEqual(client._session.last['json'], {
            'from': 'impressions', 'recommend': 'product',
            'goal': {'purchase': True}, 'limit': 8,
        })

    def test_query_uses_the_universal_endpoint(self):
        client = make_client()
        client.query({'from': 't', 'limit': 1})
        self.assertTrue(client._session.last['url'].endswith('/api/v2/_query'))

    def test_search_limit_zero_is_sent(self):
        # limit=0 reads `total` without the hits; it must survive the
        # "omit what wasn't given" filter.
        client = make_client()
        client.search(from_table='t', limit=0)
        self.assertEqual(client._session.last['json'], {'from': 't', 'limit': 0})

    def test_meta_flag_adds_the_query_parameter(self):
        client = make_client(meta=True)
        client.query({'from': 't'})
        self.assertEqual(client._session.last['params'], {'meta': 'true'})

    def test_meta_is_off_by_default(self):
        client = make_client()
        client.query({'from': 't'})
        self.assertIsNone(client._session.last['params'])

    def test_evaluate_uses_a_long_default_timeout(self):
        client = make_client([FakeResponse(200, {'kind': 'evaluation', 'data': {'accuracy': 1.0}})])
        client.evaluate({'test': {}, 'evaluate': {}})
        self.assertEqual(client._session.last['timeout'], 600.0)

    def test_upload_entries_batches_and_counts(self):
        rows = [{'i': i} for i in range(5)]
        client = make_client([
            FakeResponse(200, {'status': 'inserted', 'count': 2}),
            FakeResponse(200, {'status': 'inserted', 'count': 2}),
            FakeResponse(200, {'status': 'inserted', 'count': 1}),
        ])
        self.assertEqual(client.upload_entries('t', rows, batch_size=2), 5)
        self.assertEqual(len(client._session.calls), 3)
        self.assertEqual(client._session.calls[0]['json'], [{'i': 0}, {'i': 1}])
        self.assertTrue(client._session.calls[0]['url'].endswith('/api/v2/data/t/batch'))

    def test_delete_entries_builds_the_body(self):
        client = make_client([FakeResponse(200, {'total': 2})])
        client.delete_entries('invoices', {'gl_code': '6110'})
        call = client._session.last
        self.assertTrue(call['url'].endswith('/api/v2/data/_delete'))
        self.assertEqual(call['json'], {'from': 'invoices', 'where': {'gl_code': '6110'}})

    def test_delete_entries_refuses_an_empty_where_without_a_round_trip(self):
        # An empty filter matches every row. The server refuses it too, but
        # failing here costs nothing and says what to do instead.
        client = make_client()
        for empty in ({}, None):
            with self.assertRaises(ValueError) as ctx:
                client.delete_entries('invoices', empty)
            self.assertIn('non-empty `where`', str(ctx.exception))
        self.assertEqual(client._session.calls, [])

    def test_modify_wraps_a_list_of_operations(self):
        client = make_client([FakeResponse(200, {})])
        client.modify([{'optimize': 'a'}, {'optimize': 'b'}])
        self.assertEqual(client._session.last['json'],
                         {'operations': [{'optimize': 'a'}, {'optimize': 'b'}]})

    def test_modify_passes_a_single_operation_through(self):
        client = make_client([FakeResponse(200, {})])
        client.modify({'optimize': 'a'})
        self.assertEqual(client._session.last['json'], {'optimize': 'a'})

    def test_create_collection_sends_the_collection_type(self):
        client = make_client([FakeResponse(200, {'status': 'created'})])
        client.create_collection('invoices', {'vendor': {'type': 'String'}})
        call = client._session.last
        self.assertEqual(call['method'], 'PUT')
        self.assertTrue(call['url'].endswith('/api/v2/schema/invoices'))
        self.assertEqual(call['json'], {'type': 'collection',
                                        'columns': {'vendor': {'type': 'String'}}})


class TestErrors(BaseTestCase):
    """Verbatim v2 error bodies, and the code the client pulls out of them"""

    NOT_FOUND = {'kind': 'error', 'data': {'code': 'not_found',
                                           'message': 'no_such_table not found'}}
    MODE_MISMATCH = {'kind': 'error', 'data': {
        'code': 'request.invalid',
        'message': "the _predict endpoint expects a 'predict' query, but got a "
                   'rows/search body. Post to _query for a general query.'}}

    def test_not_found_exposes_a_machine_code(self):
        client = make_client([FakeResponse(404, self.NOT_FOUND)])
        with self.assertRaises(AitoV2Error) as ctx:
            client.query({'from': 'no_such_table'})
        err = ctx.exception
        # This is what replaces matching "failed to open '<table>'" in the body.
        self.assertEqual(err.code, 'not_found')
        self.assertEqual(err.status_code, 404)
        self.assertTrue(err.is_not_found)
        self.assertIn('no_such_table not found', str(err))

    def test_request_invalid_carries_the_engine_message(self):
        client = make_client([FakeResponse(400, self.MODE_MISMATCH)])
        with self.assertRaises(AitoV2Error) as ctx:
            client.query({'from': 't'})
        self.assertEqual(ctx.exception.code, 'request.invalid')
        self.assertFalse(ctx.exception.is_not_found)
        self.assertIn('Post to _query', str(ctx.exception))

    def test_a_non_structured_error_still_raises_with_the_body(self):
        # Framework-level failures (auth, no route) are not the v2 error kind.
        client = make_client([FakeResponse(403, {'message': 'not authorized'})])
        with self.assertRaises(AitoV2Error) as ctx:
            client.query({'from': 't'})
        self.assertIsNone(ctx.exception.code)
        self.assertEqual(ctx.exception.status_code, 403)
        self.assertIn('not authorized', str(ctx.exception))

    def test_a_non_json_error_body_still_raises(self):
        client = make_client([FakeResponse(502, None, text='<html>bad gateway</html>')])
        with self.assertRaises(AitoV2Error) as ctx:
            client.query({'from': 't'})
        self.assertEqual(ctx.exception.status_code, 502)
        self.assertIn('bad gateway', str(ctx.exception))

    def test_a_non_json_success_body_raises_rather_than_returning_none(self):
        client = make_client([FakeResponse(200, None, text='not json')])
        with self.assertRaises(AitoV2Error):
            client.query({'from': 't'})


class TestWarningPolicy(BaseTestCase):
    WARNED = {'offset': 0, 'total': 0, 'hits': [],
              'warnings': [{'code': 'where.unknown_field', 'severity': 'warning',
                            'field': 'nope', 'message': 'possible typo'}]}

    def test_warnings_are_logged_by_default(self):
        client = make_client([FakeResponse(200, self.WARNED)])
        with self.assertLogs('AitoClientV2', level=logging.WARNING) as logs:
            resp = client.query({'from': 't', 'where': {'nope': 1}})
        self.assertIsInstance(resp, V2RowsResponse)
        self.assertEqual(len(resp.warnings), 1)
        self.assertIn('where.unknown_field', '\n'.join(logs.output))

    def test_on_warning_raise_turns_a_broadened_query_into_a_failure(self):
        # The reason this option exists: a silently-dropped tenant filter comes
        # back as a 200 with more rows than asked for.
        client = make_client([FakeResponse(200, self.WARNED)], on_warning='raise')
        with self.assertRaises(AitoV2Error) as ctx:
            client.query({'from': 't'})
        self.assertIn('where.unknown_field', str(ctx.exception))

    def test_on_warning_ignore_is_silent(self):
        client = make_client([FakeResponse(200, self.WARNED)], on_warning='ignore')
        resp = client.query({'from': 't'})
        # Still readable off the response — ignoring means "don't log or raise".
        self.assertEqual(len(resp.warnings), 1)

    def test_a_clean_response_logs_nothing(self):
        client = make_client([FakeResponse(200, {'offset': 0, 'total': 0, 'hits': []})])
        logger = logging.getLogger('AitoClientV2')
        with self.assertRaises(AssertionError):
            with self.assertLogs(logger, level=logging.WARNING):
                client.query({'from': 't'})


class TestHeadersAndRepr(BaseTestCase):
    def test_headers(self):
        client = make_client()
        self.assertEqual(client.headers,
                         {'Content-Type': 'application/json', 'x-api-key': 'a-key'})

    def test_every_request_carries_the_key(self):
        client = make_client()
        client.query({'from': 't'})
        self.assertEqual(client._session.last['headers']['x-api-key'], 'a-key')

    def test_reassigning_the_api_key_takes_effect(self):
        # The v1 client documents swapping between the read-only and read-write
        # keys this way. Pinning the header onto the session at construction
        # would leave `client.headers` reporting the new key while the wire
        # still carried the old one — divergence that is worse than not
        # supporting the pattern at all.
        client = make_client()
        client.api_key = 'a-second-key'
        client.query({'from': 't'})
        self.assertEqual(client._session.last['headers']['x-api-key'], 'a-second-key')

    def test_repr_does_not_leak_the_key(self):
        self.assertNotIn('a-key', repr(make_client()))

    def test_repr_names_the_env(self):
        self.assertIn("env='v2-demo'", repr(make_client(env='v2-demo')))
