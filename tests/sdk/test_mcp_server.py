"""The MCP server's tools, offline (``aito.mcp.server``)

Each tool is called through the MCP server itself (``call_tool``) on a client with
a recording session: what each tool sends, what it returns, and what its
description tells an agent about when to use it and when not.
"""

import asyncio
import json
import os
import re
import unittest
from pathlib import Path
from unittest import mock

from tests.cases import BaseTestCase
from tests.sdk.test_v2_client import FakeResponse, make_client

try:
    from aito.mcp import server as mcp_server
except ImportError:  # the extra is not installed (or Python < 3.10)
    mcp_server = None

WHY = {'type': 'product', 'factors': [
    {'type': 'baseP', 'value': 0.25},
    {'type': 'product', 'factors': [
        {'type': 'relatedPropositionLift', 'proposition': {'vendor': 'Acme Oy'}, 'value': 1.5},
        {'type': 'relatedPropositionLift', 'proposition': {'description': 'toner'}, 'value': 4.0},
        {'type': 'relatedPropositionLift', 'proposition': {'amount': 120}, 'value': 0.5},
    ]},
]}
PREDICTION = {'offset': 0, 'total': 2, 'hits': [{'$value': '4010', '$p': 0.81, '$why': WHY},
                                                {'$value': '4000', '$p': 0.12}]}
ROWS = {'offset': 0, 'total': 0, 'hits': []}
SEPARATION_START = 'Data separation between your customers: use a separate instance or collection per customer.'
INFERENCE = ['predict', 'recommend', 'relate', 'match', 'search', 'query']
WRITES = ['put_schema', 'upload_rows']


def run(server, tool, arguments):
    """call a tool through the server; the parsed JSON it returned"""
    result = asyncio.run(server.call_tool(tool, arguments))
    if isinstance(result, tuple):   # mcp 1.x: (content, structured)
        result = result[0]
    content = getattr(result, 'content', result)
    return json.loads(content[0].text)


def descriptions(server):
    return {t.name: t.description for t in asyncio.run(server.list_tools())}


@unittest.skipIf(mcp_server is None, 'the mcp extra is not installed')
class TestInferenceTools(BaseTestCase):
    def serve(self, *responses, **kwargs):
        self.client = make_client(list(responses))
        return mcp_server.build_server(self.client, **kwargs)

    def sent(self):
        call = self.client._session.last
        return call['method'], call['url'].split('/api/v2')[1], call['json']

    def test_predict_adds_value_p_why_and_flattens_why(self):
        server = self.serve(FakeResponse(200, PREDICTION))
        out = run(server, 'predict', {'query': {'from': 'invoices', 'predict': 'gl_account'}})
        self.assertEqual(self.sent(), ('POST', '/_predict', {
            'from': 'invoices', 'predict': 'gl_account', 'select': ['$value', '$p', '$why']}))
        self.assertEqual(out['hits'][0]['$p'], 0.81)
        # base rate, then the evidence strongest effect first (|log lift|): x4, x0.5, x1.5
        self.assertEqual(out['hits'][0]['$why'], {'base_p': 0.25, 'factors': [
            {'proposition': {'description': 'toner'}, 'lift': 4.0},
            {'proposition': {'amount': 120}, 'lift': 0.5},
            {'proposition': {'vendor': 'Acme Oy'}, 'lift': 1.5}]})
        self.assertNotIn('$why', out['hits'][1])

    def test_predict_keeps_the_callers_select(self):
        server = self.serve(FakeResponse(200, ROWS))
        run(server, 'predict', {'query': {'from': 'invoices', 'predict': 'gl', 'select': ['$value']}})
        self.assertEqual(self.sent()[2]['select'], ['$value'])

    def test_recommend_and_match_add_value_p_why(self):
        for tool, path, body in (
                ('recommend', '/_recommend', {'from': 'impressions', 'recommend': 'product',
                                              'goal': {'purchased': True}}),
                ('match', '/_match', {'from': 'lines', 'where': {'text': 'toner'}, 'match': 'product'})):
            with self.subTest(tool):
                server = self.serve(FakeResponse(200, ROWS))
                run(server, tool, {'query': body})
                self.assertEqual(self.sent(), ('POST', path, {**body, 'select': ['$value', '$p', '$why']}))

    def test_relate_search_query_send_the_body_as_given(self):
        for tool, path in (('relate', '/_relate'), ('search', '/_search'), ('query', '/_query')):
            with self.subTest(tool):
                server = self.serve(FakeResponse(200, ROWS))
                body = {'from': 't', 'where': {'a': 1}}
                run(server, tool, {'query': body})
                self.assertEqual(self.sent(), ('POST', path, body))

    def test_evaluate_posts_with_a_long_timeout_and_returns_the_metrics(self):
        server = self.serve(FakeResponse(200, {'kind': 'evaluation', 'data': {'accuracy': 0.9}}))
        body = {'test': {'$index': {'$mod': [4, 0]}}, 'evaluate': {'from': 't', 'predict': 'x'}}
        self.assertEqual(run(server, 'evaluate', {'query': body}), {'accuracy': 0.9})
        self.assertEqual(self.sent(), ('POST', '/_evaluate', body))
        self.assertEqual(self.client._session.last['timeout'], 600.0)

    def test_get_schema_whole_or_one_table(self):
        server = self.serve(FakeResponse(200, {'schema': {}}), FakeResponse(200, {'type': 'collection'}))
        run(server, 'get_schema', {})
        self.assertEqual(self.sent()[:2], ('GET', '/schema'))
        run(server, 'get_schema', {'table': 'invoices'})
        self.assertEqual(self.sent()[:2], ('GET', '/schema/invoices'))

    def test_an_engine_error_reaches_the_agent_with_its_message(self):
        error = {'kind': 'error', 'data': {'code': 'query.invalid', 'message': "unknown field 'gl'"}}
        server = self.serve(FakeResponse(400, error))
        with self.assertRaises(mcp_server.ToolError) as ctx:
            run(server, 'predict', {'query': {'from': 'invoices', 'predict': 'gl'}})
        self.assertIn("unknown field 'gl'", str(ctx.exception))


@unittest.skipIf(mcp_server is None, 'the mcp extra is not installed')
class TestWrites(BaseTestCase):
    SCHEMA = {'type': 'collection', 'columns': {'id': {'type': 'String'}}}

    def test_writes_are_refused_by_default_and_send_nothing(self):
        client = make_client()
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop(mcp_server.WRITES_ENV, None)
            server = mcp_server.build_server(client)
        for tool, args in (('put_schema', {'table': 't', 'schema': self.SCHEMA}),
                           ('upload_rows', {'table': 't', 'rows': [{'id': 'a'}]})):
            with self.subTest(tool):
                with self.assertRaises(mcp_server.ToolError) as ctx:
                    run(server, tool, args)
                self.assertIn('AITO_MCP_ALLOW_WRITES=1', str(ctx.exception))
        self.assertEqual(client._session.calls, [])

    def test_the_env_var_enables_writes(self):
        client = make_client([FakeResponse(200, {'type': 'collection'}), FakeResponse(200, {})])
        with mock.patch.dict(os.environ, {mcp_server.WRITES_ENV: '1'}):
            server = mcp_server.build_server(client)
        run(server, 'put_schema', {'table': 't', 'schema': self.SCHEMA})
        out = run(server, 'upload_rows', {'table': 't', 'rows': [{'id': 'a'}, {'id': 'b'}]})
        self.assertEqual(out, {'table': 't', 'uploaded': 2})
        sent = [(c['method'], c['url'].split('/api/v2')[1], c['json']) for c in client._session.calls]
        self.assertEqual(sent, [('PUT', '/schema/t', self.SCHEMA),
                                ('POST', '/data/t/batch', [{'id': 'a'}, {'id': 'b'}])])

    def test_annotations_mark_reads_and_writes(self):
        server = mcp_server.build_server(make_client(), allow_writes=False)
        hints = {t.name: t.annotations for t in asyncio.run(server.list_tools())}
        for name, hint in hints.items():
            with self.subTest(name):
                self.assertEqual(hint.readOnlyHint if hasattr(hint, 'readOnlyHint') else hint.read_only_hint,
                                 name not in WRITES)


class TestEntryPointWithoutTheExtra(BaseTestCase):
    def test_aito_mcp_names_the_extra_when_mcp_is_missing(self):
        import aito.mcp
        missing = ModuleNotFoundError("No module named 'mcp'", name='mcp')
        with mock.patch.dict('sys.modules', {'aito.mcp.server': None}), \
                mock.patch('builtins.__import__', side_effect=missing):
            with self.assertRaises(SystemExit) as ctx:
                aito.mcp.main()
        self.assertIn("pip install 'aitoai[mcp]'", str(ctx.exception))


@unittest.skipIf(mcp_server is None, 'the mcp extra is not installed')
class TestMain(BaseTestCase):
    def test_no_instance_configured_exits_with_the_sdks_hint(self):
        from aito.local.profiles import NoCredentialsError
        with mock.patch('aito.v2.client.resolve_credentials',
                        side_effect=NoCredentialsError('run `aito start` to start a local instance')):
            with self.assertRaises(SystemExit) as ctx:
                mcp_server.main()
        self.assertIn('aito start', str(ctx.exception))


@unittest.skipIf(mcp_server is None, 'the mcp extra is not installed')
class TestWhenToUse(BaseTestCase):
    """the descriptions are where an agent learns when Aito fits and when not"""

    def setUp(self):
        super().setUp()
        self.server = mcp_server.build_server(make_client(), allow_writes=False)
        self.desc = descriptions(self.server)

    def test_every_tool_is_there(self):
        self.assertEqual(sorted(self.desc), sorted(INFERENCE + ['evaluate', 'get_schema'] + WRITES))

    def test_inference_tools_say_when_and_when_not(self):
        for tool in ('predict', 'recommend', 'relate', 'match', 'search'):
            with self.subTest(tool):
                self.assertIn("Don't use when", self.desc[tool])
        for tool in ('predict', 'recommend', 'match'):
            with self.subTest(tool):
                self.assertIn('Use when', self.desc[tool])

    def test_the_separation_rule_is_on_every_tool_that_touches_customer_data(self):
        for tool in INFERENCE + WRITES:
            with self.subTest(tool):
                self.assertIn(SEPARATION_START, self.desc[tool])
                self.assertIn('nested `from`', self.desc[tool])

    def test_recommend_warns_about_link_targets(self):
        self.assertIn('LINK target', self.desc['recommend'])
        self.assertIn('prefer predict', self.desc['recommend'])

    def test_evaluate_says_how_to_choose_a_threshold(self):
        self.assertIn('"select": ["accuracy", "cases"]', self.desc['evaluate'])
        self.assertIn('threshold', self.desc['evaluate'])

    def test_match_names_its_current_non_fit(self):
        self.assertIn('no shared history to a catalogue', self.desc['match'])

    def test_the_instructions_carry_the_fit_and_non_fit_lists_and_the_checklist(self):
        text = mcp_server.INSTRUCTIONS
        for n in range(1, 7):
            self.assertIn(f'\n{n}. ', text.split("Don't use Aito")[0])
            self.assertIn(f'\n{n}. ', text.split("Don't use Aito")[1])
        self.assertIn('Is there history of this decision in my data? No -> not Aito.', text)
        self.assertIn(mcp_server.SEPARATION, text)

    def test_no_calibration_claim_without_a_linked_benchmark(self):
        for name, text in {**self.desc, 'instructions': mcp_server.INSTRUCTIONS}.items():
            with self.subTest(name):
                self.assertNotIn('calibrated', text.lower())


SKILL = Path(__file__).resolve().parents[2] / 'claude-plugin' / 'skills' / 'add-predictive-feature' / 'SKILL.md'


@unittest.skipIf(mcp_server is None, 'the mcp extra is not installed')
class TestSkillMatchesTheServer(BaseTestCase):
    """the plugin's skill and the server quote the same page; keep them word for word"""

    def setUp(self):
        super().setUp()
        # compare words, not layout: the skill wraps its lines and bolds its headlines
        self.skill = ' '.join(SKILL.read_text().replace('**', '').replace('`', '').split())

    def test_fit_and_non_fit_lines_are_the_same(self):
        for line in (mcp_server.FIT + '\n' + mcp_server.NON_FIT).splitlines():
            numbered = re.match(r'\d\. (.*?\.)( |$)', line)
            if numbered:
                with self.subTest(line):
                    self.assertIn(numbered.group(1).replace('`', ''), self.skill)

    def test_checklist_and_separation_are_the_same(self):
        self.assertIn(' '.join(mcp_server.SEPARATION.replace('`', '').split()), self.skill)
        for line in mcp_server.CHECKLIST.splitlines()[1:]:
            question = line.split('?')[0].lstrip('- ')
            with self.subTest(question):
                self.assertIn(question, self.skill)

    def test_no_calibration_claim(self):
        self.assertNotIn('calibrated', self.skill.lower())

    def test_both_link_the_when_to_use_page(self):
        self.assertIn(mcp_server.WHEN_TO_USE_URL, mcp_server.INSTRUCTIONS)
        self.assertIn(mcp_server.WHEN_TO_USE_URL, self.skill)
