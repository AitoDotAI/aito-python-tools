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
        out = run(server, 'evaluate', {'query': body})
        self.assertEqual((out['summary']['accuracy'], out['metrics']), (0.9, {'accuracy': 0.9}))
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


# --- response shaping, from dogfooding on internal.aito.ai (10.10) ---------------------------------

RELATE_RESPONSE = {'offset': 0, 'total': 2, 'hits': [
    {'related': {'channel': 'meeting'}, 'condition': {'good_outcome': True}, 'lift': 1.18, 'n': 39.0,
     'info': 0.014, 'relation': {'n': 39.0, 'mi': 0.06},
     'fs': {'f': 10.0, 'fOnCondition': 8.0, 'fCondition': 22.0, 'n': 39.0, 'fOnNotCondition': 2.0},
     'ps': {'p': 0.256, 'pOnCondition': 0.364, 'pOnNotCondition': 0.118, 'pCondition': 0.564}},
    {'related': {'channel': 'linkedin'}, 'condition': {'good_outcome': True}, 'lift': 0.62, 'n': 39.0,
     'info': 0.022, 'relation': {'n': 39.0, 'mi': 0.09},
     'fs': {'f': 6.0, 'fOnCondition': 1.0, 'fCondition': 22.0, 'n': 39.0, 'fOnNotCondition': 5.0},
     'ps': {'p': 0.154, 'pOnCondition': 0.045, 'pOnNotCondition': 0.294, 'pCondition': 0.564}},
]}

EVALUATION = {'kind': 'evaluation', 'data': {
    'accuracy': 0.8928571428571429, 'baseAccuracy': 0.7767857142857143, 'accuracyGain': 0.1160714285714286,
    'ece': 0.05470956672436107, 'testSamples': 224, 'trainSamples': 892, 'meanMs': 106.96899712946428,
    'mxe': 0.597, 'meanNs': 106968997.1, 'logLoss': 0.41}}

NEUTRAL_WHY = {'type': 'product', 'factors': [
    {'type': 'baseP', 'value': 0.79},
    {'type': 'relatedPropositionLift', 'proposition': {'title': 'CI'}, 'value': 1.23},
    {'type': 'relatedPropositionLift', 'proposition': {'title': 'SDK'}, 'value': 1.0},
]}


@unittest.skipIf(mcp_server is None, 'the mcp extra is not installed')
class TestShaping(BaseTestCase):
    def serve(self, *responses):
        self.client = make_client(list(responses))
        return mcp_server.build_server(self.client, allow_writes=False)

    def test_factors_that_change_nothing_are_dropped_from_why(self):
        server = self.serve(FakeResponse(200, {'hits': [{'$value': 'rnd', '$p': 0.98, '$why': NEUTRAL_WHY}]}))
        out = run(server, 'predict', {'query': {'from': 'todos', 'predict': 'area'}})
        self.assertEqual(out['hits'][0]['$why']['factors'], [{'proposition': {'title': 'CI'}, 'lift': 1.23}])

    def test_by_default_only_the_top_hit_carries_why(self):
        hits = [{'$value': v, '$p': p, '$why': NEUTRAL_WHY} for v, p in (('rnd', 0.98), ('ops', 0.01))]
        for tool in ('predict', 'recommend', 'match'):
            with self.subTest(tool):
                server = self.serve(FakeResponse(200, {'hits': hits}))
                out = run(server, tool, {'query': {'from': 't', tool: 'x'}})
                self.assertIn('$why', out['hits'][0])
                self.assertNotIn('$why', out['hits'][1])
                self.assertEqual(out['hits'][1], {'$value': 'ops', '$p': 0.01})

    def test_an_explicit_select_with_why_keeps_it_on_every_hit(self):
        hits = [{'$value': v, '$p': p, '$why': NEUTRAL_WHY} for v, p in (('rnd', 0.98), ('ops', 0.01))]
        server = self.serve(FakeResponse(200, {'hits': hits}))
        out = run(server, 'predict', {'query': {'from': 't', 'predict': 'x', 'select': ['$value', '$p', '$why']}})
        self.assertTrue(all('$why' in h for h in out['hits']))

    def test_relate_reads_as_lift_with_its_counts_and_flags_small_samples(self):
        server = self.serve(FakeResponse(200, RELATE_RESPONSE))
        out = run(server, 'relate', {'query': {'from': 'touches', 'where': {'good_outcome': True},
                                               'relate': 'channel'}})
        self.assertEqual(out['condition'], {'good_outcome': True})
        self.assertEqual(out['hits'][0], {
            'related': {'channel': 'meeting'}, 'lift': 1.18, 'n_related': 10, 'n_with': 8,
            'n_condition': 22, 'n': 39, 'p': 0.256, 'p_given_condition': 0.364, 'small_sample': False})
        self.assertTrue(out['hits'][1]['small_sample'])        # a ×0.62 lift resting on 6 rows
        self.assertIn('small_sample', out['note'])

    def test_relate_raw_returns_the_engine_response(self):
        server = self.serve(FakeResponse(200, RELATE_RESPONSE))
        out = run(server, 'relate', {'query': {'from': 'touches', 'relate': 'channel'}, 'raw': True})
        self.assertEqual(out, RELATE_RESPONSE)

    def test_evaluate_leads_with_the_honest_comparison(self):
        server = self.serve(FakeResponse(200, EVALUATION))
        out = run(server, 'evaluate', {'query': {'test': {}, 'evaluate': {}}})
        self.assertEqual(out['summary'], {
            'reading': 'accuracy 89.3% on 224 held-out rows, vs 77.7% for always guessing the most '
                       'common value; calibration error (ECE) 0.055; 107 ms per prediction',
            'accuracy': 0.8928571428571429, 'base_accuracy': 0.7767857142857143,
            'accuracy_gain': 0.1160714285714286, 'ece': 0.05470956672436107,
            'n': 224, 'train_rows': 892, 'mean_ms': 106.96899712946428})
        self.assertEqual(out['metrics'], EVALUATION['data'])

    def test_evaluate_raw_returns_the_metrics_as_before(self):
        server = self.serve(FakeResponse(200, EVALUATION))
        out = run(server, 'evaluate', {'query': {'test': {}, 'evaluate': {}}, 'raw': True})
        self.assertEqual(out, EVALUATION['data'])

    def test_evaluate_cases_stay_next_to_the_summary(self):
        cases = [{'accurate': True, 'top': {'$value': 'a', '$p': 0.9}}]
        server = self.serve(FakeResponse(200, {'kind': 'evaluation', 'data': {'accuracy': 1.0, 'cases': cases}}))
        out = run(server, 'evaluate', {'query': {'test': {}, 'evaluate': {}, 'select': ['accuracy', 'cases']}})
        self.assertEqual(out['cases'], cases)
        self.assertNotIn('cases', out['metrics'])

    def test_the_tools_that_return_rows_carry_the_privacy_note(self):
        desc = descriptions(self.serve())
        for tool in ('search', 'query', 'evaluate'):
            with self.subTest(tool):
                self.assertIn(mcp_server.PRIVACY, desc[tool])
        self.assertIn(mcp_server.PRIVACY, mcp_server.INSTRUCTIONS)
