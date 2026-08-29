"""Offline tests for the v2 response classes

Every payload here is a verbatim body captured from a live Aito instance
(``https://shared.aito.ai``, build ``3de8f4f7ede5``, 2026-08-27) while
implementing the client, so these lock the real wire contract rather than an
assumed one. The live counterparts are in ``test_v2_live.py``.
"""

from aito.client.v2 import (
    V2AggregateResponse, AitoV2ResponseError, V2BatchResponse, V2EstimateResponse,
    V2EvaluationResponse, V2RowsResponse, V2Warning, response_for_kind, unwrap_payload,
)
from tests.cases import BaseTestCase


#: verbatim: POST /api/v2/_predict on a v2 collection
PREDICT_BODY = {
    'offset': 0,
    'total': 4,
    'hits': [
        {'$p': 0.9656853940913004, '$value': '6110'},
        {'$p': 0.014075043166280023, '$value': '6200'},
    ],
}

#: verbatim: POST /api/v2/_estimate on a v2 collection — enveloped
ESTIMATE_V2_BODY = {'kind': 'estimate', 'data': {'value': 325.79213669489786}}

#: verbatim: POST /api/v2/_estimate on a LEGACY table — the rep1 shim's v1 shape.
#: Same endpoint, same build, no `kind`, and the scalar is named `estimate`.
ESTIMATE_REP1_BODY = {'estimate': 0.7546647387662796, 'why': {'type': 'weightedAverage'}}

#: verbatim: POST /api/v2/_aggregate on a v2 collection
AGGREGATE_BODY = {
    'kind': 'aggregate',
    'data': {
        'amount.$mean': 458.14192500000007,
        'amount.$mean.samples': 400,
        'amount.$sum': 183256.77000000002,
    },
}

#: verbatim: POST /api/v2/_evaluate on a v2 collection
EVALUATION_BODY = {
    'kind': 'evaluation',
    'data': {
        'accuracy': 1.0, 'baseAccuracy': 0.2777777777777778,
        'testSamples': 40, 'trainSamples': 360, 'n': 40,
    },
}

#: verbatim: POST /api/v2/_query with a where on an undeclared column
WARNING_BODY = {
    'offset': 0, 'total': 0, 'hits': [],
    'warnings': [{
        'code': 'where.unknown_field',
        'message': "where references 'no_such_col', which is not a declared column "
                   'on this collection; it is present on no row, so this condition '
                   'matched nothing (possible typo).',
        'severity': 'warning',
        'field': 'no_such_col',
    }],
}


class TestUnwrapPayload(BaseTestCase):
    """The adapter that copes with the two shapes a non-rows v2 endpoint returns"""

    def test_unwraps_an_envelope_of_the_expected_kind(self):
        self.assertEqual(unwrap_payload(ESTIMATE_V2_BODY, 'estimate'), {'value': 325.79213669489786})

    def test_passes_a_bare_rep1_body_through(self):
        # The trap: `kind ?? "rows"` would classify this scalar as a page of rows.
        self.assertEqual(unwrap_payload(ESTIMATE_REP1_BODY, 'estimate'), ESTIMATE_REP1_BODY)

    def test_raises_on_a_different_kind(self):
        with self.assertRaises(AitoV2ResponseError) as ctx:
            unwrap_payload(AGGREGATE_BODY, 'estimate')
        self.assertIn("expected a 'estimate' response", str(ctx.exception))
        self.assertIn("got 'aggregate'", str(ctx.exception))

    def test_passes_a_non_dict_through(self):
        self.assertEqual(unwrap_payload([1, 2], 'batch'), [1, 2])


class TestRowsResponse(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.resp = V2RowsResponse(PREDICT_BODY)

    def test_envelope(self):
        self.assertEqual(self.resp.total, 4)
        self.assertEqual(self.resp.offset, 0)
        self.assertEqual(len(self.resp), 2)

    def test_hit_accessors(self):
        top = self.resp.first
        self.assertEqual(top.value, '6110')
        self.assertAlmostEqual(top.probability, 0.9656853940913004)
        self.assertEqual(top['$value'], '6110')
        self.assertIn('$p', top)

    def test_predicted_value_is_not_aliased_to_the_v1_name(self):
        # v1 called this `feature`. The SDK deliberately does not alias it back.
        self.assertNotIn('feature', self.resp.first)

    def test_missing_select_raises_a_message_that_says_why(self):
        with self.assertRaises(KeyError) as ctx:
            _ = self.resp.first.why
        self.assertIn('select', str(ctx.exception))

    def test_first_is_none_when_nothing_matched(self):
        self.assertIsNone(V2RowsResponse({'offset': 0, 'total': 0, 'hits': []}).first)

    def test_iterates_hits(self):
        self.assertEqual([hit.value for hit in self.resp], ['6110', '6200'])

    def test_tolerates_a_body_without_offset_or_total(self):
        resp = V2RowsResponse({'hits': [{'$value': 'x'}]})
        self.assertEqual(resp.total, 1)
        self.assertEqual(resp.offset, 0)


class TestEstimateResponse(BaseTestCase):
    def test_reads_the_v2_scalar(self):
        self.assertAlmostEqual(V2EstimateResponse(ESTIMATE_V2_BODY).value, 325.79213669489786)

    def test_reads_the_rep1_scalar_under_its_v1_name(self):
        # The whole point of the dual read: the same call against a legacy table.
        self.assertAlmostEqual(V2EstimateResponse(ESTIMATE_REP1_BODY).value, 0.7546647387662796)

    def test_why_is_none_when_not_selected(self):
        self.assertIsNone(V2EstimateResponse(ESTIMATE_V2_BODY).why)

    def test_why_is_read_from_either_shape(self):
        self.assertEqual(V2EstimateResponse(ESTIMATE_REP1_BODY).why, {'type': 'weightedAverage'})

    def test_raises_when_neither_key_is_present(self):
        with self.assertRaises(KeyError):
            _ = V2EstimateResponse({'kind': 'estimate', 'data': {}}).value


class TestAggregateResponse(BaseTestCase):
    def test_reads_aggregate_keys(self):
        resp = V2AggregateResponse(AGGREGATE_BODY)
        self.assertAlmostEqual(resp['amount.$mean'], 458.14192500000007)
        self.assertEqual(resp['amount.$mean.samples'], 400)
        self.assertIn('amount.$sum', resp)

    def test_unknown_key_raises(self):
        with self.assertRaises(KeyError):
            _ = V2AggregateResponse(AGGREGATE_BODY)['amount.$median']


class TestEvaluationResponse(BaseTestCase):
    def test_metrics(self):
        resp = V2EvaluationResponse(EVALUATION_BODY)
        self.assertEqual(resp.accuracy, 1.0)
        self.assertAlmostEqual(resp.base_accuracy, 0.2777777777777778)
        self.assertEqual(resp.test_sample_count, 40)
        self.assertEqual(resp.train_sample_count, 360)
        self.assertEqual(resp.cases, [])

    def test_reads_a_bare_rep1_evaluation(self):
        # The rep1 shim returns the metrics flat, with no `kind`.
        resp = V2EvaluationResponse({'accuracy': 1.0, 'baseAccuracy': 0.0,
                                   'testSamples': 1, 'trainSamples': 41.0})
        self.assertEqual(resp.accuracy, 1.0)
        self.assertEqual(resp.test_sample_count, 1)


class TestBatchResponse(BaseTestCase):
    def test_reads_the_bare_array_the_engine_actually_returns(self):
        # POST /api/v2/_batch answers a bare JSON array, NOT the
        # {"kind": "batch", "data": [...]} envelope the spec documents.
        # This is the shape a caller meets today.
        resp = V2BatchResponse([PREDICT_BODY, PREDICT_BODY])
        self.assertEqual(len(resp), 2)
        self.assertTrue(all(isinstance(r, V2RowsResponse) for r in resp.responses))
        self.assertEqual(resp.responses[0].first.value, '6110')

    def test_reads_the_enveloped_form_the_spec_documents(self):
        # Kept so the class survives the engine being brought in line.
        resp = V2BatchResponse({'kind': 'batch', 'data': [PREDICT_BODY, ESTIMATE_V2_BODY]})
        self.assertEqual(len(resp), 2)
        rows, estimate = resp.responses
        self.assertIsInstance(rows, V2RowsResponse)
        self.assertIsInstance(estimate, V2EstimateResponse)
        self.assertEqual(rows.first.value, '6110')
        self.assertAlmostEqual(estimate.value, 325.79213669489786)


class TestWarnings(BaseTestCase):
    def test_warnings_are_parsed(self):
        resp = V2RowsResponse(WARNING_BODY)
        self.assertEqual(len(resp.warnings), 1)
        warning = resp.warnings[0]
        self.assertIsInstance(warning, V2Warning)
        self.assertEqual(warning.code, 'where.unknown_field')
        self.assertEqual(warning.severity, 'warning')
        self.assertEqual(warning.field, 'no_such_col')
        self.assertIn('possible typo', warning.message)
        self.assertIn("field 'no_such_col'", str(warning))

    def test_no_warnings_is_an_empty_list_not_none(self):
        self.assertEqual(V2RowsResponse(PREDICT_BODY).warnings, [])

    def test_severity_defaults_to_warning(self):
        self.assertEqual(V2Warning({'code': 'x'}).severity, 'warning')


class TestMetaAndDispatch(BaseTestCase):
    def test_meta_is_empty_unless_requested(self):
        resp = V2RowsResponse(PREDICT_BODY)
        self.assertEqual(resp.meta, {})
        self.assertIsNone(resp.engine)

    def test_engine_is_read_from_meta(self):
        body = dict(PREDICT_BODY, meta={'engine': 'v1'})
        self.assertEqual(V2RowsResponse(body).engine, 'v1')

    def test_response_for_kind(self):
        self.assertIs(response_for_kind('estimate'), V2EstimateResponse)
        self.assertIs(response_for_kind('aggregate'), V2AggregateResponse)
        self.assertIs(response_for_kind('evaluation'), V2EvaluationResponse)
        self.assertIs(response_for_kind('batch'), V2BatchResponse)
        self.assertIs(response_for_kind('rows'), V2RowsResponse)

    def test_unknown_kind_falls_back_to_rows(self):
        # A kind added by a newer engine must not crash an older SDK.
        self.assertIs(response_for_kind('something_new'), V2RowsResponse)
