"""End-to-end tests for the v2 client against a live Aito instance

Skipped unless ``AITO_INSTANCE_URL`` and ``AITO_API_KEY`` are set — the key must
be read-write, because the suite creates a collection, loads it, queries it and
drops it again.

These are the tests that would have caught the things the offline suite cannot:
that the named endpoints exist and enforce their mode, that a rep2 collection and
a legacy table answer ``_estimate`` with different shapes, and that the warnings
channel is real.
"""

import os
import unittest

from aito.client.v2 import AitoClientV2, AitoV2Error, V2EstimateResponse
from tests.cases import BaseTestCase

#: the collection this suite creates and drops
TEST_COLLECTION = 'aito_python_sdk_v2_test'

VENDORS = [
    ('Elenia Oy', 'electricity network transfer invoice', '6110'),
    ('Neste Oyj', 'fuel purchase diesel', '6200'),
    ('Fazer Food Services', 'staff lunch catering', '7300'),
    ('Telia Finland', 'mobile subscription monthly', '6400'),
]


def _entries(count=400):
    """deterministic rows — a vendor implies a GL code, which is what predict learns"""
    rows = []
    for i in range(count):
        vendor, description, gl_code = VENDORS[i % len(VENDORS)]
        rows.append({
            'vendor': vendor,
            'description': description,
            'amount': round(50 + (i * 7 % 850) + 0.5, 2),
            'gl_code': gl_code,
        })
    return rows


def _env(name):
    return os.getenv(name)


@unittest.skipUnless(
    _env('AITO_INSTANCE_URL') and _env('AITO_API_KEY'),
    'AITO_INSTANCE_URL and a read-write AITO_API_KEY are required for the live v2 tests')
class TestAitoClientV2Live(BaseTestCase):
    """The full v2 surface, against a real instance, on a collection we own"""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.client = AitoClientV2(_env('AITO_INSTANCE_URL'), _env('AITO_API_KEY'))
        # Drop a collection left behind by an interrupted run, then recreate.
        try:
            cls.client.delete_collection(TEST_COLLECTION)
        except AitoV2Error as err:
            if not err.is_not_found:
                raise
        cls.client.create_collection(TEST_COLLECTION, {
            'vendor': {'type': 'String'},
            'description': {'type': 'Text', 'analyzer': 'english'},
            'amount': {'type': 'Decimal'},
            'gl_code': {'type': 'String'},
        })
        cls.uploaded = cls.client.upload_entries(TEST_COLLECTION, _entries(), batch_size=200)
        # Without this, predict's per-segment statistics are unmerged and the
        # posteriors are flatter and dependent on the batch count.
        cls.client.optimize(TEST_COLLECTION)

    @classmethod
    def tearDownClass(cls):
        try:
            cls.client.delete_collection(TEST_COLLECTION)
        except AitoV2Error:
            pass

    # --- schema and data ----------------------------------------------

    def test_upload_reports_the_row_count(self):
        self.assertEqual(self.uploaded, 400)

    def test_get_schema_lists_the_collection(self):
        schema = self.client.get_schema()
        tables = schema.get('schema', schema)
        self.assertIn(TEST_COLLECTION, tables)

    def test_get_schema_round_trips_the_column_options(self):
        table = self.client.get_schema(TEST_COLLECTION)
        columns = table['columns']
        self.assertEqual(columns['vendor']['type'], 'String')
        # `analyzer` used to be accepted and then dropped from the read-back.
        self.assertEqual(columns['description'].get('analyzer'), 'english')

    def test_delete_entries_removes_only_the_selected_rows(self):
        # Runs on its own collection so it cannot disturb the shared fixture.
        scratch = f'{TEST_COLLECTION}_delete'
        try:
            self.client.delete_collection(scratch)
        except AitoV2Error as err:
            if not err.is_not_found:
                raise
        self.client.create_collection(scratch, {'a': {'type': 'String'}})
        try:
            self.client.upload_entries(scratch, [{'a': 'keep'}, {'a': 'drop'}, {'a': 'drop'}])
            self.assertEqual(self.client.search(from_table=scratch, limit=0).total, 3)
            self.client.delete_entries(scratch, {'a': 'drop'})
            self.assertEqual(self.client.search(from_table=scratch, limit=0).total, 1)
            # `_modify` is table maintenance, not row modification.
            self.client.modify([{'optimize': scratch}])
        finally:
            self.client.delete_collection(scratch)

    def test_missing_collection_is_a_typed_not_found(self):
        with self.assertRaises(AitoV2Error) as ctx:
            self.client.get_schema('no_such_collection_here')
        self.assertTrue(ctx.exception.is_not_found)
        self.assertEqual(ctx.exception.status_code, 404)

    # --- queries -------------------------------------------------------

    def test_search_returns_rows(self):
        res = self.client.search(from_table=TEST_COLLECTION,
                                 where={'vendor': 'Elenia Oy'},
                                 select=['vendor', 'gl_code'], limit=3)
        self.assertEqual(res.total, 100)
        self.assertEqual(len(res), 3)
        self.assertEqual(res.first['gl_code'], '6110')

    def test_search_limit_zero_returns_only_the_count(self):
        res = self.client.search(from_table=TEST_COLLECTION, limit=0)
        self.assertEqual(res.total, 400)
        self.assertEqual(len(res), 0)

    def test_predict_learns_the_vendor_to_gl_code_relationship(self):
        res = self.client.predict(from_table=TEST_COLLECTION,
                                  where={'vendor': 'Elenia Oy'}, predict='gl_code')
        self.assertEqual(res.first.value, '6110')
        self.assertGreater(res.first.probability, 0.9)

    def test_predict_why_returns_an_explanation(self):
        res = self.client.predict(from_table=TEST_COLLECTION,
                                  where={'vendor': 'Neste Oyj'}, predict='gl_code',
                                  why=True, limit=1)
        self.assertEqual(res.first.value, '6200')
        self.assertIsInstance(res.first.why, dict)

    def test_relate_returns_lift_and_frequencies(self):
        res = self.client.relate(from_table=TEST_COLLECTION,
                                 where={'gl_code': '6110'}, relate='vendor', limit=5)
        self.assertGreaterEqual(len(res), 1)
        hit = res.first
        self.assertEqual(hit['related'], {'vendor': 'Elenia Oy'})
        self.assertGreater(hit['lift'], 1.0)
        self.assertIn('fOnCondition', hit['fs'])

    def test_estimate_returns_the_v2_scalar_on_a_collection(self):
        res = self.client.estimate(from_table=TEST_COLLECTION,
                                   where={'vendor': 'Elenia Oy'}, estimate='amount')
        self.assertIsInstance(res, V2EstimateResponse)
        self.assertEqual(res.json.get('kind'), 'estimate')
        self.assertIsInstance(res.value, float)

    def test_aggregate_returns_the_requested_expressions(self):
        res = self.client.aggregate(from_table=TEST_COLLECTION,
                                    aggregate=['amount.$mean', 'amount.$sum'])
        self.assertIn('amount.$mean', res)
        self.assertIn('amount.$sum', res)
        self.assertEqual(res['amount.$mean.samples'], 400)

    def test_evaluate_beats_the_base_rate(self):
        res = self.client.evaluate({
            'test': {'$index': {'$mod': [10, 0]}},
            'evaluate': {
                'from': TEST_COLLECTION,
                'where': {'vendor': {'$get': 'vendor'}},
                'predict': 'gl_code',
            },
        })
        self.assertEqual(res.json.get('kind'), 'evaluation')
        self.assertGreater(res.accuracy, res.base_accuracy)
        self.assertEqual(res.test_sample_count + res.train_sample_count, 400)

    def test_query_is_the_escape_hatch(self):
        res = self.client.query({'from': TEST_COLLECTION, 'limit': 2, 'select': ['vendor']})
        self.assertEqual(len(res), 2)

    def test_batch_dispatches_each_sub_result(self):
        res = self.client.batch([
            {'from': TEST_COLLECTION, 'limit': 1, 'select': ['vendor']},
            {'from': TEST_COLLECTION, 'where': {'vendor': 'Elenia Oy'},
             'predict': 'gl_code', 'select': ['$p', '$value'], 'limit': 1},
        ])
        self.assertEqual(len(res), 2)
        # The engine answers a bare array here, not the documented
        # {"kind": "batch", "data": [...]} envelope.
        self.assertIsInstance(res.json, list)
        self.assertEqual(res.responses[1].first.value, '6110')

    def test_one_bad_query_fails_the_whole_batch(self):
        # A batch is not a way to collect per-query failures.
        with self.assertRaises(AitoV2Error) as ctx:
            self.client.batch([
                {'from': TEST_COLLECTION, 'limit': 1},
                {'from': 'no_such_collection_here', 'limit': 1},
            ])
        self.assertEqual(ctx.exception.code, 'not_found')

    def test_reassigning_the_api_key_takes_effect(self):
        client = AitoClientV2(_env('AITO_INSTANCE_URL'), 'a-wrong-key', check_credentials=False)
        with self.assertRaises(AitoV2Error):
            client.get_schema()
        client.api_key = _env('AITO_API_KEY')
        self.assertIn('schema', client.get_schema())

    # --- the sharp edges the client exists to smooth --------------------

    def test_named_endpoints_enforce_their_mode(self):
        # A bare filter posted to _predict is a loud 400 naming the right
        # endpoint, rather than silently running a filter.
        with self.assertRaises(AitoV2Error) as ctx:
            self.client.request('POST', '/_predict', {'from': TEST_COLLECTION, 'limit': 1})
        self.assertEqual(ctx.exception.code, 'request.invalid')
        self.assertIn('_query', str(ctx.exception))

    def test_a_predict_body_posted_to_search_is_rejected(self):
        with self.assertRaises(AitoV2Error) as ctx:
            self.client.request('POST', '/_search',
                                {'from': TEST_COLLECTION, 'predict': 'gl_code'})
        self.assertEqual(ctx.exception.code, 'request.invalid')
        self.assertIn('_predict', str(ctx.exception))

    def test_a_where_on_an_undeclared_column_comes_back_as_a_warning(self):
        res = self.client.query({'from': TEST_COLLECTION,
                                 'where': {'no_such_col': 'x'}, 'limit': 1})
        codes = [w.code for w in res.warnings]
        self.assertIn('where.unknown_field', codes)
        self.assertEqual(res.total, 0)

    def test_on_warning_raise_makes_a_broadened_query_fail(self):
        strict = AitoClientV2(_env('AITO_INSTANCE_URL'), _env('AITO_API_KEY'),
                              on_warning='raise', check_credentials=False)
        with self.assertRaises(AitoV2Error) as ctx:
            strict.query({'from': TEST_COLLECTION, 'where': {'no_such_col': 'x'}, 'limit': 1})
        self.assertIn('where.unknown_field', str(ctx.exception))

    def test_meta_names_the_engine_that_answered(self):
        client = AitoClientV2(_env('AITO_INSTANCE_URL'), _env('AITO_API_KEY'),
                              meta=True, check_credentials=False)
        res = client.predict(from_table=TEST_COLLECTION,
                             where={'vendor': 'Elenia Oy'}, predict='gl_code', limit=1)
        self.assertEqual(res.engine, 'v2')

    def test_introspection_endpoints(self):
        self.assertIn('version', self.client.get_version())
        ops = self.client.get_operators()
        self.assertGreater(ops['count'], 0)


@unittest.skipUnless(
    _env('AITO_GROCERY_DEMO_INSTANCE_URL') and _env('AITO_GROCERY_DEMO_API_KEY'),
    'the grocery demo credentials are required for the legacy-table tests')
class TestAitoClientV2AgainstLegacyTables(BaseTestCase):
    """A read-only database of v1-created tables, answered by the rep1 shim

    This is the half of the surface the offline fixtures were captured from and
    the reason ``unwrap_payload`` cannot just trust ``kind ?? "rows"``.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.client = AitoClientV2(_env('AITO_GROCERY_DEMO_INSTANCE_URL'),
                                  _env('AITO_GROCERY_DEMO_API_KEY'))

    def test_predict_works_on_a_legacy_table(self):
        res = self.client.predict(from_table='products',
                                  where={'name': 'Pirkka banana'}, predict='category', limit=3)
        self.assertGreater(res.total, 0)
        self.assertIsNotNone(res.first.value)

    def test_meta_reports_the_rep1_engine_for_a_legacy_table(self):
        client = AitoClientV2(_env('AITO_GROCERY_DEMO_INSTANCE_URL'),
                              _env('AITO_GROCERY_DEMO_API_KEY'),
                              meta=True, check_credentials=False)
        res = client.predict(from_table='products', where={'name': 'Pirkka banana'},
                             predict='category', limit=1)
        self.assertEqual(res.engine, 'v1')

    def test_estimate_returns_the_bare_v1_shape_and_is_still_read_correctly(self):
        # The documented client rule `kind ?? "rows"` would call this a page of
        # rows. It is a scalar, in the v1 shape, from a v2 endpoint.
        res = self.client.estimate(from_table='products',
                                   where={'name': 'Pirkka banana'}, estimate='price')
        self.assertNotIn('kind', res.json)
        self.assertIsInstance(res.value, float)

    def test_a_missing_table_is_a_typed_not_found(self):
        with self.assertRaises(AitoV2Error) as ctx:
            self.client.query({'from': 'no_such_table', 'limit': 1})
        self.assertEqual(ctx.exception.code, 'not_found')
