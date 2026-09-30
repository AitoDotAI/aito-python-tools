"""The CLI's database commands on the v2 API (``--api-version v2``).

The offline half pins how the API version is chosen and what the v2 backend
refuses. The live half runs each command against a real instance and checks
the result through ``aito.v2`` — skipped unless ``AITO_INSTANCE_URL`` and a
read-write ``AITO_API_KEY`` are set. It creates and drops its own uniquely named
collections, and never deletes the database.
"""

import io
import json
import os
import tempfile
import unittest
import warnings
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import MagicMock
from uuid import uuid4

from aito.cli._backends import NotSupportedOnV2, V2Backend
from aito.cli.parser import ParseError, create_backend_from_parsed_args, resolve_api_version
from tests.cli.parser_and_cli_test_case import ParserAndCLITestCase

SAMPLE = Path(__file__).resolve().parent.parent / 'io' / 'in' / 'sample_invoice'

VENDORS = [
    ('Elenia Oy', 'electricity network transfer', '6110'),
    ('Neste Oyj', 'fuel purchase diesel', '6200'),
    ('Fazer Food Services', 'staff lunch catering', '7300'),
    ('Telia Finland', 'mobile subscription monthly', '6400'),
]
GL_COLUMNS = {
    'vendor': {'type': 'String'},
    'description': {'type': 'Text', 'analyzer': 'english'},
    'amount': {'type': 'Decimal'},
    'gl_code': {'type': 'String'},
}


def _gl_rows(count=200):
    """deterministic rows: the vendor implies the GL code, which predict and evaluate learn"""
    return [{'vendor': v, 'description': d, 'amount': float(50 + (i * 7 % 850)), 'gl_code': g}
            for i, (v, d, g) in ((i, VENDORS[i % len(VENDORS)]) for i in range(count))]


class TestApiVersionResolution(unittest.TestCase):
    """offline: which API a command uses"""

    def setUp(self):
        self._saved = os.environ.pop('AITO_API_VERSION', None)

    def tearDown(self):
        os.environ.pop('AITO_API_VERSION', None)
        if self._saved is not None:
            os.environ['AITO_API_VERSION'] = self._saved

    def test_flag_wins_over_env_var(self):
        os.environ['AITO_API_VERSION'] = 'v1'
        self.assertEqual(resolve_api_version({'api_version': 'v2'}), 'v2')

    def test_env_var_is_used_without_warning(self):
        os.environ['AITO_API_VERSION'] = 'v2'
        with warnings.catch_warnings():
            warnings.simplefilter('error')
            self.assertEqual(resolve_api_version({'api_version': None}), 'v2')

    def test_default_is_v1_in_1x_and_warns_about_2_0(self):
        with self.assertWarns(FutureWarning) as caught:
            self.assertEqual(resolve_api_version({'api_version': None}), 'v1')
        self.assertIn('2.0', str(caught.warning))

    def test_invalid_env_var_is_refused(self):
        os.environ['AITO_API_VERSION'] = 'v3'
        with self.assertRaises(ParseError):
            resolve_api_version({'api_version': None})

    def test_env_needs_v2(self):
        with self.assertRaises(ParseError):
            create_backend_from_parsed_args(
                {'api_version': 'v1', 'env': 'staging', 'instance_url': 'http://x', 'api_key': 'k',
                 'profile': 'default'}, check_credentials=False)

    def test_env_reaches_the_v2_client(self):
        backend = create_backend_from_parsed_args(
            {'api_version': 'v2', 'env': 'staging', 'instance_url': 'http://x', 'api_key': 'k',
             'profile': 'default'}, check_credentials=False)
        self.assertEqual(backend.client.api_url, 'http://x/env/staging/api/v2')


class TestV2BackendOffline(unittest.TestCase):
    """offline: what v2 refuses, and how bodies reach the endpoints"""

    def setUp(self):
        self.client = MagicMock()
        self.backend = V2Backend(self.client)

    def test_rename_is_refused_with_the_workaround(self):
        with self.assertRaises(NotSupportedOnV2) as caught:
            self.backend.rename_table('a', 'b', False)
        self.assertIn('copy-table', str(caught.exception))

    def test_similarity_and_jobs_are_refused(self):
        with self.assertRaises(NotSupportedOnV2):
            self.backend.send_query('similarity', {}, use_job=False)
        with self.assertRaises(NotSupportedOnV2):
            self.backend.send_query('predict', {}, use_job=True)

    def test_a_query_body_is_posted_unchanged(self):
        self.client.request.return_value = {'hits': []}
        body = {'from': 't', 'where': {'a': 1}, 'predict': 'b'}
        self.backend.send_query('predict', body, use_job=False)
        self.client.request.assert_called_once_with('POST', '/_predict', body)
        self.backend.send_query('generic_query', body, use_job=False)
        self.client.request.assert_called_with('POST', '/_query', body)

    def test_a_v1_table_schema_creates_a_collection_from_its_columns(self):
        schema = json.loads((SAMPLE / 'invoice_aito_schema.json').read_text())
        self.backend.create_table('invoices', schema)
        self.client.create_collection.assert_called_once_with('invoices', schema['columns'])

    def test_a_v1_language_analyzer_object_becomes_a_v2_alias(self):
        self.backend.create_table('t', {'columns': {'c': {'type': 'Text', 'analyzer': {
            'type': 'language', 'language': 'english', 'useDefaultStopWords': False,
            'customStopWords': [], 'customKeyWords': []}}}})
        self.client.create_collection.assert_called_once_with('t', {'c': {'type': 'Text', 'analyzer': 'english'}})

    def test_custom_analyzer_words_are_refused(self):
        with self.assertRaises(NotSupportedOnV2):
            self.backend.create_table('t', {'columns': {'c': {'type': 'Text', 'analyzer': {
                'type': 'language', 'language': 'english', 'customStopWords': ['acme']}}}})

    def test_create_database_creates_each_table(self):
        self.backend.create_database({'schema': {'a': {'type': 'table', 'columns': {'x': {'type': 'Int'}}},
                                                 'b': {'type': 'collection', 'columns': {'y': {'type': 'Int'}}}}})
        self.assertEqual([c.args[0] for c in self.client.create_collection.call_args_list], ['a', 'b'])


@unittest.skipUnless(
    os.getenv('AITO_INSTANCE_URL') and os.getenv('AITO_API_KEY'),
    'AITO_INSTANCE_URL and a read-write AITO_API_KEY are required for the live CLI v2 tests')
class TestDatabaseSubCommandsV2Live(ParserAndCLITestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        from aito.v2 import Client
        cls.client = Client(os.environ['AITO_INSTANCE_URL'], os.environ['AITO_API_KEY'])
        cls.tmp = Path(tempfile.mkdtemp())
        cls.gl_rows_file = cls.tmp / 'gl_rows.json'
        cls.gl_rows_file.write_text(json.dumps(_gl_rows()))

    def setUp(self):
        super().setUp()
        self.name = f'cli_v2_{uuid4().hex[:10]}'
        self.addCleanup(self._drop, self.name)

    def _drop(self, name):
        try:
            self.client.delete_collection(name)
        except Exception:
            pass

    def cli(self, *args) -> str:
        """run `aito <args> --api-version v2` in-process and return its stdout"""
        argv = list(args)
        argv.insert(1, '--api-version')
        argv.insert(2, 'v2')
        out = io.StringIO()
        with redirect_stdout(out):
            self.parser.parse_and_execute(vars(self.parser.parse_args(argv)))
        return out.getvalue()

    def cli_refused(self, *args) -> str:
        """run a command that must be refused: exit status 2, and the reason on stderr"""
        err = io.StringIO()
        with redirect_stderr(err), self.assertRaises(SystemExit) as caught:
            self.cli(*args)
        self.assertEqual(caught.exception.code, 2)
        return err.getvalue()

    def rows(self, name):
        return self.client.search(from_table=name, limit=1000).json['hits']

    def load_gl(self):
        self.client.create_collection(self.name, GL_COLUMNS)
        self.client.upload_entries(self.name, _gl_rows())
        self.client.optimize(self.name)

    def test_create_table_from_a_v1_schema_file_makes_a_collection(self):
        self.cli('create-table', self.name, str(SAMPLE / 'invoice_aito_schema.json'))
        schema = self.client.get_schema(self.name)
        self.assertEqual(schema['type'], 'collection')
        self.assertEqual(set(schema['columns']), {'Remark', 'amount', 'id', 'name'})

    def test_get_table_and_show_tables(self):
        self.client.create_collection(self.name, GL_COLUMNS)
        self.assertEqual(set(json.loads(self.cli('get-table', self.name))['columns']), set(GL_COLUMNS))
        self.assertIn(self.name, self.cli('show-tables').split('\n'))
        database = json.loads(self.cli('get-database'))
        self.assertIn(self.name, database.get('schema', database))

    def test_upload_entries_and_optimize(self):
        self.client.create_collection(self.name, GL_COLUMNS)
        self.cli('upload-entries', self.name, str(self.gl_rows_file))
        self.cli('optimize-table', self.name)
        self.assertEqual(len(self.rows(self.name)), 200)

    def test_upload_file_converts_by_the_collection_schema(self):
        for fmt, path in (('csv', 'invoice.csv'), ('json', 'invoice_no_null_value.json')):
            self.cli('create-table', self.name, str(SAMPLE / 'invoice_aito_schema.json'))
            self.cli('upload-file', self.name, str(SAMPLE / path), '-f', fmt)
            rows = self.rows(self.name)
            self.assertEqual(len(rows), 4, fmt)
            self.assertTrue(all(isinstance(r['amount'], float) for r in rows), fmt)
            self.client.delete_collection(self.name)

    def test_quick_add_table(self):
        self.cli('quick-add-table', '-n', self.name, str(SAMPLE / 'invoice.csv'))
        self.assertEqual(self.client.get_schema(self.name)['type'], 'collection')
        self.assertEqual(len(self.rows(self.name)), 4)

    def test_copy_table_and_rename_refused(self):
        copy = f'{self.name}_copy'
        self.addCleanup(self._drop, copy)
        self.load_gl()
        self.cli('copy-table', self.name, copy)
        self.assertEqual(len(self.rows(copy)), 200)
        self.assertIn('copy-table', self.cli_refused('rename-table', self.name, f'{self.name}_renamed'))

    def test_copy_table_pages_past_1000_rows(self):
        copy = f'{self.name}_copy'
        self.addCleanup(self._drop, copy)
        self.client.create_collection(self.name, GL_COLUMNS)
        self.client.upload_entries(self.name, _gl_rows(2500))
        self.client.optimize(self.name)
        self.cli('copy-table', self.name, copy)
        self.assertEqual(self.client.search(from_table=copy, limit=0).json['total'], 2500)

    def test_query_commands_post_to_the_v2_endpoints(self):
        self.load_gl()
        predicted = json.loads(self.cli(
            'predict', json.dumps({'from': self.name, 'where': {'vendor': 'Neste Oyj'}, 'predict': 'gl_code'})))
        self.assertEqual(predicted['hits'][0]['$value'], '6200')
        searched = json.loads(self.cli('search', json.dumps({'from': self.name, 'where': {'gl_code': '7300'}})))
        self.assertEqual(searched['total'], 50)
        queried = json.loads(self.cli('generic-query', json.dumps({'from': self.name, 'limit': 1})))
        self.assertEqual(len(queried['hits']), 1)
        related = json.loads(self.cli(
            'relate', json.dumps({'from': self.name, 'where': {'vendor': 'Telia Finland'}, 'relate': 'gl_code'})))
        self.assertTrue(related['hits'])
        estimated = json.loads(self.cli(
            'estimate', json.dumps({'from': self.name, 'where': {'vendor': 'Elenia Oy'}, 'estimate': 'amount'})))
        self.assertIn('data', estimated)
        aggregated = json.loads(self.cli(
            'aggregate', json.dumps({'from': self.name, 'aggregate': ['amount.$sum']})))
        self.assertIn('data', aggregated)
        evaluated = json.loads(self.cli('evaluate', json.dumps({
            'test': {'$index': {'$mod': [10, 0]}},
            'evaluate': {'from': self.name, 'where': {'vendor': {'$get': 'vendor'}}, 'predict': 'gl_code'}})))
        self.assertEqual(evaluated['data']['accuracy'], 1.0)

    def test_similarity_and_use_job_are_refused(self):
        self.assertIn('no v2 endpoint', self.cli_refused('similarity', json.dumps({'from': self.name})))
        self.assertIn('v1 only', self.cli_refused(
            'predict', '--use-job', json.dumps({'from': self.name, 'predict': 'x'})))

    def test_quick_predict_with_evaluate(self):
        self.load_gl()
        out = self.cli('quick-predict', self.name, 'gl_code', '--evaluate')
        example = json.loads(out.split('[Predict Query Example]\n')[1].split('[Evaluation Result]')[0])
        self.assertEqual(example['select'], ['$p', '$value', '$why'])
        self.assertIn('- Accuracy: 1.0', out)

    def test_create_database_makes_each_collection(self):
        other = f'{self.name}_b'
        self.addCleanup(self._drop, other)
        schema_file = self.tmp / f'{self.name}_db.json'
        schema_file.write_text(json.dumps({'schema': {
            self.name: {'type': 'table', 'columns': GL_COLUMNS},
            other: {'type': 'collection', 'columns': {'x': {'type': 'Int'}}}}}))
        self.cli('create-database', str(schema_file))
        self.assertEqual(self.client.get_schema(self.name)['type'], 'collection')
        self.assertEqual(self.client.get_schema(other)['type'], 'collection')
