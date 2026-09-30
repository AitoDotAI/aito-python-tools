"""`Client.upload_csv` (ADR 0003), offline: the typing rules and the server/client choice

The rules mirror aito-core #1535 (CollectionDbCsvImport, DocumentType.StringStats);
the boundary vectors below are the ones a live test runs against the engine.
"""

import io
import json
import logging
import tempfile
from pathlib import Path

from aito._csv_types import CsvFormatError, infer_csv, looks_like_text, parse_csv
from aito.v2 import AitoClientV2, AitoV2Error
from tests.cases import BaseTestCase
from tests.sdk.test_v2_client import FakeResponse, RecordingSession


def _prose(i):
    return f"invoice for office supplies number {i} and more"


class TestParse(BaseTestCase):
    def test_quotes_doubled_quotes_and_quoted_newlines(self):
        header, rows = parse_csv(b'a,b\n"x, y","say ""hi"""\n"line1\nline2",z\n')
        self.assertEqual(header, ['a', 'b'])
        self.assertEqual(rows, [['x, y', 'say "hi"'], ['line1\nline2', 'z']])

    def test_an_empty_cell_is_missing(self):
        _, rows = parse_csv(b'a,b\n1,\n,2\n')
        self.assertEqual(rows, [['1', None], [None, '2']])

    def test_bad_shapes_name_the_row(self):
        for body, phrase in ((b'', 'empty'), (b'a,,c\n1,2,3\n', 'header column 2 is empty'),
                             (b'a,a\n1,2\n', 'duplicate header'), (b'a,b\n1,2\n3\n', 'row 3 has 1 cells')):
            with self.assertRaises(CsvFormatError, msg=body) as ctx:
                parse_csv(body)
            self.assertIn(phrase, str(ctx.exception))


class TestColumnKinds(BaseTestCase):
    def kinds(self, body):
        return {c: t['type'] for c, t in infer_csv(body).items()}

    def test_numbers_booleans_and_strings(self):
        self.assertEqual(self.kinds(b'i,d,b,s\n1,1.5,true,x\n-2,1e3,FALSE,y\n'),
                         {'i': 'Int', 'd': 'Decimal', 'b': 'Boolean', 's': 'String'})

    def test_a_leading_zero_keeps_a_column_a_string(self):
        self.assertEqual(self.kinds(b'zip,f\n00100,0.5\n12,0\n'), {'zip': 'String', 'f': 'Decimal'})

    def test_beyond_32_bits_is_long(self):
        self.assertEqual(self.kinds(b'n\n5000000000\n1\n'), {'n': 'Long'})

    def test_a_column_with_a_missing_cell_is_nullable(self):
        schema = infer_csv(b'a,b\n1,x\n,y\n')
        self.assertTrue(schema['a']['nullable'])
        self.assertNotIn('nullable', schema['b'])

    def test_free_text_is_text_with_no_analyzer(self):
        body = 'description,category\n' + ''.join(f'"{_prose(i)}",Office\n' for i in range(30))
        schema = infer_csv(body.encode())
        self.assertEqual(schema['description'], {'type': 'Text'})
        self.assertEqual(schema['category'], {'type': 'String'})


class TestTextRuleBoundaries(BaseTestCase):
    """the StringStats rule: > half multi-word, average length > 15, distinct > min(20, n // 2)"""

    def test_average_length_must_exceed_15(self):
        at_15 = [f'ab cd efgh ij{i:02d}' for i in range(30)]           # 15 characters each
        self.assertEqual(len(at_15[0]), 15)
        self.assertFalse(looks_like_text(at_15))
        self.assertTrue(looks_like_text([v + 'x' for v in at_15]))

    def test_more_than_half_must_be_multi_word(self):
        half = [f'word number {i:03d} here' if i % 2 else f'singlewordvalue{i:03d}' for i in range(30)]
        self.assertFalse(looks_like_text(half))                   # exactly half
        self.assertTrue(looks_like_text(half[1:]))                # 15 of 29

    def test_distinct_must_exceed_min_20_half_n(self):
        twenty = [f'a long enough phrase {i:02d}' for i in range(20)] * 3    # n=60, 20 distinct
        self.assertFalse(looks_like_text(twenty))
        self.assertTrue(looks_like_text(twenty + ['a long enough phrase 99']))

    def test_small_columns_use_half_n(self):
        vals = [f'another long phrase {i}' for i in range(6)]      # n=6: needs > 3 distinct
        self.assertTrue(looks_like_text(vals))
        self.assertFalse(looks_like_text(vals[:3] * 2))


class TestUploadCsv(BaseTestCase):
    CSV = b'invoice_id,amount,category\nA1,12.5,Office\nA2,7,Travel\n'
    OLD_ENGINE = FakeResponse(400, {'kind': 'error', 'data': {
        'code': 'json.malformed',
        'message': "Unrecognized token 'invoice_id': was expecting (JSON String, Number, Array, Object or token "
                   "'null', 'true' or 'false')"}})

    def client(self, *responses):
        c = AitoClientV2('https://x/db/y', 'k', check_credentials=False)
        c._session = RecordingSession(list(responses))
        return c

    def test_a_new_engine_takes_the_file_as_it_is(self):
        c = self.client(FakeResponse(404, {'kind': 'error', 'data': {'code': 'not_found', 'message': 'no table'}}),
                        FakeResponse(200, {'status': 'created', 'count': 2, 'inferred': {'amount': {'type': 'Decimal'}}}))
        res = c.upload_csv('invoices', self.CSV)
        self.assertEqual(res.via, 'server')
        self.assertEqual(res.rows, 2)
        post = c._session.calls[-1]
        self.assertEqual(post['headers']['Content-Type'], 'text/csv')
        self.assertEqual(post['data'], self.CSV)

    def test_an_old_engine_falls_back_to_the_client_path(self):
        c = self.client(FakeResponse(404, {'kind': 'error', 'data': {'code': 'not_found', 'message': 'no table'}}),
                        self.OLD_ENGINE,
                        FakeResponse(200, {'status': 'created'}),
                        FakeResponse(200, {'count': 2}))
        with self.assertLogs('aito.v2', level=logging.DEBUG) as logs:
            res = c.upload_csv('invoices', self.CSV)
        self.assertEqual(res.via, 'client')
        self.assertEqual(res.rows, 2)
        self.assertTrue(any("via='client'" in line for line in logs.output), logs.output)
        created = c._session.calls[2]['json']['columns']
        self.assertEqual(created['amount'], {'type': 'Decimal'})
        rows = c._session.calls[3]['json']
        self.assertEqual(rows[1], {'invoice_id': 'A2', 'amount': 7, 'category': 'Travel'})

    def test_an_old_engine_on_a_quoted_header_also_falls_back(self):
        quoted = FakeResponse(400, {'kind': 'error', 'data': {
            'code': 'data.bad_request', 'message': 'Expected JSON array for import'}})
        c = self.client(FakeResponse(404, {'kind': 'error', 'data': {'code': 'not_found', 'message': 'x'}}),
                        quoted, FakeResponse(200, {'status': 'created'}), FakeResponse(200, {'count': 1}))
        self.assertEqual(c.upload_csv('t', b'"a","b"\n1,x\n').via, 'client')

    def test_a_new_engines_csv_error_is_raised_never_retried(self):
        # the other direction: a real CSV problem must not silently become a client-side upload
        for body in ({'code': 'data.bad_request', 'message': 'CSV import: row 3 has 2 cells; the header has 3'},
                     {'code': 'import.failed', 'message': "column 'amount' is required"}):
            c = self.client(FakeResponse(404, {'kind': 'error', 'data': {'code': 'not_found', 'message': 'x'}}),
                            FakeResponse(400, {'kind': 'error', 'data': body}))
            with self.assertRaises(AitoV2Error) as ctx:
                c.upload_csv('invoices', self.CSV)
            self.assertIn(body['message'], str(ctx.exception))
            self.assertEqual(len(c._session.calls), 2, "it retried on the client path")

    def test_via_client_never_calls_import(self):
        c = self.client(FakeResponse(404, {'kind': 'error', 'data': {'code': 'not_found', 'message': 'x'}}),
                        FakeResponse(200, {'status': 'created'}), FakeResponse(200, {'count': 2}))
        self.assertEqual(c.upload_csv('invoices', self.CSV, via='client').via, 'client')
        self.assertFalse(any(call['url'].endswith('/import') for call in c._session.calls))

    def test_appending_converts_to_the_declared_types_and_names_a_bad_cell(self):
        existing = FakeResponse(200, {'type': 'collection', 'columns': {
            'invoice_id': {'type': 'String'}, 'amount': {'type': 'Decimal'}, 'category': {'type': 'String'}}})
        c = self.client(existing, FakeResponse(200, {'count': 2}))
        res = c.upload_csv('invoices', self.CSV)
        self.assertEqual(res.via, 'client')
        self.assertEqual(c._session.calls[-1]['json'][0]['amount'], 12.5)
        bad = self.client(existing)
        with self.assertRaises(ValueError) as ctx:
            bad.upload_csv('invoices', b'invoice_id,amount,category\nA1,twelve,Office\n')
        self.assertIn("row 2, column 'amount': 'twelve' is not a Decimal", str(ctx.exception))
        self.assertEqual(len(bad._session.calls), 1, "a bad cell must fail before anything is sent")

    def test_a_str_source_is_always_a_path(self):
        c = self.client()
        with self.assertRaises(FileNotFoundError):
            c.upload_csv('invoices', 'invoice_id,amount\nA1,1\n')
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / 'x.csv'
            p.write_bytes(self.CSV)
            self.assertEqual(infer_csv(p)['amount'], {'type': 'Decimal'})
            self.assertEqual(infer_csv(io.BytesIO(self.CSV))['amount'], {'type': 'Decimal'})


class TestV1UploadFileWarnsOnProseInString(BaseTestCase):
    def test_a_string_column_of_free_text_is_named(self):
        import pandas as pd
        from aito.cli._backends import warn_prose_in_string_columns
        from aito.schema import AitoTableSchema
        schema = AitoTableSchema.from_deserialized_object({'type': 'table', 'columns': {
            'description': {'type': 'String'}, 'category': {'type': 'String'}}})
        df = pd.DataFrame({'description': [_prose(i) for i in range(30)], 'category': ['Office'] * 30})
        with self.assertLogs('aito.cli', level='WARNING') as logs:
            warn_prose_in_string_columns(schema, df, 'invoices')
        self.assertEqual(len(logs.output), 1)
        self.assertIn("column 'description' of 'invoices' is declared String", logs.output[0])
