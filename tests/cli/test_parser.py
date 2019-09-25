import json
import os

import ndjson

from aito.cli.main_parser import MainParser
from aito.client.aito_client import AitoClient
from tests.test_case import TestCaseCompare


class TestConvertParser(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass(test_path='cli/convert')
        cls.input_folder = cls.input_folder.parent.parent / 'schema'
        cls.main_parser = MainParser()

    def setUp(self):
        super().setUp()
        self.out_file_path = self.output_folder / f"{self.method_name}_out.ndjson"

    def test_json_to_ndjson(self):
        self.main_parser.parse_and_execute(['convert', 'json', f"{self.input_folder / 'sample.json'}",
                                            f"{self.out_file_path}"])
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'sample.ndjson').open()))

    def test_csv_to_ndjson(self):
        self.main_parser.parse_and_execute(['convert', 'csv', f"{self.input_folder / 'sample.csv'}",
                                            f"{self.out_file_path}"])
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'sample.ndjson').open()))

    def test_csv_semicolon_to_ndjson(self):
        self.main_parser.parse_and_execute(['convert', 'csv', f"{self.input_folder / 'sample_semicolon.csv'}",
                                            f"{self.out_file_path}", "-d=;"])
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'sample.ndjson').open()))

    def test_excel_to_ndjson(self):
        self.main_parser.parse_and_execute(['convert', 'xlsx', f"{self.input_folder / 'sample.xlsx'}",
                                            f"{self.out_file_path}"])
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'sample.ndjson').open()))

    def test_generate_schema(self):
        schema_out_path = self.output_folder / f"{self.method_name}_schema_out.json"
        self.main_parser.parse_and_execute(['convert', f"-c={schema_out_path}", 'csv',
                                            f"{self.input_folder / 'sample.csv'}", f"{self.out_file_path}"])
        self.assertDictEqual(json.load(schema_out_path.open()),
                             json.load((self.input_folder / 'sample_schema.json').open()))

    def test_use_schema(self):
        self.main_parser.parse_and_execute(['convert', f"-s={self.input_folder / 'sample_schema_altered.json'}", 'csv',
                                            f"{self.input_folder / 'sample.csv'}", f"{self.out_file_path}"])
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'sample_altered.ndjson').open()))


class TestClientParser(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass(test_path='cli/client')
        cls.input_folder = cls.input_folder.parent.parent / 'schema'
        cls.main_parser = MainParser()
        env_var = os.environ
        cls.client = AitoClient(env_var['AITO_INSTANCE_URL'], env_var['AITO_RW_KEY'], env_var['AITO_RO_KEY'])

    def setUp(self):
        super().setUp()
        self.client.delete_database()

    def test_upload_batch_no_table_schema(self):
        with self.assertRaises(ValueError):
            self.main_parser.parse_and_execute(['client', 'upload-batch', 'sample',
                                                str(self.input_folder / 'sample.json')])

    def test_upload_batch_invalid_entries(self):
        with self.assertRaises(Exception):
            self.main_parser.parse_and_execute(['client', 'upload-batch', 'sample',
                                                str(self.input_folder / 'sample.ndjson')])

    def test_upload_batch(self):
        with (self.input_folder / "sample_schema.json").open() as f:
            table_schema = json.load(f)
        self.client.put_table_schema('sample', table_schema)
        self.main_parser.parse_and_execute(['client', 'upload-batch', 'sample',
                                            str(self.input_folder / 'sample.json')])
        self.assertEqual(self.client.query_table_entries('sample')['total'], 4)

    def test_upload_file_no_table_schema(self):
        with self.assertRaises(SystemExit) as context:
            self.main_parser.parse_and_execute(['client', 'upload-file', 'sample',
                                                str(self.input_folder / 'sample.ndjson')])
        self.assertEqual(context.exception.code, 2)

    def test_upload_file_with_table_schema(self):
        with (self.input_folder / "sample_schema.json").open() as f:
            table_schema = json.load(f)
        self.client.put_table_schema('sample', table_schema)
        self.main_parser.parse_and_execute(['client', 'upload-file', 'sample',
                                            str(self.input_folder / 'sample.ndjson')])
        self.assertEqual(self.client.query_table_entries('sample')['total'], 4)

    def test_upload_file_different_format(self):
        with (self.input_folder / "sample_schema.json").open() as f:
            table_schema = json.load(f)
        self.client.put_table_schema('sample', table_schema)
        self.main_parser.parse_and_execute(['client', 'upload-file', '-f=csv', 'sample',
                                            str(self.input_folder / 'sample.csv')])
        self.assertEqual(self.client.query_table_entries('sample')['total'], 4)

    def test_upload_file_infer_format(self):
        with (self.input_folder / "sample_schema.json").open() as f:
            table_schema = json.load(f)
        self.client.put_table_schema('sample', table_schema)
        self.main_parser.parse_and_execute(['client', 'upload-file', 'sample',
                                            str(self.input_folder / 'sample.csv')])
        self.assertEqual(self.client.query_table_entries('sample')['total'], 4)

    def test_upload_file_infer_schema(self):
        self.main_parser.parse_and_execute(['client', 'upload-file', '-c','sample',
                                            str(self.input_folder / 'sample.csv')])
        self.assertEqual(self.client.query_table_entries('sample')['total'], 4)

    def test_upload_file_use_schema(self):
        self.main_parser.parse_and_execute(['client', 'upload-file',
                                            f"-s={self.input_folder / 'sample_schema_altered.json'}",
                                            'sample', str(self.input_folder / 'sample.csv')])
        self.assertEqual(self.client.query_table_entries('sample')['total'], 4)