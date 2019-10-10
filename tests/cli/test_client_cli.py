import json
import os

from aito.cli.main_parser import MainParserWrapper
from aito.client.aito_client import AitoClient
from tests.test_case import TestCaseCompare


class TestClientParser(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass(test_path='cli/client')
        cls.input_folder = cls.input_folder.parent.parent / 'schema'
        cls.main_parser = MainParserWrapper()
        env_var = os.environ
        cls.client = AitoClient(env_var['AITO_INSTANCE_URL'], env_var['AITO_RW_KEY'], env_var['AITO_RO_KEY'])

    def create_table(self):
        with (self.input_folder / "sample_schema.json").open() as f:
            table_schema = json.load(f)
        self.client.put_table_schema('sample', table_schema)

    def setUp(self):
        super().setUp()
        self.client.delete_database()

    def test_upload_batch_no_table_schema(self):
        with self.assertRaises(ValueError):
            self.main_parser.parse_and_execute(['client', 'upload-batch', 'sample',
                                                str(self.input_folder / 'sample.json')])

    def test_upload_batch_invalid_entries(self):
        self.create_table()
        with self.assertRaises(Exception):
            self.main_parser.parse_and_execute(['client', 'upload-batch', 'sample',
                                                str(self.input_folder / 'sample.ndjson')])

    def test_upload_batch(self):
        self.create_table()
        os.system(f"python aito.py client upload-batch sample {self.input_folder}/sample.json")
        self.assertEqual(self.client.query_table_entries('sample')['total'], 4)

    def test_upload_file_no_table_schema(self):
        with self.assertRaises(ValueError):
            self.main_parser.parse_and_execute(['client', 'upload-file', 'sample',
                                                str(self.input_folder / 'sample.ndjson')])

    def test_upload_file_with_table_schema(self):
        self.create_table()
        os.system(f"python aito.py client upload-file sample {self.input_folder}/sample.ndjson")
        self.assertEqual(self.client.query_table_entries('sample')['total'], 4)

    def test_upload_file_different_format(self):
        self.create_table()
        os.system(f"python aito.py client upload-file -f csv sample {self.input_folder}/sample.csv")
        self.assertEqual(self.client.query_table_entries('sample')['total'], 4)

    def test_upload_file_infer_format(self):
        self.create_table()
        os.system(f"python aito.py client upload-file sample {self.input_folder}/sample.csv")
        self.assertEqual(self.client.query_table_entries('sample')['total'], 4)