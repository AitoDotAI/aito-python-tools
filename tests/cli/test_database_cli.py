import json
import os
import unittest
from subprocess import Popen, PIPE

from aito.cli.main_parser_wrapper import MainParserWrapper
from aito.utils.aito_client import AitoClient
from tests.test_case import TestCaseCompare
from uuid import uuid4


class TestDatabaseCli(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass(test_path='cli/client')
        cls.input_folder = cls.input_folder.parent.parent / 'sample_invoice'
        cls.main_parser = MainParserWrapper()
        env_var = os.environ
        cls.client = AitoClient(env_var['AITO_INSTANCE_NAME'], env_var['AITO_API_KEY'])
        with (cls.input_folder / "invoice_aito_schema.json").open() as f:
            cls.default_table_schema = json.load(f)
        cls.default_table_name = f"invoice_{uuid4()}"

    def create_table(self):
        self.client.put_table_schema(self.default_table_name, self.default_table_schema)

    def setUp(self):
        super().setUp()
        self.client.delete_table(self.default_table_name)

    def test_upload_batch_no_table_schema(self):
        with self.assertRaises(Exception):
            self.main_parser.parse_and_execute(['database', 'upload-batch', self.default_table_name,
                                                str(self.input_folder / 'invoice.json')])

    def test_upload_batch_invalid_entries(self):
        self.create_table()
        with self.assertRaises(Exception):
            self.main_parser.parse_and_execute(['database', 'upload-batch', self.default_table_name,
                                                str(self.input_folder / 'invoice.ndjson')])

    def test_upload_batch(self):
        self.create_table()
        os.system(f"python -m aito.cli.main_parser_wrapper database upload-batch {self.default_table_name} "
                  f"{self.input_folder}/invoice.json")
        table_entries_result = self.client.query_table_entries(self.default_table_name)
        self.assertEqual(table_entries_result['total'], 4)
        with (self.input_folder / 'invoice_no_null_value.json').open() as exp_f:
            self.assertCountEqual(table_entries_result['hits'],json.load(exp_f))

    def test_upload_file_table_not_exist(self):
        with self.assertRaises(SystemExit) as context:
            self.main_parser.parse_and_execute(['database', 'upload-file', self.default_table_name,
                                                str(self.input_folder / 'invoice.ndjson')])
        self.assertEqual(context.exception.code, 2)

    def test_upload_file_table_exist(self):
        self.create_table()
        os.system(f"python -m aito.cli.main_parser_wrapper database upload-file {self.default_table_name} "
                  f"{self.input_folder}/invoice.ndjson")
        table_entries_result = self.client.query_table_entries(self.default_table_name)
        self.assertEqual(table_entries_result['total'], 4)
        with (self.input_folder / 'invoice_no_null_value.json').open() as exp_f:
            self.assertCountEqual(table_entries_result['hits'], json.load(exp_f))

    def test_upload_file_different_format(self):
        self.create_table()
        os.system(f"python -m aito.cli.main_parser_wrapper database upload-file -f csv {self.default_table_name} "
                  f"{self.input_folder}/invoice.csv")
        table_entries_result = self.client.query_table_entries(self.default_table_name)
        self.assertEqual(table_entries_result['total'], 4)
        with (self.input_folder / 'invoice_no_null_value.json').open() as exp_f:
            self.assertCountEqual(table_entries_result['hits'], json.load(exp_f))

    def test_upload_file_infer_format(self):
        self.create_table()
        os.system(f"python -m aito.cli.main_parser_wrapper database upload-file {self.default_table_name} "
                  f"{self.input_folder}/invoice.csv")
        table_entries_result = self.client.query_table_entries(self.default_table_name)
        self.assertEqual(table_entries_result['total'], 4)
        with (self.input_folder / 'invoice_no_null_value.json').open() as exp_f:
            self.assertCountEqual(table_entries_result['hits'], json.load(exp_f))

    def test_create_table(self):
        os.system(f"python -m aito.cli.main_parser_wrapper database create-table {self.default_table_name} < "
                  f"{self.input_folder / 'invoice_aito_schema.json'}")
        self.assertTrue(self.client.check_table_existed(self.default_table_name))

    def test_delete_table(self):
        self.create_table()
        proc = Popen(f"python -m aito.cli.main_parser_wrapper database delete-table {self.default_table_name}",
                     stdin=PIPE, stdout=PIPE, shell=True)
        proc.communicate(b"y")
        self.assertFalse(self.client.check_table_existed(self.default_table_name))

    def test_quick_add_table(self):
        input_file = self.input_folder / 'invoice.csv'
        renamed_file = self.input_folder / f"{self.default_table_name}.csv"
        import shutil
        shutil.copyfile(input_file, renamed_file)
        os.system(f"python -m aito.cli.main_parser_wrapper database quick-add-table {renamed_file}")
        renamed_file.unlink()
        table_entries_result = self.client.query_table_entries(self.default_table_name)
        self.assertEqual(table_entries_result['total'], 4)
        with (self.input_folder / 'invoice_no_null_value.json').open() as exp_f:
            self.assertCountEqual(table_entries_result['hits'], json.load(exp_f))

    def test_quick_add_table_different_name(self):
        new_id = uuid4()
        os.system(f"python -m aito.cli.main_parser_wrapper database quick-add-table -n {new_id} "
                  f"-f json {self.input_folder / 'invoice.json'}")
        table_entries_result = self.client.query_table_entries(str(new_id))
        self.assertEqual(table_entries_result['total'], 4)
        with (self.input_folder / 'invoice_no_null_value.json').open() as exp_f:
            self.assertCountEqual(table_entries_result['hits'], json.load(exp_f))

    @unittest.skipUnless(os.environ.get('RUN_SKIP_TEST'), "Avoid delete DB when running sql functions test")
    def test_delete_database(self):
        self.create_table()
        with (self.input_folder / 'invoice_aito_schema_altered.json').open() as f:
            another_tbl_schema = json.load(f)
        self.client.put_table_schema('invoice_altered', another_tbl_schema)
        proc = Popen("python -m aito.cli.main_parser_wrapper database delete-database",
                     stdin=PIPE, stdout=PIPE, shell=True)
        proc.communicate(b"y")
        self.assertFalse(self.client.check_table_existed(self.default_table_name))
        self.assertFalse(self.client.check_table_existed('invoice_altered'))
