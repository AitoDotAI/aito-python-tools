import json
import os
from subprocess import Popen, PIPE

from aito.cli.main_parser_wrapper import MainParserWrapper
from aito.utils.aito_client import AitoClient
from tests.test_case import TestCaseCompare


class TestDatabaseCli(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass(test_path='cli/client')
        cls.input_folder = cls.input_folder.parent.parent / 'sample_invoice'
        cls.main_parser = MainParserWrapper()
        env_var = os.environ
        cls.client = AitoClient(env_var['AITO_INSTANCE_NAME'], env_var['AITO_RW_KEY'], env_var['AITO_RO_KEY'])

    def create_table(self):
        with (self.input_folder / "invoice_aito_schema.json").open() as f:
            table_schema = json.load(f)
        self.client.put_table_schema('invoice', table_schema)

    def setUp(self):
        super().setUp()
        self.client.delete_database()

    def test_upload_batch_no_table_schema(self):
        with self.assertRaises(Exception):
            self.main_parser.parse_and_execute(['database', 'upload-batch', 'invoice',
                                                str(self.input_folder / 'invoice.json')])

    def test_upload_batch_invalid_entries(self):
        self.create_table()
        with self.assertRaises(Exception):
            self.main_parser.parse_and_execute(['database', 'upload-batch', 'invoice',
                                                str(self.input_folder / 'invoice.ndjson')])

    def test_upload_batch(self):
        self.create_table()
        os.system(f"python -m aito.cli.main_parser_wrapper database upload-batch invoice "
                  f"{self.input_folder}/invoice.json")
        table_entries_result = self.client.query_table_entries('invoice')
        self.assertEqual(table_entries_result['total'], 4)
        self.assertCountEqual(table_entries_result['hits'],
                              json.load((self.input_folder / 'invoice_no_null_value.json').open()))

    def test_upload_file_table_not_exist(self):
        with self.assertRaises(SystemExit) as context:
            self.main_parser.parse_and_execute(['database', 'upload-file', 'invoice',
                                                str(self.input_folder / 'invoice.ndjson')])
        self.assertEqual(context.exception.code, 2)

    def test_upload_file_table_exist(self):
        self.create_table()
        os.system(f"python -m aito.cli.main_parser_wrapper database upload-file invoice "
                  f"{self.input_folder}/invoice.ndjson")
        table_entries_result = self.client.query_table_entries('invoice')
        self.assertEqual(table_entries_result['total'], 4)
        self.assertCountEqual(table_entries_result['hits'],
                              json.load((self.input_folder / 'invoice_no_null_value.json').open()))

    def test_upload_file_different_format(self):
        self.create_table()
        os.system(f"python -m aito.cli.main_parser_wrapper database upload-file -f csv invoice "
                  f"{self.input_folder}/invoice.csv")
        table_entries_result = self.client.query_table_entries('invoice')
        self.assertEqual(table_entries_result['total'], 4)
        self.assertCountEqual(table_entries_result['hits'],
                              json.load((self.input_folder / 'invoice_no_null_value.json').open()))

    def test_upload_file_infer_format(self):
        self.create_table()
        os.system(f"python -m aito.cli.main_parser_wrapper database upload-file invoice "
                  f"{self.input_folder}/invoice.csv")
        table_entries_result = self.client.query_table_entries('invoice')
        self.assertEqual(table_entries_result['total'], 4)
        self.assertCountEqual(table_entries_result['hits'],
                              json.load((self.input_folder / 'invoice_no_null_value.json').open()))

    def test_create_table(self):
        os.system(f"python -m aito.cli.main_parser_wrapper database create-table invoice < "
                  f"{self.input_folder / 'invoice_aito_schema.json'}")
        self.assertTrue(self.client.check_table_existed('invoice'))

    def test_delete_table(self):
        self.create_table()
        proc = Popen("python -m aito.cli.main_parser_wrapper database delete-table invoice",
                     stdin=PIPE, stdout=PIPE, shell=True)
        proc.communicate(b"y")
        self.assertFalse(self.client.check_table_existed('invoice'))

    def test_delete_database(self):
        self.create_table()
        with (self.input_folder / 'invoice_aito_schema_altered.json').open() as f:
            another_tbl_schema = json.load(f)
        self.client.put_table_schema('invoice_altered', another_tbl_schema)
        proc = Popen("python -m aito.cli.main_parser_wrapper database delete-database",
                     stdin=PIPE, stdout=PIPE, shell=True)
        proc.communicate(b"y")
        self.assertFalse(self.client.check_table_existed('invoice'))
        self.assertFalse(self.client.check_table_existed('invoice_altered'))

    def test_quick_add_table(self):
        os.system(f"python -m aito.cli.main_parser_wrapper database quick-add-table "
                  f"{self.input_folder / 'invoice.csv'}")
        table_entries_result = self.client.query_table_entries('invoice')
        self.assertEqual(table_entries_result['total'], 4)
        self.assertCountEqual(table_entries_result['hits'],
                              json.load((self.input_folder / 'invoice_no_null_value.json').open()))

    def test_quick_add_table_different_name(self):
        os.system(f"python -m aito.cli.main_parser_wrapper database quick-add-table -n newTable -f json "
                  f"{self.input_folder / 'invoice.json'}")
        table_entries_result = self.client.query_table_entries('newTable')
        self.assertEqual(table_entries_result['total'], 4)
        self.assertCountEqual(table_entries_result['hits'],
                              json.load((self.input_folder / 'invoice_no_null_value.json').open()))
