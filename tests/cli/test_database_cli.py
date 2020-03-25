import json
import os
import subprocess
import unittest
from pathlib import Path
from uuid import uuid4

from aito.cli.main_parser_wrapper import MainParserWrapper
from aito.utils.aito_client import AitoClient
from tests.cases import TestCaseCompare


class TestDatabaseCli(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass(test_path='cli/client')
        cls.input_folder = cls.input_folder.parent.parent / 'sample_invoice'
        cls.main_parser = MainParserWrapper()
        env_var = os.environ
        cls.client = AitoClient(env_var['AITO_INSTANCE_URL'], env_var['AITO_API_KEY'])
        with (cls.input_folder / "invoice_aito_schema.json").open() as f:
            cls.default_table_schema = json.load(f)
        cls.default_table_name = f"invoice_{uuid4()}"
        cls.prefix_args = ['python', '-m', 'aito.cli.main_parser_wrapper']
        if os.getenv('TEST_BUILT_PACKAGE'):
            cls.prefix_args = ['aito']

    def create_table(self):
        self.client.create_table(self.default_table_name, self.default_table_schema)

    def compare_table_entries_to_file_content(self, table_name: str, exp_file_path: Path, compare_order: bool = False):
        table_entries = self.client.query_entries(table_name)['hits']
        with exp_file_path.open() as exp_f:
            file_content = json.load(exp_f)
        if compare_order:
            self.assertEqual(table_entries, file_content)
        else:
            self.assertCountEqual(table_entries, file_content)

    def tearDown(self):
        super().tearDown()
        self.client.delete_table(self.default_table_name)

    def test_upload_batch_no_table_schema(self):
        with self.assertRaises(subprocess.CalledProcessError):
            subprocess.check_call(self.prefix_args + ['database', 'upload-batch', self.default_table_name,
                                                      str(self.input_folder / 'invoice.json')])

    def test_upload_batch_invalid_entries(self):
        self.create_table()
        with self.assertRaises(subprocess.CalledProcessError):
            subprocess.check_call(self.prefix_args + ['database', 'upload-batch', self.default_table_name,
                                                      str(self.input_folder / 'invoice.ndjson')])

    def test_upload_batch(self):
        self.create_table()
        subprocess.run(self.prefix_args + ['database', 'upload-batch', self.default_table_name,
                                           f'{self.input_folder}/invoice.json'])
        self.compare_table_entries_to_file_content(self.default_table_name,
                                                   self.input_folder / 'invoice_no_null_value.json')

    def test_upload_batch_stdin(self):
        self.create_table()
        with (self.input_folder / 'invoice.json').open() as in_f:
            subprocess.run(self.prefix_args + ['database', 'upload-batch', self.default_table_name], stdin=in_f)
        self.compare_table_entries_to_file_content(self.default_table_name,
                                                   self.input_folder / 'invoice_no_null_value.json')

    def test_upload_file_table_not_exist(self):
        with self.assertRaises(subprocess.CalledProcessError):
            subprocess.check_call(self.prefix_args + ['database', 'upload-file', self.default_table_name,
                                                      f'{self.input_folder}/invoice.ndjson'])

    def test_upload_file(self):
        self.create_table()
        subprocess.run(self.prefix_args + ['database', 'upload-file', self.default_table_name,
                                           f'{self.input_folder}/invoice.ndjson'])
        self.compare_table_entries_to_file_content(self.default_table_name,
                                                   self.input_folder / 'invoice_no_null_value.json')

    def test_upload_file_different_format(self):
        self.create_table()
        subprocess.run(self.prefix_args + ['database', 'upload-file', '-f', 'csv', self.default_table_name,
                                           f'{self.input_folder}/invoice.csv'])
        self.compare_table_entries_to_file_content(self.default_table_name,
                                                   self.input_folder / 'invoice_no_null_value.json')

    def test_upload_file_infer_format(self):
        self.create_table()
        subprocess.run(self.prefix_args + ['database', 'upload-file', self.default_table_name,
                                           f'{self.input_folder}/invoice.csv'])
        self.compare_table_entries_to_file_content(self.default_table_name,
                                                   self.input_folder / 'invoice_no_null_value.json')

    def test_create_table(self):
        subprocess.run(self.prefix_args + ['database', 'create-table', self.default_table_name,
                                           f'{self.input_folder}/invoice_aito_schema.json'])
        self.assertTrue(self.client.check_table_exists(self.default_table_name))

    def test_create_table_stdin(self):
        with (self.input_folder / 'invoice_aito_schema.json').open() as in_f:
            subprocess.run(self.prefix_args +['database', 'create-table', self.default_table_name], stdin=in_f)
        self.assertTrue(self.client.check_table_exists(self.default_table_name))

    def test_delete_table(self):
        self.create_table()
        proc = subprocess.Popen(self.prefix_args + ['database', 'delete-table', self.default_table_name],
                                stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        proc.communicate(b"yes")
        self.assertFalse(self.client.check_table_exists(self.default_table_name))

    def test_quick_add_table(self):
        input_file = self.input_folder / 'invoice.csv'
        default_table_name_file = self.input_folder / f"{self.default_table_name}.csv"
        import shutil
        shutil.copyfile(input_file, default_table_name_file)
        subprocess.run(self.prefix_args + ['database', 'quick-add-table', str(default_table_name_file)])
        default_table_name_file.unlink()
        self.compare_table_entries_to_file_content(self.default_table_name,
                                                   self.input_folder / 'invoice_no_null_value.json')

    def test_quick_add_table_different_name(self):
        subprocess.run(self.prefix_args + ['database', 'quick-add-table', '-n', self.default_table_name, '-f', 'json',
                                           str(self.input_folder / 'invoice.json')])
        self.compare_table_entries_to_file_content(self.default_table_name,
                                                   self.input_folder / 'invoice_no_null_value.json')

    @unittest.skipUnless(os.environ.get('RUN_DELETE_DATABASE_TEST'), "Avoid delete DB when running other tests")
    def test_delete_database(self):
        self.create_table()
        with (self.input_folder / 'invoice_aito_schema_altered.json').open() as f:
            another_tbl_schema = json.load(f)
        self.client.create_table('invoice_altered', another_tbl_schema)
        proc = subprocess.Popen(self.prefix_args + ['database', 'delete-database'],
                                stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        proc.communicate(b"y")
        self.assertFalse(self.client.check_table_exists(self.default_table_name))
        self.assertFalse(self.client.check_table_exists('invoice_altered'))
