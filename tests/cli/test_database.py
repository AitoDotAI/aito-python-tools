import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from aito.sdk.aito_client import AitoClient
from aito.sdk.aito_client import RequestError
from tests.cli.parser_and_cli_test_case import ParserAndCLITestCase


class TestDatabase(ParserAndCLITestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.input_folder = cls.input_folder.parent.parent / 'sample_invoice'
        cls.default_main_parser_args = {
            'command': 'database', 'verbose': False, 'version': False, 'quiet': False,
            'api_key': '.env', 'instance_url': '.env', 'use_env_file': None
        }
        cls.client = AitoClient(os.environ['AITO_INSTANCE_URL'], os.environ['AITO_API_KEY'])
        with (cls.input_folder / "invoice_aito_schema.json").open() as f:
            cls.default_table_schema = json.load(f)
        cls.default_table_name = f"invoice_{uuid4()}"

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
        expected_args = {
            'operation': 'upload-entries',
            'table-name': self.default_table_name,
            'input': self.input_folder / 'invoice.json',
            **self.default_main_parser_args
        }

        self.parse_and_execute(
            ['database', 'upload-entries', self.default_table_name, str(self.input_folder / 'invoice.json')],
            expected_args,
            execute_exception=RequestError
        )

    def test_upload_batch_invalid_entries(self):
        self.create_table()
        expected_args = {
            'operation': 'upload-entries',
            'table-name': self.default_table_name,
            'input': self.input_folder / 'invoice.ndjson',
            **self.default_main_parser_args
        }

        self.parse_and_execute(
            ['database', 'upload-entries', self.default_table_name, str(self.input_folder / 'invoice.ndjson')],
            expected_args,
            execute_exception=SystemExit
        )

    def test_upload_batch(self):
        self.create_table()
        expected_args = {
            'operation': 'upload-entries',
            'table-name': self.default_table_name,
            'input': self.input_folder / 'invoice.json',
            **self.default_main_parser_args
        }

        self.parse_and_execute(
            ['database', 'upload-entries', self.default_table_name, str(self.input_folder / 'invoice.json')],
            expected_args,
        )
        self.compare_table_entries_to_file_content(
            self.default_table_name, self.input_folder / 'invoice_no_null_value.json'
        )

    def test_upload_batch_stdin(self):
        self.create_table()
        expected_args = {
            'operation': 'upload-entries',
            'table-name': self.default_table_name,
            'input': sys.stdin,
            **self.default_main_parser_args
        }

        with (self.input_folder / 'invoice.json').open() as in_f:
            self.parse_and_execute(
                ['database', 'upload-entries', self.default_table_name],
                expected_args, stub_stdin=in_f
            )
        self.compare_table_entries_to_file_content(
            self.default_table_name, self.input_folder / 'invoice_no_null_value.json'
        )

    def test_upload_file_table_not_exist(self):
        expected_args = {
            'operation': 'upload-file',
            'table-name': self.default_table_name,
            'input-file': self.input_folder / 'invoice.ndjson',
            'file_format': 'infer',
            **self.default_main_parser_args
        }

        self.parse_and_execute(
            ['database', 'upload-file', self.default_table_name, str(self.input_folder / 'invoice.ndjson')],
            expected_args,
            execute_exception=RequestError
        )

    def test_upload_file(self):
        self.create_table()
        expected_args = {
            'operation': 'upload-file',
            'table-name': self.default_table_name,
            'input-file': self.input_folder / 'invoice.ndjson',
            'file_format': 'infer',
            **self.default_main_parser_args
        }
        self.parse_and_execute(
            ['database', 'upload-file', self.default_table_name, str(self.input_folder / 'invoice.ndjson')],
            expected_args
        )
        self.compare_table_entries_to_file_content(
            self.default_table_name, self.input_folder / 'invoice_no_null_value.json'
        )

    def test_upload_file_different_format(self):
        self.create_table()
        expected_args = {
            'operation': 'upload-file',
            'table-name': self.default_table_name,
            'input-file': self.input_folder / 'invoice.csv',
            'file_format': 'csv',
            **self.default_main_parser_args
        }
        self.parse_and_execute(
            ['database', 'upload-file', '-f', 'csv', self.default_table_name, str(self.input_folder / 'invoice.csv')],
            expected_args
        )
        self.compare_table_entries_to_file_content(
            self.default_table_name, self.input_folder / 'invoice_no_null_value.json'
        )

    def test_upload_file_infer_format(self):
        self.create_table()
        expected_args = {
            'operation': 'upload-file',
            'table-name': self.default_table_name,
            'input-file': self.input_folder / 'invoice.csv',
            'file_format': 'infer',
            **self.default_main_parser_args
        }
        self.parse_and_execute(
            ['database', 'upload-file', self.default_table_name, str(self.input_folder / 'invoice.csv')],
            expected_args
        )
        self.compare_table_entries_to_file_content(
            self.default_table_name, self.input_folder / 'invoice_no_null_value.json'
        )

    def test_create_table(self):
        expected_args = {
            'operation': 'create-table',
            'table-name': self.default_table_name,
            'input': self.input_folder / 'invoice_aito_schema.json',
            **self.default_main_parser_args
        }
        self.parse_and_execute(
            ['database', 'create-table', self.default_table_name, f'{self.input_folder}/invoice_aito_schema.json'],
            expected_args
        )
        self.assertTrue(self.client.check_table_exists(self.default_table_name))

    def test_create_table_stdin(self):
        expected_args = {
            'operation': 'create-table',
            'table-name': self.default_table_name,
            'input': sys.stdin,
            **self.default_main_parser_args
        }
        with (self.input_folder / 'invoice_aito_schema.json').open() as in_f:
            self.parse_and_execute(
                ['database', 'create-table', self.default_table_name], expected_args, stub_stdin=in_f
            )
        self.assertTrue(self.client.check_table_exists(self.default_table_name))

    def test_delete_table(self):
        self.create_table()
        expected_args = {
            'operation': 'delete-table',
            'table-name': self.default_table_name,
            **self.default_main_parser_args
        }
        # Manually run test built pacakge to communicate
        if os.environ.get('TEST_BUILT_PACKAGE'):
            import subprocess
            proc = subprocess.Popen(['aito', 'database', 'delete-table', self.default_table_name],
                                    stdin=subprocess.PIPE, stdout=subprocess.PIPE)
            proc.communicate(b"yes")
        else:
            with patch('builtins.input', return_value='yes'):
                self.parse_and_execute(['database', 'delete-table', self.default_table_name], expected_args)
        self.assertFalse(self.client.check_table_exists(self.default_table_name))

    def test_quick_add_table(self):
        self.maxDiff = None
        expected_args = {
            'operation': 'quick-add-table',
            'table_name': None,
            'file_format': 'infer',
            'input-file': self.input_folder / f'{self.default_table_name}.csv',
            **self.default_main_parser_args
        }
        # create a file with the same name as the default table name to test table name inference
        default_table_name_file = self.input_folder / f'{self.default_table_name}.csv'
        import shutil
        shutil.copyfile(self.input_folder / 'invoice.csv', default_table_name_file)
        self.parse_and_execute(
            ['database', 'quick-add-table', str(default_table_name_file)],
            expected_args
        )
        default_table_name_file.unlink()
        self.compare_table_entries_to_file_content(
            self.default_table_name, self.input_folder / 'invoice_no_null_value.json')

    def test_quick_add_table_different_name(self):
        expected_args = {
            'operation': 'quick-add-table',
            'table_name': self.default_table_name,
            'file_format': 'json',
            'input-file': self.input_folder / 'invoice.json',
            **self.default_main_parser_args
        }
        self.parse_and_execute(
            ['database', 'quick-add-table', '-n', self.default_table_name, '-f', 'json',
             str(self.input_folder / 'invoice.json')],
            expected_args
        )
        self.compare_table_entries_to_file_content(self.default_table_name,
                                                   self.input_folder / 'invoice_no_null_value.json')

    @unittest.skipUnless(os.environ.get('RUN_DELETE_DATABASE_TEST'), "Avoid delete DB when running other tests")
    def test_delete_database(self):
        self.create_table()
        with (self.input_folder / 'invoice_aito_schema_altered.json').open() as f:
            another_tbl_schema = json.load(f)
        self.client.create_table('invoice_altered', another_tbl_schema)

        expected_args = {
            'operation': 'delete-database',
            **self.default_main_parser_args
        }
        with patch('builtins.input', return_value='yes'):
            self.parse_and_execute(['database', 'delete-database'], expected_args)
        self.assertFalse(self.client.check_table_exists(self.default_table_name))
        self.assertFalse(self.client.check_table_exists('invoice_altered'))
