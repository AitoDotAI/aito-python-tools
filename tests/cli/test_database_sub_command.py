import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4
import subprocess
import shutil

from aito.cli.database_parser import DatabaseParser
from aito.client import AitoClient
from aito.client import RequestError, BaseError
from tests.cli.parser_and_cli_test_case import ParserAndCLITestCase


class TestMainParserDatabaseSubCommand(ParserAndCLITestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.input_folder = cls.input_folder.parent.parent / 'sample_invoice'
        cls.default_parser_args = {
            'command': 'database', 'verbose': False, 'version': False, 'quiet': False,
            'api_key': '.env', 'instance_url': '.env', 'use_env_file': None
        }
        # hacky way to remove this for aitodb test
        cls.command = ['database']
        cls.client = AitoClient(os.environ['AITO_INSTANCE_URL'], os.environ['AITO_API_KEY'])
        with (cls.input_folder / "invoice_aito_schema.json").open() as f:
            cls.default_table_schema = json.load(f)
        cls.default_table_name = f"invoice_{str(uuid4()).replace('-', '_')}"

    def setUp(self):
        super().setUp()
        self.expected_args = self.default_parser_args.copy()

    def create_table(self):
        self.client.create_table(self.default_table_name, self.default_table_schema)

    def tearDown(self):
        super().tearDown()
        try:
            self.client.delete_table(self.default_table_name)
        except Exception as e:
            self.logger.error(f"failed to delete table in tearDown: {e}")

    def compare_table_entries_to_file_content(self, table_name: str, exp_file_path: Path, compare_order: bool = False):
        table_entries = self.client.query_entries(table_name)
        with exp_file_path.open() as exp_f:
            file_content = json.load(exp_f)
        if compare_order:
            self.assertEqual(table_entries, file_content)
        else:
            self.assertCountEqual(table_entries, file_content)

    def test_upload_batch_no_table_schema(self):
        self.expected_args.update({
            'operation': 'upload-entries',
            'table-name': self.default_table_name,
            'input': self.input_folder / 'invoice.json',
        })

        self.parse_and_execute(
            self.command + ['upload-entries', self.default_table_name, str(self.input_folder / 'invoice.json')],
            self.expected_args,
            execute_exception=BaseError
        )

    def test_upload_batch_invalid_entries(self):
        self.create_table()
        self.expected_args.update({
            'operation': 'upload-entries',
            'table-name': self.default_table_name,
            'input': self.input_folder / 'invoice.ndjson',
        })

        self.parse_and_execute(
            self.command + ['upload-entries', self.default_table_name, str(self.input_folder / 'invoice.ndjson')],
            self.expected_args,
            execute_exception=SystemExit
        )

    def test_upload_batch(self):
        self.create_table()
        self.expected_args.update({
            'operation': 'upload-entries',
            'table-name': self.default_table_name,
            'input': self.input_folder / 'invoice.json',
        })

        self.parse_and_execute(
            self.command + ['upload-entries', self.default_table_name, str(self.input_folder / 'invoice.json')],
            self.expected_args,
        )
        self.compare_table_entries_to_file_content(
            self.default_table_name, self.input_folder / 'invoice_no_null_value.json'
        )

    def test_upload_batch_stdin(self):
        self.create_table()
        self.expected_args.update({
            'operation': 'upload-entries',
            'table-name': self.default_table_name,
            'input': sys.stdin,
        })

        with (self.input_folder / 'invoice.json').open() as in_f:
            self.parse_and_execute(
                self.command + ['upload-entries', self.default_table_name], self.expected_args, stub_stdin=in_f
            )
        self.compare_table_entries_to_file_content(
            self.default_table_name, self.input_folder / 'invoice_no_null_value.json'
        )

    def test_upload_file_table_not_exist(self):
        self.expected_args.update({
            'operation': 'upload-file',
            'table-name': self.default_table_name,
            'input-file': self.input_folder / 'invoice.ndjson',
            'file_format': 'infer',
        })

        self.parse_and_execute(
            self.command + ['upload-file', self.default_table_name, str(self.input_folder / 'invoice.ndjson')],
            self.expected_args,
            execute_exception=RequestError
        )

    def test_upload_file(self):
        self.create_table()
        self.expected_args.update({
            'operation': 'upload-file',
            'table-name': self.default_table_name,
            'input-file': self.input_folder / 'invoice.ndjson',
            'file_format': 'infer',
        })
        self.parse_and_execute(
            self.command + ['upload-file', self.default_table_name, str(self.input_folder / 'invoice.ndjson')],
            self.expected_args
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
            **self.default_parser_args
        }
        self.parse_and_execute(
            self.command + ['upload-file', '-f', 'csv', self.default_table_name, str(self.input_folder / 'invoice.csv')],
            expected_args
        )
        self.compare_table_entries_to_file_content(
            self.default_table_name, self.input_folder / 'invoice_no_null_value.json'
        )

    def test_upload_file_infer_format(self):
        self.create_table()
        self.expected_args.update({
            'operation': 'upload-file',
            'table-name': self.default_table_name,
            'input-file': self.input_folder / 'invoice.csv',
            'file_format': 'infer',
        })
        self.parse_and_execute(
            self.command + ['upload-file', self.default_table_name, str(self.input_folder / 'invoice.csv')],
            self.expected_args
        )
        self.compare_table_entries_to_file_content(
            self.default_table_name, self.input_folder / 'invoice_no_null_value.json'
        )

    def test_create_table(self):
        self.expected_args.update({
            'operation': 'create-table',
            'table-name': self.default_table_name,
            'input': self.input_folder / 'invoice_aito_schema.json',
        })
        self.parse_and_execute(
            self.command + ['create-table', self.default_table_name, f'{self.input_folder}/invoice_aito_schema.json'],
            self.expected_args
        )
        self.assertTrue(self.client.check_table_exists(self.default_table_name))

    def test_create_table_stdin(self):
        expected_args = {
            'operation': 'create-table',
            'table-name': self.default_table_name,
            'input': sys.stdin,
            **self.default_parser_args
        }
        with (self.input_folder / 'invoice_aito_schema.json').open() as in_f:
            self.parse_and_execute(
                self.command + ['create-table', self.default_table_name], expected_args, stub_stdin=in_f
            )
        self.assertTrue(self.client.check_table_exists(self.default_table_name))

    def test_delete_table(self):
        self.create_table()
        self.expected_args.update({
            'operation': 'delete-table',
            'table-name': self.default_table_name,
        })
        # Manually run test built package to communicate
        if os.environ.get('TEST_BUILT_PACKAGE'):
            proc = subprocess.Popen([self.program_name] + self.command + ['delete-table', self.default_table_name],
                                    stdin=subprocess.PIPE, stdout=subprocess.PIPE)
            proc.communicate(b"yes")
        else:
            with patch('builtins.input', return_value='yes'):
                self.parse_and_execute(self.command + ['delete-table', self.default_table_name], self.expected_args)
        self.assertFalse(self.client.check_table_exists(self.default_table_name))

    def test_quick_add_table(self):
        self.expected_args.update({
            'operation': 'quick-add-table',
            'table_name': None,
            'file_format': 'infer',
            'input-file': self.input_folder / f'{self.default_table_name}.csv'
        })
        # create a file with the same name as the default table name to test table name inference
        default_table_name_file = self.input_folder / f'{self.default_table_name}.csv'
        shutil.copyfile(self.input_folder / 'invoice.csv', default_table_name_file)
        self.parse_and_execute(
            self.command + ['quick-add-table', str(default_table_name_file)], self.expected_args
        )
        default_table_name_file.unlink()
        self.compare_table_entries_to_file_content(
            self.default_table_name, self.input_folder / 'invoice_no_null_value.json')

    def test_quick_add_table_different_name(self):
        self.expected_args.update({
            'operation': 'quick-add-table',
            'table_name': self.default_table_name,
            'file_format': 'json',
            'input-file': self.input_folder / 'invoice.json',
        })
        self.parse_and_execute(
            self.command + ['quick-add-table', '-n', self.default_table_name, '-f', 'json',
             str(self.input_folder / 'invoice.json')],
            self.expected_args
        )
        self.compare_table_entries_to_file_content(
            self.default_table_name, self.input_folder / 'invoice_no_null_value.json'
        )

    def test_copy_table(self):
        self.create_table()
        copy_table_name = f'{self.default_table_name}_copy'
        self.expected_args.update({
            'operation': 'copy-table',
            'table-name': self.default_table_name,
            'copy-table-name': copy_table_name,
            'replace': False
        })
        self.parse_and_execute(
            self.command + ['copy-table', self.default_table_name, copy_table_name],
            self.expected_args
        )
        db_tables = self.client.get_existing_tables()
        self.assertIn(self.default_table_name, db_tables)
        self.assertIn(copy_table_name, db_tables)
        self.client.delete_table(copy_table_name)

    def test_rename_table(self):
        self.create_table()
        rename_table_name = f'{self.default_table_name}_rename'
        self.expected_args.update({
            'operation': 'rename-table',
            'old-name': self.default_table_name,
            'new-name': rename_table_name,
            'replace': False
        })
        self.parse_and_execute(
            self.command + ['rename-table', self.default_table_name, rename_table_name],
            self.expected_args
        )
        db_tables = self.client.get_existing_tables()
        self.assertIn(rename_table_name, db_tables)
        self.assertNotIn(self.default_table_name, db_tables)
        self.client.delete_table(rename_table_name)

    def test_show_tables(self):
        self.create_table()
        self.expected_args.update({
            'operation': 'show-tables',
        })
        with self.out_file_path.open('w') as out_f:
            self.parse_and_execute(self.command + ['show-tables'], self.expected_args, stub_stdout=out_f)

        with self.out_file_path.open() as in_f:
            content = in_f.read()

        listed_tables = content.splitlines()

        self.assertIn(self.default_table_name, listed_tables)

    @unittest.skipUnless(os.environ.get('RUN_DELETE_DATABASE_TEST'), "Avoid delete DB when running other tests")
    def test_delete_database(self):
        self.create_table()
        with (self.input_folder / 'invoice_aito_schema_altered.json').open() as f:
            another_tbl_schema = json.load(f)
        self.client.create_table('invoice_altered', another_tbl_schema)

        self.expected_args = {'operation': 'delete-database'}
        with patch('builtins.input', return_value='yes'):
            self.parse_and_execute(self.command + ['delete-database'], self.expected_args)
        self.assertFalse(self.client.check_table_exists(self.default_table_name))
        self.assertFalse(self.client.check_table_exists('invoice_altered'))

    @patch('builtins.input', return_value='yes')
    def test_login(self, mock_input):
        self.expected_args.update({
            'operation': 'login',
        })
        instance_url = os.environ['AITO_INSTANCE_URL']
        api_key = os.environ['AITO_API_KEY']
        if os.environ.get('TEST_BUILT_PACKAGE'):
            proc = subprocess.Popen(
                [self.program_name] + self.command + ['login'], stdin=subprocess.PIPE, stdout=subprocess.PIPE
            )
            proc.stdin.write(b"yes")
            proc.stdin.write(instance_url.encode())
            proc.stdin.write(api_key.encode())
        else:
            with patch('getpass.getpass', side_effect=[instance_url, api_key]):
                self.parse_and_execute(self.command + ['login'], self.expected_args)
        self.assertEqual(os.environ['AITO_INSTANCE_URL'], instance_url)
        self.assertEqual(os.environ['AITO_API_KEY'], api_key)


class TestDatabaseParser(TestMainParserDatabaseSubCommand):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.parser = DatabaseParser()
        cls.program_name = 'aitodb'
        cls.default_parser_args.pop('command')
        cls.command = []
