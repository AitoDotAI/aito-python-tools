import json
import os
import shutil
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from parameterized import parameterized

from aito.api import create_table, delete_table, get_existing_tables, check_table_exists, query_entries, upload_entries
from aito.cli.parser import get_credentials_file_config
from aito.client import RequestError
from aito.schema import AitoTableSchema, AitoDatabaseSchema
from tests.cli.parser_and_cli_test_case import ParserAndCLITestCase
from tests.sdk.contexts import default_client


class TestDatabaseSubCommands(ParserAndCLITestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.input_folder = cls.input_folder.parent.parent / 'sample_invoice'
        cls.default_parser_args = {
            'verbose': False, 'version': False, 'quiet': False,
            'profile': 'default', 'api_key': '.env', 'instance_url': '.env'
        }
        cls.client = default_client()
        with (cls.input_folder / "invoice_aito_schema.json").open() as f:
            json_schema = json.load(f)
        cls.default_table_schema = AitoTableSchema.from_deserialized_object(json_schema)
        cls.default_table_name = f"invoice_{str(uuid4()).replace('-', '_')}"
        with (cls.input_folder / "invoice_no_null_value.json").open() as f:
            cls.default_entries = json.load(f)

    def setUp(self):
        super().setUp()

        def _default_clean_up():
            try:
                delete_table(self.client, self.default_table_name)
            except Exception as e:
                self.logger.error(f"failed to delete table in cleanup: {e}")
        self.addCleanup(_default_clean_up)

    def create_table(self):
        create_table(self.client, self.default_table_name, self.default_table_schema)

    def upload_entries_to_table(self):
        upload_entries(self.client, self.default_table_name, self.default_entries)

    def compare_table_entries_to_file_content(self, table_name: str, exp_file_path: Path, compare_order: bool = False):
        table_entries = query_entries(self.client, table_name)
        with exp_file_path.open() as exp_f:
            file_content = json.load(exp_f)
        if compare_order:
            self.assertEqual(table_entries, file_content)
        else:
            self.assertCountEqual(table_entries, file_content)

    def test_upload_entries_no_table_schema(self):
        expected_args = {
            'command': 'upload-entries',
            'table-name': self.default_table_name,
            'input': self.input_folder / 'invoice.json',
            **self.default_parser_args
        }

        self.parse_and_execute(
            ['upload-entries', self.default_table_name, str(self.input_folder / 'invoice.json')],
            expected_args,
            execute_exception=Exception
        )

    def test_upload_entries_invalid_entries(self):
        self.create_table()
        expected_args = {
            'command': 'upload-entries',
            'table-name': self.default_table_name,
            'input': self.input_folder / 'invoice.ndjson',
            **self.default_parser_args
        }

        self.parse_and_execute(
            ['upload-entries', self.default_table_name, str(self.input_folder / 'invoice.ndjson')],
            expected_args,
            execute_exception=SystemExit
        )

    def test_upload_entries(self):
        self.create_table()
        expected_args = {
            'command': 'upload-entries',
            'table-name': self.default_table_name,
            'input': self.input_folder / 'invoice.json',
            **self.default_parser_args
        }

        self.parse_and_execute(
            ['upload-entries', self.default_table_name, str(self.input_folder / 'invoice.json')],
            expected_args,
        )
        self.compare_table_entries_to_file_content(
            self.default_table_name, self.input_folder / 'invoice_no_null_value.json'
        )

    def test_upload_entries_stdin(self):
        self.create_table()
        expected_args = {
            'command': 'upload-entries',
            'table-name': self.default_table_name,
            'input': sys.stdin,
            **self.default_parser_args
        }

        with (self.input_folder / 'invoice.json').open() as in_f:
            self.parse_and_execute(
                ['upload-entries', self.default_table_name], expected_args, stub_stdin=in_f
            )
        self.compare_table_entries_to_file_content(
            self.default_table_name, self.input_folder / 'invoice_no_null_value.json'
        )

    def test_upload_file_table_not_exist(self):
        expected_args = {
            'command': 'upload-file',
            'table-name': self.default_table_name,
            'input-file': self.input_folder / 'invoice.ndjson',
            'file_format': 'infer',
            **self.default_parser_args
        }

        self.parse_and_execute(
            ['upload-file', self.default_table_name, str(self.input_folder / 'invoice.ndjson')],
            expected_args,
            execute_exception=RequestError
        )

    def test_upload_file(self):
        self.create_table()
        expected_args= {
            'command': 'upload-file',
            'table-name': self.default_table_name,
            'input-file': self.input_folder / 'invoice.ndjson',
            'file_format': 'infer',
            **self.default_parser_args
        }
        self.parse_and_execute(
            ['upload-file', self.default_table_name, str(self.input_folder / 'invoice.ndjson')],
            expected_args
        )
        self.compare_table_entries_to_file_content(
            self.default_table_name, self.input_folder / 'invoice_no_null_value.json'
        )

    def test_upload_file_different_format(self):
        self.create_table()
        expected_args = {
            'command': 'upload-file',
            'table-name': self.default_table_name,
            'input-file': self.input_folder / 'invoice.csv',
            'file_format': 'csv',
            **self.default_parser_args
        }
        self.parse_and_execute(
            ['upload-file', '-f', 'csv', self.default_table_name, str(self.input_folder / 'invoice.csv')],
            expected_args
        )
        self.compare_table_entries_to_file_content(
            self.default_table_name, self.input_folder / 'invoice_no_null_value.json'
        )

    def test_upload_file_infer_format(self):
        self.create_table()
        expected_args = {
            'command': 'upload-file',
            'table-name': self.default_table_name,
            'input-file': self.input_folder / 'invoice.csv',
            'file_format': 'infer',
            **self.default_parser_args
        }
        self.parse_and_execute(
            ['upload-file', self.default_table_name, str(self.input_folder / 'invoice.csv')],
            expected_args
        )
        self.compare_table_entries_to_file_content(
            self.default_table_name, self.input_folder / 'invoice_no_null_value.json'
        )

    @unittest.skipUnless(os.environ.get('RUN_DELETE_DATABASE_TEST'), "Avoid create DB when running other tests")
    def test_create_database(self):
        database_schema = {'schema': {self.default_table_name: self.default_table_schema.to_json_serializable()}}
        database_schema_fp = self.output_folder / 'database_schema.json'
        with database_schema_fp.open('w') as f:
            json.dump(database_schema, f)

        self.addCleanup(database_schema_fp.unlink)

        expected_args = {
            'command': 'create-database',
            'input': database_schema_fp,
            **self.default_parser_args
        }
        self.parse_and_execute(['create-database', str(database_schema_fp)], expected_args)
        self.assertTrue(check_table_exists(self.client, self.default_table_name))

    def test_create_table(self):
        expected_args = {
            'command': 'create-table',
            'table-name': self.default_table_name,
            'input': self.input_folder / 'invoice_aito_schema.json',
            **self.default_parser_args
        }
        self.parse_and_execute(
            ['create-table', self.default_table_name, f'{self.input_folder}/invoice_aito_schema.json'],
            expected_args
        )
        self.assertTrue(check_table_exists(self.client, self.default_table_name))

    def test_create_table_stdin(self):
        expected_args = {
            'command': 'create-table',
            'table-name': self.default_table_name,
            'input': sys.stdin,
            **self.default_parser_args
        }
        with (self.input_folder / 'invoice_aito_schema.json').open() as in_f:
            self.parse_and_execute(
                ['create-table', self.default_table_name], expected_args, stub_stdin=in_f
            )
        self.assertTrue(check_table_exists(self.client, self.default_table_name))

    def test_get_table(self):
        self.create_table()
        expected_args = {
            'command': 'get-table',
            'table-name': self.default_table_name,
            **self.default_parser_args
        }
        with self.out_file_path.open('w') as out_f:
            self.parse_and_execute(['get-table', self.default_table_name], expected_args, stub_stdout=out_f)

        with self.out_file_path.open() as f:
            returned_content = json.load(f)
        returned_table_schema = AitoTableSchema.from_deserialized_object(returned_content)
        self.assertEqual(returned_table_schema, self.default_table_schema)

    def test_delete_table(self):
        self.create_table()
        expected_args = {
            'command': 'delete-table',
            'table-name': self.default_table_name,
            **self.default_parser_args
        }
        # Manually run test built package to communicate
        if os.environ.get('TEST_BUILT_PACKAGE'):
            proc = subprocess.Popen([self.program_name, 'delete-table', self.default_table_name],
                                    stdin=subprocess.PIPE, stdout=subprocess.PIPE)
            proc.communicate(b"yes")
        else:
            with patch('builtins.input', return_value='yes'):
                self.parse_and_execute(['delete-table', self.default_table_name], expected_args)
        self.assertFalse(check_table_exists(self.client, self.default_table_name))

    def test_quick_add_table(self):
        expected_args = {
            'command': 'quick-add-table',
            'table_name': None,
            'file_format': 'infer',
            'input-file': self.input_folder / f'{self.default_table_name}.csv',
            **self.default_parser_args
        }
        # create a file with the same name as the default table name to test table name inference
        default_table_name_file = self.input_folder / f'{self.default_table_name}.csv'
        shutil.copyfile(self.input_folder / 'invoice.csv', default_table_name_file)
        self.parse_and_execute(
            ['quick-add-table', str(default_table_name_file)], expected_args
        )
        default_table_name_file.unlink()
        self.compare_table_entries_to_file_content(
            self.default_table_name, self.input_folder / 'invoice_no_null_value.json')

    def test_quick_add_table_different_name(self):
        expected_args = {
            'command': 'quick-add-table',
            'table_name': self.default_table_name,
            'file_format': 'json',
            'input-file': self.input_folder / 'invoice.json',
            **self.default_parser_args
        }
        self.parse_and_execute(
            ['quick-add-table', '-n', self.default_table_name, '-f', 'json',
             str(self.input_folder / 'invoice.json')],
            expected_args
        )
        self.compare_table_entries_to_file_content(
            self.default_table_name, self.input_folder / 'invoice_no_null_value.json'
        )

    def test_copy_table(self):
        copy_table_name = f'{self.default_table_name}_copy'

        def clean_up():
            delete_table(self.client, copy_table_name)
        self.addCleanup(clean_up)

        self.create_table()

        expected_args = {
            'command': 'copy-table',
            'table-name': self.default_table_name,
            'copy-table-name': copy_table_name,
            'replace': False,
            **self.default_parser_args
        }
        self.parse_and_execute(
            ['copy-table', self.default_table_name, copy_table_name], expected_args
        )
        db_tables = get_existing_tables(self.client)
        self.assertIn(self.default_table_name, db_tables)
        self.assertIn(copy_table_name, db_tables)

    def test_rename_table(self):
        rename_table_name = f'{self.default_table_name}_rename'

        def clean_up():
            delete_table(self.client, rename_table_name)
        self.addCleanup(clean_up)

        self.create_table()

        expected_args = {
            'command': 'rename-table',
            'old-name': self.default_table_name,
            'new-name': rename_table_name,
            'replace': False,
            **self.default_parser_args
        }
        self.parse_and_execute(
            ['rename-table', self.default_table_name, rename_table_name], expected_args
        )
        db_tables = get_existing_tables(self.client)
        self.assertIn(rename_table_name, db_tables)
        self.assertNotIn(self.default_table_name, db_tables)

    def test_show_tables(self):
        self.addCleanup(self.delete_out_file)
        self.create_table()
        expected_args = {'command': 'show-tables', **self.default_parser_args}
        with self.out_file_path.open('w') as out_f:
            self.parse_and_execute(['show-tables'], expected_args, stub_stdout=out_f)

        with self.out_file_path.open() as in_f:
            content = in_f.read()

        listed_tables = content.splitlines()

        self.assertIn(self.default_table_name, listed_tables)

    @unittest.skipUnless(os.environ.get('RUN_DELETE_DATABASE_TEST'), "Avoid delete DB when running other tests")
    def test_delete_database(self):
        self.create_table()
        with (self.input_folder / 'invoice_aito_schema_altered.json').open() as f:
            another_tbl_schema = json.load(f)
        create_table(self.client, 'invoice_altered', another_tbl_schema)

        expected_args = {'command': 'delete-database', **self.default_parser_args}
        with patch('builtins.input', return_value='yes'):
            self.parse_and_execute(['delete-database'], expected_args)
        self.assertFalse(check_table_exists(self.client, self.default_table_name))
        self.assertFalse(check_table_exists(self.client, 'invoice_altered'))

    def test_get_database(self):
        self.create_table()
        expected_args = {
            'command': 'get-database',
            **self.default_parser_args
        }
        with self.out_file_path.open('w') as out_f:
            self.parse_and_execute(['get-database'], expected_args, stub_stdout=out_f)

        with self.out_file_path.open() as f:
            returned_content = json.load(f)
        returned_table_schema = AitoDatabaseSchema.from_deserialized_object(returned_content)
        self.assertIn(self.default_table_name, returned_table_schema.tables)
        self.assertEqual(returned_table_schema[self.default_table_name], self.default_table_schema)

    def test_configure_default_profile(self):
        # TODO: Test built package: communicate with getpass and stub existing credentials file
        if os.environ.get('TEST_BUILT_PACKAGE'):
            return

        self.addCleanup(self.delete_out_file)

        configure_args = {
            'command': 'configure', 'verbose': False, 'version': False, 'quiet': False, 'profile': 'default'
        }
        instance_url = os.environ['AITO_INSTANCE_URL']
        api_key = os.environ['AITO_API_KEY']

        with patch('aito.cli.parser.DEFAULT_CREDENTIAL_FILE', self.out_file_path):
            with patch('builtins.input', side_effect=[instance_url, api_key]):
                self.parse_and_execute(['configure'], configure_args)

            config = get_credentials_file_config()
            self.assertEqual(config.get('default', 'instance_url'), instance_url)
            self.assertEqual(config.get('default', 'api_key'), api_key)

            # remove env to trigger use default profile
            self.stub_environment_variable('AITO_INSTANCE_URL', None)
            self.stub_environment_variable('AITO_API_KEY', None)
            self.create_table()
            with (self.output_folder / 'show_table_result_out.txt').open('w') as out_f:
                self.parse_and_execute(
                    ['show-tables'],
                    {'command': 'show-tables', **self.default_parser_args},
                    stub_stdout=out_f
                )
            with (self.output_folder / 'show_table_result_out.txt').open() as in_f:
                content = in_f.read()
            listed_tables = content.splitlines()
            self.assertIn(self.default_table_name, listed_tables)

    def test_configure_new_profile(self):
        if os.environ.get('TEST_BUILT_PACKAGE'):
            return

        self.addCleanup(self.delete_out_file)

        configure_expected_args = {
            'command': 'configure', 'profile': 'new_profile', 'verbose': False, 'version': False, 'quiet': False
        }
        instance_url = os.environ['AITO_INSTANCE_URL']
        api_key = os.environ['AITO_API_KEY']

        with patch('aito.cli.parser.DEFAULT_CREDENTIAL_FILE', self.out_file_path):
            with patch('builtins.input', side_effect=[instance_url, api_key]):
                self.parse_and_execute(
                    ['configure', '--profile', 'new_profile'], configure_expected_args
                )

            config = get_credentials_file_config()
            self.assertEqual(config.get('new_profile', 'instance_url'), instance_url)
            self.assertEqual(config.get('new_profile', 'api_key'), api_key)

            # remove env to trigger use default profile
            self.stub_environment_variable('AITO_INSTANCE_URL', None)
            self.stub_environment_variable('AITO_API_KEY', None)
            self.create_table()
            with (self.output_folder / 'show_table_result_out.txt').open('w') as out_f:
                self.parse_and_execute(
                    ['show-tables', '--profile', 'new_profile'],
                    {**self.default_parser_args, 'command': 'show-tables', 'profile': 'new_profile'},
                    stub_stdout=out_f
                )
            with (self.output_folder / 'show_table_result_out.txt').open() as in_f:
                content = in_f.read()
            listed_tables = content.splitlines()
            self.assertIn(self.default_table_name, listed_tables)

    @parameterized.expand([
        ('search', {"from": "users"}),
        ('predict', {"from": "products", "where": {"name": "Pirkka banana"}, "predict": "tags"}),
        ('recommend', {"from": "impressions", "recommend": "product", "goal": {"session.user": "veronica"}}),
        ('evaluate', {"test": {"$index": {"$mod": [10, 0]}}, "evaluate": {"from": "products", "predict": "tags"}}),
        ('similarity', {"from": "products", "similarity": {"name": "rye bread"}}),
        ('match', {"from": "impressions", "where": {"session.user": "veronica"}, "match": "product"}),
        ('relate', {"from": "products", "where": {"$exists": "name"}, "relate": "tags"}),
        ('query', {"from": "products", "where": {"name": "Pirkka banana"}, "get": "tags", "orderBy": "$p"})
    ])
    def test_query_to_endpoint(self, endpoint, query):
        instance_url = os.environ['AITO_GROCERY_DEMO_INSTANCE_URL']
        api_key = os.environ['AITO_GROCERY_DEMO_API_KEY']
        query_str = json.dumps(query)

        expected_args = {
            'command': endpoint,
            'query': query_str,
            **self.default_parser_args,
            **{
                'instance_url': instance_url,
                'api_key': api_key
            }
        }
        self.parse_and_execute([endpoint, '-i', instance_url, '-k', api_key, query_str], expected_args)

    def test_quick_predict(self):
        self.create_table()
        self.upload_entries_to_table()

        predicting_field = 'name'

        expected_args = {
            'command': 'quick-predict',
            'from-table': self.default_table_name,
            'predicting-field': predicting_field,
            'evaluate': False,
            **self.default_parser_args
        }
        with self.out_file_path.open('w') as out_f:
            self.parse_and_execute(
                ['quick-predict', self.default_table_name, predicting_field],
                expected_args,
                stub_stdout=out_f
            )

        with self.out_file_path.open() as f:
            returned_content = f.read()
        self.assertIn('[Predict Query Example]', returned_content)

    def test_quick_predict_and_evaluate(self):
        self.create_table()
        self.upload_entries_to_table()

        predicting_field = 'name'

        expected_args = {
            'command': 'quick-predict',
            'from-table': self.default_table_name,
            'predicting-field': predicting_field,
            'evaluate': True,
            **self.default_parser_args
        }
        with self.out_file_path.open('w') as out_f:
            self.parse_and_execute(
                ['quick-predict', self.default_table_name, predicting_field, '--evaluate'],
                expected_args,
                stub_stdout=out_f
            )

        with self.out_file_path.open() as f:
            returned_content = f.read()
        self.assertIn('[Predict Query Example]', returned_content)
        self.assertIn('[Evaluation Result]', returned_content)
