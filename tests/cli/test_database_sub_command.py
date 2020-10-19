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
from aito.utils._credentials_file_utils import get_credentials_file_config
from aito.schema import AitoTableSchema, AitoDatabaseSchema
from tests.cli.parser_and_cli_test_case import ParserAndCLITestCase
from tests.sdk.contexts import default_client, endpoint_methods_test_context


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
            execute_exception=SystemExit
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
            execute_exception=SystemExit
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
        input_file = self.input_folder / f'{self.default_table_name}.csv'
        self.addCleanup(input_file.unlink)
        shutil.copyfile(self.input_folder / 'invoice.csv', input_file)

        expected_args = {
            'command': 'quick-add-table',
            'table_name': None,
            'file_format': None,
            'input-file': input_file,
            **self.default_parser_args
        }
        self.parse_and_execute(['quick-add-table', str(input_file)], expected_args)

        self.compare_table_entries_to_file_content(
            self.default_table_name, self.input_folder / 'invoice_no_null_value.json')

    def test_quick_add_table_different_name(self):
        input_file = self.input_folder / 'invoice.txt'
        self.addCleanup(input_file.unlink)
        shutil.copyfile(self.input_folder / 'invoice.json', input_file)

        expected_args = {
            'command': 'quick-add-table',
            'table_name': self.default_table_name,
            'file_format': 'json',
            'input-file': input_file,
            **self.default_parser_args
        }
        self.parse_and_execute(
            ['quick-add-table', '-n', self.default_table_name, '-f', 'json', str(input_file)], expected_args
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

    def test_get_database(self):
        self.create_table()
        expected_args = {
            **self.default_parser_args,
            'command': 'get-database'
        }
        with self.out_file_path.open('w') as out_f:
            self.parse_and_execute(['get-database'], expected_args, stub_stdout=out_f)

        with self.out_file_path.open() as f:
            returned_content = json.load(f)
        returned_table_schema = AitoDatabaseSchema.from_deserialized_object(returned_content)
        self.assertIn(self.default_table_name, returned_table_schema.tables)
        self.assertEqual(returned_table_schema[self.default_table_name], self.default_table_schema)

    def check_show_table_output_with_profile(self, profile_name: str = None):
        # remove env var to use profile
        self.stub_environment_variable('AITO_INSTANCE_URL', None)
        self.stub_environment_variable('AITO_API_KEY', None)
        self.create_table()

        parsing_args = ['show-tables'] if profile_name is None else ['show-tables', '--profile', profile_name]
        with (self.output_folder / 'show_table_result_out.txt').open('w') as out_f:
            self.parse_and_execute(
                parsing_args,
                {
                    **self.default_parser_args,
                    'command': 'show-tables',
                    'profile': 'default' if profile_name is None else profile_name
                },
                stub_stdout=out_f
            )
        with (self.output_folder / 'show_table_result_out.txt').open() as in_f:
            content = in_f.read()
        listed_tables = content.splitlines()
        self.assertIn(self.default_table_name, listed_tables)

    def check_config_profile(self, profile_name: str = None, reconfigure = False):
        parsing_args = ['configure'] if profile_name is None else ['configure', '--profile', profile_name]
        instance_url = os.environ['AITO_INSTANCE_URL']
        api_key = os.environ['AITO_API_KEY']
        inputs = ['y', instance_url, api_key] if reconfigure else [instance_url, api_key]
        if profile_name is None:
            profile_name = 'default'

        with patch('builtins.input', side_effect=inputs):
            self.parse_and_execute(
                parsing_args,
                {
                    'command': 'configure', 'verbose': False, 'version': False, 'quiet': False,
                    'profile': profile_name
                }
            )

        config = get_credentials_file_config()
        self.assertEqual(config.get(profile_name, 'instance_url'), instance_url)
        self.assertEqual(config.get(profile_name, 'api_key'), api_key)

    @parameterized.expand([
        ('default_profile', None),
        ('new_profile', 'another_profile')
    ])
    @unittest.skipIf(os.environ.get('TEST_BUILT_PACKAGE') is not None, "stub credentials file does not work")
    # TODO: Test built package: communicate with getpass and stub existing credentials file
    def test_configure(self, _, profile_name):
        self.addCleanup(self.delete_out_file)

        with patch('aito.utils._credentials_file_utils.DEFAULT_CREDENTIAL_FILE', self.out_file_path):
            self.check_config_profile(profile_name=profile_name)
            self.check_show_table_output_with_profile(profile_name=profile_name)

    @unittest.skipIf(os.environ.get('TEST_BUILT_PACKAGE') is not None, "stub credentials file does not work")
    def test_reconfigure(self,):
        self.addCleanup(self.delete_out_file)

        with patch('aito.utils._credentials_file_utils.DEFAULT_CREDENTIAL_FILE', self.out_file_path):
            self.check_config_profile(profile_name='another_profile')
            self.check_config_profile(profile_name='another_profile', reconfigure=True)
            self.check_show_table_output_with_profile('another_profile')

    @parameterized.expand(endpoint_methods_test_context)
    def test_query_to_endpoint(self, endpoint, request_cls, query, response_cls):
        command = endpoint.replace('_', '-')
        instance_url = os.environ['AITO_GROCERY_DEMO_INSTANCE_URL']
        api_key = os.environ['AITO_GROCERY_DEMO_API_KEY']
        query_str = json.dumps(query)

        expected_args = {
            'command': command,
            'query': query_str,
            'use_job': False,
            **self.default_parser_args,
            **{
                'instance_url': instance_url,
                'api_key': api_key
            }
        }
        self.parse_and_execute([command, '-i', instance_url, '-k', api_key, query_str], expected_args)

    @parameterized.expand(endpoint_methods_test_context)
    def test_query_to_endpoint_with_job(self, endpoint, request_cls, query, response_cls):
        command = endpoint.replace('_', '-')
        instance_url = os.environ['AITO_GROCERY_DEMO_INSTANCE_URL']
        api_key = os.environ['AITO_GROCERY_DEMO_API_KEY']
        query_str = json.dumps(query)

        expected_args = {
            'command': command,
            'query': query_str,
            'use_job': True,
            **self.default_parser_args,
            **{
                'instance_url': instance_url,
                'api_key': api_key
            }
        }
        self.parse_and_execute([command, '-i', instance_url, '-k', api_key, query_str, '--use-job'], expected_args)

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

    @unittest.skipUnless(
        os.environ.get('RUN_ALTER_INSTANCE_DB_TESTS'),
        "Avoid altering the instance DB when running other tests"
    )
    def test_delete_and_create_database(self):
        with patch('builtins.input', return_value='yes'):
            self.parse_and_execute(['delete-database'], {'command': 'delete-database', **self.default_parser_args})

        self.assertEqual(len(get_existing_tables(self.client)), 0)

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
        self.assertEqual(len(get_existing_tables(self.client)), 1)
        self.assertTrue(check_table_exists(self.client, self.default_table_name))
