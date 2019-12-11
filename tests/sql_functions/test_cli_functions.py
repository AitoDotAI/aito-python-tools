import json
import os

from aito.utils.aito_client import AitoClient
from tests.test_case import TestCaseCompare


class TestSQLFunctionsCli(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass(test_path='sql_functions/sql_connector')
        cls.input_folder = cls.input_folder.parent.parent / 'sample_invoice'
        env_vars = os.environ
        cls.client = AitoClient(env_vars['AITO_INSTANCE_NAME'], env_vars['AITO_RW_KEY'], env_vars['AITO_RO_KEY'])

    def setUp(self):
        super().setUp()
        self.client.delete_database()
        self.out_file_path = self.output_folder / f"{self.method_name}_out.json"

    def create_table(self):
        with (self.input_folder / "invoice_aito_schema.json").open() as f:
            table_schema = json.load(f)
        self.client.put_table_schema('invoice', table_schema)

    def test_infer_schema_from_query(self):
        os.system(f"python -m aito.cli.main_parser_wrapper infer-table-schema from-sql "
                  f"'SELECT * FROM invoice' > {self.out_file_path}")
        self.assertCountEqual(json.load(self.out_file_path.open()),
                              json.load((self.input_folder / 'invoice_aito_schema.json').open()))

    def test_upload_data_from_query(self):
        self.create_table()
        os.system('python -m aito.cli.main_parser_wrapper database upload-data-from-sql invoice '
                  '"SELECT * FROM invoice"')
        self.assertEqual(self.client.query_table_entries('invoice')['total'], 4)

    def test_quick_add_table_from_query(self):
        os.system('python -m aito.cli.main_parser_wrapper database quick-add-table-from-sql invoice '
                  '"SELECT * FROM invoice"')
        table_entries_result = self.client.query_table_entries('invoice')
        self.assertEqual(table_entries_result['total'], 4)
        self.assertCountEqual(table_entries_result['hits'],
                              json.load((self.input_folder / 'invoice_no_null_value.json').open()))
