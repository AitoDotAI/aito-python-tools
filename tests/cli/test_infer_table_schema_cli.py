import os
import subprocess

from tests.cases import TestCaseCompare


class TestInferTableSchemaCli(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass(test_path='cli/infer_table_schema')
        cls.input_folder = cls.input_folder.parent.parent / 'sample_invoice'
        cls.prefix_args = ['python', '-m', 'aito.cli']
        if os.getenv('TEST_BUILT_PACKAGE'):
            cls.prefix_args = ['aito']

    def test_infer_schema_from_csv_stdin(self):
        with (self.input_folder / 'invoice.csv').open() as in_f, self.out_file_path.open('w') as out_f:
            subprocess.run(self.prefix_args + ['infer-table-schema', 'csv'], stdin=in_f, stdout=out_f)
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_aito_schema.json')

    def test_infer_schema_from_csv_file_path(self):
        with self.out_file_path.open('w') as out_f:
            subprocess.run(self.prefix_args + ['infer-table-schema', 'csv', f'{self.input_folder}/invoice.csv'],
                           stdout=out_f)
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_aito_schema.json')

    def test_infer_schema_from_csv_semicolon(self):
        with self.out_file_path.open('w') as out_f:
            subprocess.run(self.prefix_args + ['infer-table-schema', 'csv', '-d', ';',
                                               f'{self.input_folder}/invoice_semicolon_delimiter.csv'],
                           stdout=out_f)
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_aito_schema.json')

    def test_infer_schema_from_csv_semicolon_comma_decimal(self):
        with self.out_file_path.open('w') as out_f:
            subprocess.run(self.prefix_args + ['infer-table-schema', 'csv', '-d', ';', '-p', ',',
                                               f'{self.input_folder}/invoice_semicolon_delimiter_comma_decimal.csv'],
                           stdout=out_f)
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_aito_schema.json')

    def test_infer_schema_from_excel_stdin(self):
        with self.assertRaises(subprocess.CalledProcessError):
            with (self.input_folder / 'invoice.xlsx').open() as in_f, self.out_file_path.open('w') as out_f:
                subprocess.check_call(self.prefix_args + ['infer-table-schema', 'excel'], stdin=in_f, stdout=out_f)

    def test_infer_schema_from_excel_file_path(self):
        with self.out_file_path.open('w') as out_f:
            subprocess.run(self.prefix_args + ['infer-table-schema', 'excel', f'{self.input_folder}/invoice.xlsx'],
                           stdout=out_f)
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_aito_schema.json')

    def test_infer_schema_from_excel_one_sheet(self):
        with self.out_file_path.open('w') as out_f:
            subprocess.run(self.prefix_args + ['infer-table-schema', 'excel', '-o', 'Sheet2',
                                               f'{self.input_folder}/invoice_multi_sheets.xlsx'],
                           stdout=out_f)
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_aito_schema.json')

    def test_infer_schema_from_json(self):
        with (self.input_folder / 'invoice.json').open() as in_f, self.out_file_path.open('w') as out_f:
            subprocess.run(self.prefix_args + ['infer-table-schema', 'json'], stdin= in_f, stdout=out_f)
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_aito_schema.json')

    def test_infer_schema_from_json_file_path(self):
        with self.out_file_path.open('w') as out_f:
            subprocess.run(self.prefix_args + ['infer-table-schema', 'json', f'{self.input_folder}/invoice.json'],
                           stdout=out_f)
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_aito_schema.json')

    def test_infer_schema_from_ndjson(self):
        with (self.input_folder / 'invoice.ndjson').open() as in_f, self.out_file_path.open('w') as out_f:
            subprocess.run(self.prefix_args + ['infer-table-schema', 'ndjson'], stdin= in_f, stdout=out_f)
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_aito_schema.json')

    def test_infer_schema_from_ndjson_file_path(self):
        with self.out_file_path.open('w') as out_f:
            subprocess.run(self.prefix_args + ['infer-table-schema', 'ndjson', f'{self.input_folder}/invoice.ndjson'],
                           stdout=out_f)
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_aito_schema.json')
