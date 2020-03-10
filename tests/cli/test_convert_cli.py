import os
import subprocess

from tests.cases import TestCaseCompare


class TestConvertCli(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass(test_path='cli/convert')
        cls.input_folder = cls.input_folder.parent.parent / 'sample_invoice'
        cls.prefix_args = ['python', '-m', 'aito.cli.main_parser_wrapper']
        if os.getenv('TEST_BUILT_PACKAGE'):
            cls.prefix_args = ['aito']

    def test_json_to_ndjson(self):
        with (self.input_folder / 'invoice.json').open() as in_f, self.out_file_path.open('w') as out_f:
            subprocess.run(self.prefix_args + ['convert', 'json'], stdin=in_f, stdout=out_f)
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice.ndjson', is_ndjson_file=True)

    def test_json_to_ndjson_file_path(self):
        with self.out_file_path.open('w') as out_f:
            subprocess.run(self.prefix_args + ['convert', 'json', f'{self.input_folder}/invoice.json'], stdout=out_f)
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice.ndjson', is_ndjson_file=True)

    def test_csv_to_ndjson(self):
        with (self.input_folder / 'invoice.csv').open() as in_f, self.out_file_path.open('w') as out_f:
            subprocess.run(self.prefix_args + ['convert', 'csv'], stdin=in_f, stdout=out_f)
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice.ndjson', is_ndjson_file=True)

    def test_csv_to_ndjson_file_path(self):
        with self.out_file_path.open('w') as out_f:
            subprocess.run(self.prefix_args + ['convert', 'csv', f'{self.input_folder}/invoice.csv'], stdout=out_f)
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice.ndjson', is_ndjson_file=True)

    def test_csv_to_json(self):
        with self.out_file_path.open('w') as out_f:
            subprocess.run(self.prefix_args + ['convert', 'csv', '-j', f'{self.input_folder}/invoice.csv'],
                           stdout=out_f)
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice.json')

    def test_csv_semicolon_to_ndjson(self):
        with (self.input_folder / 'invoice_semicolon_delimiter.csv').open() as in_f, \
                self.out_file_path.open('w') as out_f:
            subprocess.run(self.prefix_args + ['convert', 'csv', '-d', ';'], stdin=in_f, stdout=out_f)
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice.ndjson', is_ndjson_file=True)

    def test_csv_semicolon_to_json(self):
        with self.out_file_path.open('w') as out_f:
            subprocess.run(
                self.prefix_args + ['convert', 'csv', '-d', ';', '--json',
                                    f'{self.input_folder}/invoice_semicolon_delimiter.csv'],
                stdout=out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice.json')

    def test_csv_semicolon_comma_decimal_to_ndjson(self):
        with (self.input_folder / 'invoice_semicolon_delimiter_comma_decimal.csv').open() as in_f, \
                self.out_file_path.open('w') as out_f:
            subprocess.run(self.prefix_args + ['convert', 'csv', '-d', ';', '-p', ','], stdin=in_f, stdout=out_f)
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice.ndjson', is_ndjson_file=True)

    def test_csv_semicolon_comma_decimal_to_json(self):
        with self.out_file_path.open('w') as out_f:
            subprocess.run(
                self.prefix_args + ['convert', 'csv', '-d', ';', '-p', ',', '-j',
                                    f'{self.input_folder}/invoice_semicolon_delimiter_comma_decimal.csv'],
                stdout=out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice.json')

    def test_excel_to_ndjson_stdin(self):
        with self.assertRaises(subprocess.CalledProcessError, msg='Expected error when using stdin for excel file'), \
             (self.input_folder/'invoice.xlsx').open() as in_f, self.out_file_path.open('w') as out_f:
            subprocess.check_call(self.prefix_args + ['convert', 'excel'], stdin=in_f, stdout=out_f)

    def test_excel_to_ndjson_file_path(self):
        with self.out_file_path.open('w') as out_f:
            subprocess.run(self.prefix_args + ['convert', 'excel', f'{self.input_folder}/invoice.xlsx'], stdout=out_f)
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice.ndjson', is_ndjson_file=True)

    def test_excel_one_sheet_to_ndjson(self):
        with self.out_file_path.open('w') as out_f:
            subprocess.run(self.prefix_args + ['convert', 'excel', '-o', 'Sheet2',
                                               f'{self.input_folder}/invoice_multi_sheets.xlsx'], stdout=out_f)
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_id_reversed.ndjson',
                                is_ndjson_file=True)

    def test_excel_to_json(self):
        with self.out_file_path.open('w') as out_f:
            subprocess.run(
                self.prefix_args + ['convert', 'excel', '-j', f'{self.input_folder}/invoice.xlsx'],
                stdout=out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice.json')

    def test_excel_one_sheet_to_json(self):
        with self.out_file_path.open('w') as out_f:
            subprocess.run(
                self.prefix_args + ['convert', 'excel', '-o', 'Sheet2', '-j',
                                    f'{self.input_folder}/invoice_multi_sheets.xlsx'],
                stdout=out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_id_reversed.json')

    def test_generate_schema_from_csv(self):
        generated_schema_path = self.output_folder / f'{self.method_name}_schema_out.txt'
        with (self.input_folder / 'invoice.csv').open() as in_f, self.out_file_path.open('w') as out_f:
            subprocess.run(
                self.prefix_args + ['convert', 'csv', '-c', str(generated_schema_path)],
                stdin=in_f, stdout=out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice.ndjson', is_ndjson_file=True)
        self.compare_json_files(generated_schema_path, self.input_folder / 'invoice_aito_schema.json')

    def test_generate_schema_from_excel(self):
        generated_schema_path = self.output_folder / f'{self.method_name}_schema_out.txt'
        with self.out_file_path.open('w') as out_f:
            subprocess.run(
                self.prefix_args + ['convert', 'excel', '-c', str(generated_schema_path),
                                    str(self.input_folder / 'invoice.xlsx')],
                stdout=out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice.ndjson', is_ndjson_file=True)
        self.compare_json_files(generated_schema_path, self.input_folder / 'invoice_aito_schema.json')

    def test_generate_schema_from_json(self):
        generated_schema_path = self.output_folder / f'{self.method_name}_schema_out.txt'
        with (self.input_folder / 'invoice.json').open() as in_f, self.out_file_path.open('w') as out_f:
            subprocess.run(
                self.prefix_args + ['convert', 'json', '-c', str(generated_schema_path)],
                stdin=in_f, stdout=out_f)
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice.ndjson', is_ndjson_file=True)
        self.compare_json_files(generated_schema_path, self.input_folder / 'invoice_aito_schema.json')

    def test_generate_schema_from_ndjson(self):
        generated_schema_path = self.output_folder / f'{self.method_name}_schema_out.txt'
        with (self.input_folder / 'invoice.ndjson').open() as in_f, self.out_file_path.open('w') as out_f:
            subprocess.run(
                self.prefix_args + ['convert', 'ndjson', '-c', str(generated_schema_path)],
                stdin=in_f, stdout=out_f
            )
        self.compare_json_files(generated_schema_path, self.input_folder / 'invoice_aito_schema.json')

    def test_generate_schema_file_path(self):
        generated_schema_path = self.output_folder / f'{self.method_name}_schema_out.txt'
        with self.out_file_path.open('w') as out_f:
            subprocess.run(
                self.prefix_args + ['convert', 'csv', '-c', str(generated_schema_path),
                                    f'{self.input_folder}/invoice.csv'],
                stdout=out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice.ndjson', is_ndjson_file=True)
        self.compare_json_files(generated_schema_path, self.input_folder / 'invoice_aito_schema.json')

    def test_use_schema_csv(self):
        with (self.input_folder / 'invoice.csv').open() as in_f, self.out_file_path.open('w') as out_f:
            subprocess.run(
                self.prefix_args + ['convert', 'csv',
                                    '-s', f"{self.input_folder / 'invoice_aito_schema_altered.json'}"],
                stdin=in_f, stdout=out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_altered.ndjson', is_ndjson_file=True)

    def test_use_schema_excel(self):
        with self.out_file_path.open('w') as out_f:
            subprocess.run(
                self.prefix_args + ['convert', 'excel',
                                    '-s', f"{self.input_folder / 'invoice_aito_schema_altered.json'}",
                                    f'{self.input_folder}/invoice.xlsx'],
                stdout=out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_altered.ndjson', is_ndjson_file=True)

    def test_use_schema_json(self):
        with (self.input_folder / 'invoice.json').open() as in_f, self.out_file_path.open('w') as out_f:
            subprocess.run(
                self.prefix_args + ['convert', 'json',
                                    '-s', f"{self.input_folder / 'invoice_aito_schema_altered.json'}"],
                stdin=in_f, stdout=out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_altered.ndjson', is_ndjson_file=True)

    def test_use_schema_ndjson(self):
        with (self.input_folder / 'invoice.ndjson').open() as in_f, self.out_file_path.open('w') as out_f:
            subprocess.run(
                self.prefix_args + ['convert', 'ndjson',
                                    '-s', f"{self.input_folder / 'invoice_aito_schema_altered.json'}"],
                stdin=in_f, stdout=out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_altered.ndjson', is_ndjson_file=True)

    def test_use_schema_file_path(self):
        with self.out_file_path.open('w') as out_f:
            subprocess.run(
                self.prefix_args + ['convert', 'csv',
                                    '-s', f"{self.input_folder / 'invoice_aito_schema_altered.json'}",
                                    f'{self.input_folder}/invoice.csv'],
                stdout=out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_altered.ndjson', is_ndjson_file=True)

    def test_both_create_and_use_schema(self):
        with self.assertRaises(subprocess.CalledProcessError) as context:
            subprocess.check_call(
                self.prefix_args + ['convert', 'csv',
                                    '-c', str(self.output_folder / f'{self.method_name}_schema_out.txt'),
                                    '-s', str(self.input_folder / 'invoice_aito_schema_altered.json'),
                                    str(self.input_folder / 'invoice.csv')]
            )
        self.assertEqual(context.exception.returncode, 2)
