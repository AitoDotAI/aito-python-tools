import sys

from aito.cli.sub_commands.infer_table_schema_sub_command import InferFromFormatSubCommand
from tests.cli.parser_and_cli_test_case import ParserAndCLITestCase


class TestInferTableSchema(ParserAndCLITestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.input_folder = cls.input_folder.parent.parent / 'sample_invoice'
        cls.default_main_parser_args = {
            'command': 'infer-table-schema', 'verbose': False, 'version': False, 'quiet': False
        }

    def test_parsed_args_to_df_handler_read_args(self):
        self.assertEqual(
            {'read_input': sys.stdin, 'in_format': 'json', 'read_options': {}},
            InferFromFormatSubCommand.parsed_args_to_data_frame_handler_read_args(
                vars(self.parser.parse_args(['infer-table-schema', 'json']))
            )
        )

        self.assertEqual(
            {
                'read_input': self.input_folder / 'invoice.ndjson',
                'in_format': 'ndjson',
                'read_options': {}
            },
            InferFromFormatSubCommand.parsed_args_to_data_frame_handler_read_args(
                vars(self.parser.parse_args([
                    'infer-table-schema', 'ndjson', str(self.input_folder / 'invoice.ndjson')
                ]))
            )
        )

        self.assertEqual(
            {
                'read_input': sys.stdin,
                'in_format': 'csv',
                'read_options': {'delimiter': ';', 'decimal': ','}
            },
            InferFromFormatSubCommand.parsed_args_to_data_frame_handler_read_args(
                vars(self.parser.parse_args(['infer-table-schema', 'csv', '-d', ';', '-p', ',']))
            )
        )

        self.assertEqual(
            {
                'read_input': self.input_folder / 'invoice.xlsx',
                'in_format': 'excel',
                'read_options': {'sheet_name': 'sheet_name'}
            },
            InferFromFormatSubCommand.parsed_args_to_data_frame_handler_read_args(
                vars(self.parser.parse_args([
                    'infer-table-schema', 'excel', str(self.input_folder / 'invoice.xlsx'), '-o', 'sheet_name'
                ]))
            )
        )

    def test_infer_schema_from_csv_stdin(self):
        expected_args = {
            'decimal': '.',
            'delimiter': ',',
            'input': sys.stdin,
            'input-format': 'csv',
            **self.default_main_parser_args
        }
        with (self.input_folder / 'invoice.csv').open() as in_f, self.out_file_path.open('w') as out_f:
            self.parse_and_execute(['infer-table-schema', 'csv'], expected_args, in_f, out_f)
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_aito_schema.json')

    def test_infer_schema_from_csv_file_path(self):
        expected_args = {
            'decimal': '.',
            'delimiter': ',',
            'input': self.input_folder / 'invoice.csv',
            'input-format': 'csv',
            **self.default_main_parser_args
        }
        with self.out_file_path.open('w') as out_f:
            self.parse_and_execute(
                ['infer-table-schema', 'csv', f'{self.input_folder}/invoice.csv'],
                expected_args, stub_stdout=out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_aito_schema.json')

    def test_infer_schema_from_csv_semicolon(self):
        expected_args = {
            'decimal': '.',
            'delimiter': ';',
            'input': self.input_folder / 'invoice_semicolon_delimiter.csv',
            'input-format': 'csv',
            **self.default_main_parser_args
        }
        with self.out_file_path.open('w') as out_f:
            self.parse_and_execute(
                ['infer-table-schema', 'csv', '-d', ';', str(self.input_folder / 'invoice_semicolon_delimiter.csv')],
                expected_args, stub_stdout=out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_aito_schema.json')

    def test_infer_schema_from_csv_semicolon_comma_decimal(self):
        expected_args = {
            'decimal': ',',
            'delimiter': ';',
            'input': self.input_folder / 'invoice_semicolon_delimiter_comma_decimal.csv',
            'input-format': 'csv',
            **self.default_main_parser_args
        }
        with self.out_file_path.open('w') as out_f:
            self.parse_and_execute(
                ['infer-table-schema', 'csv', '-d', ';', '-p', ',',
                 str((self.input_folder / 'invoice_semicolon_delimiter_comma_decimal.csv'))],
                expected_args, stub_stdout=out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_aito_schema.json')

    def test_infer_schema_from_csv_empty_columns(self):
        expected_args = {
            'decimal': '.',
            'delimiter': ',',
            'input': self.input_folder / 'invoice_empty_columns.csv',
            'input-format': 'csv',
            **self.default_main_parser_args
        }
        with self.out_file_path.open('w') as out_f:
            self.parse_and_execute(
                ['infer-table-schema', 'csv',
                 str((self.input_folder / 'invoice_empty_columns.csv'))],
                expected_args, stub_stdout=out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_aito_schema_empty_columns.json')

    def test_infer_schema_from_csv_with_huge_numbers(self):
        expected_args = {
            'decimal': '.',
            'delimiter': ',',
            'input': self.input_folder / 'invoice_with_huge_numbers.csv',
            'input-format': 'csv',
            **self.default_main_parser_args
        }
        with self.out_file_path.open('w') as out_f:
            self.parse_and_execute(
                ['infer-table-schema', 'csv',
                 str((self.input_folder / 'invoice_with_huge_numbers.csv'))],
                expected_args, stub_stdout=out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_aito_schema.json')

    def test_infer_schema_from_excel_stdin(self):
        self.parse_and_execute(
            ['infer-table-schema', 'excel'],
            {'one_sheet': None, 'input': sys.stdin, 'input-format': 'excel', **self.default_main_parser_args},
            execute_exception=SystemExit
        )

    def test_infer_schema_from_excel_file_path(self):
        expected_args = {
            'one_sheet': None,
            'input': self.input_folder / 'invoice.xlsx',
            'input-format': 'excel',
            **self.default_main_parser_args
        }
        with self.out_file_path.open('w') as out_f:
            self.parse_and_execute(
                ['infer-table-schema', 'excel', f'{self.input_folder}/invoice.xlsx'],
                expected_args, stub_stdout=out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_aito_schema.json')

    def test_infer_schema_from_excel_one_sheet(self):
        expected_args = {
            'one_sheet': 'Sheet2',
            'input': self.input_folder / 'invoice_multi_sheets.xlsx',
            'input-format': 'excel',
            **self.default_main_parser_args
        }

        with self.out_file_path.open('w') as out_f:
            self.parse_and_execute(
                ['infer-table-schema', 'excel', '-o', 'Sheet2', str(self.input_folder / 'invoice_multi_sheets.xlsx')],
                expected_args,
                stub_stdout=out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_aito_schema.json')

    def test_infer_schema_from_json(self):
        with (self.input_folder / 'invoice.json').open() as in_f, self.out_file_path.open('w') as out_f:
            self.parse_and_execute(
                ['infer-table-schema', 'json'],
                {'input': sys.stdin, 'input-format': 'json', **self.default_main_parser_args},
                in_f, out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_aito_schema.json')

    def test_infer_schema_from_json_file_path(self):
        with self.out_file_path.open('w') as out_f:
            self.parse_and_execute(
                ['infer-table-schema', 'json', str(self.input_folder / 'invoice.json')],
                {'input': self.input_folder / 'invoice.json', 'input-format': 'json', **self.default_main_parser_args},
                stub_stdout=out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_aito_schema.json')

    def test_infer_schema_from_ndjson(self):
        with (self.input_folder / 'invoice.ndjson').open() as in_f, self.out_file_path.open('w') as out_f:
            self.parse_and_execute(
                ['infer-table-schema', 'ndjson'],
                {'input': sys.stdin, 'input-format': 'ndjson', **self.default_main_parser_args},
                in_f, out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_aito_schema.json')

    def test_infer_schema_from_ndjson_file_path(self):
        in_fp = self.input_folder / 'invoice.ndjson'
        with self.out_file_path.open('w') as out_f:
            self.parse_and_execute(
                ['infer-table-schema', 'ndjson', str(in_fp)],
                {'input': in_fp, 'input-format': 'ndjson', **self.default_main_parser_args},
                stub_stdout=out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_aito_schema.json')

    def test_infer_schema_from_parquet(self):
        expected_args = {
            'input': self.input_folder / 'invoice.parquet',
            'input-format': 'parquet',
            **self.default_main_parser_args
        }
        with self.out_file_path.open('w') as out_f:
            self.parse_and_execute(
                ['infer-table-schema', 'parquet', f'{self.input_folder}/invoice.parquet'],
                expected_args, stub_stdout=out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_aito_schema.json')

    def test_incorrect_input_file_path(self):
        with self.assertRaises(SystemExit):
            self.parser.parse_args(['infer-table-schema', 'csv', str(self.input_folder / 'a_wild_file_appears.csv')])
