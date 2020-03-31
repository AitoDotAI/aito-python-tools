import sys

from aito.cli.main_parser import MainParser
from aito.cli.sub_commands.infer_table_schema import InferFromFormatSubCommand
from tests.cases import CompareTestCase


class TestInferTableSchema(CompareTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.parser = MainParser()
        cls.input_folder = cls.input_folder.parent.parent / 'sample_invoice'
        cls.default_main_parser_args = {
            'encoding': 'utf-8', 'command': 'infer-table-schema', 'verbose': False, 'version': False, 'quiet': False
        }

    def assert_parse_then_execute(
            self, parsing_args, expected_args, stub_stdin=None, stub_stdout=None, execute_exception=None
    ):
        self.assertDictEqual(vars(self.parser.parse_args(parsing_args)), expected_args)

        if stub_stdin:
            self.stub_stdin(stub_stdin)
        if stub_stdout:
            self.stub_stdout(stub_stdout)
        # re run parse_args to use the new stubbed stdio
        if execute_exception:
            with self.assertRaises(execute_exception):
                self.parser.parse_and_execute(vars(self.parser.parse_args(parsing_args)))
        else:
            self.parser.parse_and_execute(vars(self.parser.parse_args(parsing_args)))

    def test_parsed_args_to_df_handler_read_args(self):
        self.assertEqual(
            {'read_input': sys.stdin, 'in_format': 'json', 'read_options': {'encoding': 'utf-8'}},
            InferFromFormatSubCommand.parsed_args_to_data_frame_handler_read_args(
                vars(self.parser.parse_args(['infer-table-schema', 'json']))
            )
        )

        self.assertEqual(
            {
                'read_input': self.input_folder / 'invoice.ndjson',
                'in_format': 'ndjson',
                'read_options': {'encoding': 'utf-8'}
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
                'read_options': {'encoding': 'utf-8', 'delimiter': ';', 'decimal': ','}
            },
            InferFromFormatSubCommand.parsed_args_to_data_frame_handler_read_args(
                vars(self.parser.parse_args(['infer-table-schema', 'csv', '-d', ';', '-p', ',']))
            )
        )

        self.assertEqual(
            {
                'read_input': self.input_folder / 'invoice.xlsx',
                'in_format': 'excel',
                'read_options': {'encoding': 'utf-8', 'sheet_name': 'sheet_name'}
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
            self.assert_parse_then_execute(['infer-table-schema', 'csv'], expected_args, in_f, out_f)
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
            self.assert_parse_then_execute(
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
            self.assert_parse_then_execute(
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
            self.assert_parse_then_execute(
                ['infer-table-schema', 'csv', '-d', ';', '-p', ',',
                 str((self.input_folder / 'invoice_semicolon_delimiter_comma_decimal.csv'))],
                expected_args, stub_stdout=out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_aito_schema.json')

    def test_infer_schema_from_excel_stdin(self):
        self.assert_parse_then_execute(
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
            self.assert_parse_then_execute(
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
            self.assert_parse_then_execute(
                ['infer-table-schema', 'excel', '-o', 'Sheet2', str(self.input_folder / 'invoice_multi_sheets.xlsx')],
                expected_args,
                stub_stdout=out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_aito_schema.json')

    def test_infer_schema_from_json(self):
        with (self.input_folder / 'invoice.json').open() as in_f, self.out_file_path.open('w') as out_f:
            self.assert_parse_then_execute(
                ['infer-table-schema', 'json'],
                {'input': sys.stdin, 'input-format': 'json', **self.default_main_parser_args},
                in_f, out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_aito_schema.json')

    def test_infer_schema_from_json_file_path(self):
        with self.out_file_path.open('w') as out_f:
            self.assert_parse_then_execute(
                ['infer-table-schema', 'json', str(self.input_folder / 'invoice.json')],
                {'input': self.input_folder / 'invoice.json', 'input-format': 'json', **self.default_main_parser_args},
                stub_stdout=out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_aito_schema.json')

    def test_infer_schema_from_ndjson(self):
        with (self.input_folder / 'invoice.ndjson').open() as in_f, self.out_file_path.open('w') as out_f:
            self.assert_parse_then_execute(
                ['infer-table-schema', 'ndjson'],
                {'input': sys.stdin, 'input-format': 'ndjson', **self.default_main_parser_args},
                in_f, out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_aito_schema.json')

    def test_infer_schema_from_ndjson_file_path(self):
        in_fp = self.input_folder / 'invoice.ndjson'
        with self.out_file_path.open('w') as out_f:
            self.assert_parse_then_execute(
                ['infer-table-schema', 'ndjson', str(in_fp)],
                {'input': in_fp, 'input-format': 'ndjson', **self.default_main_parser_args},
                stub_stdout=out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_aito_schema.json')

    def test_incorrect_input_file_path(self):
        with self.assertRaises(SystemExit):
            self.parser.parse_args(['infer-table-schema', 'csv', str(self.input_folder / 'a_wild_file_appears.csv')])
