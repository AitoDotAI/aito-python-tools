import sys

from aito.cli.main_parser import MainParser
from aito.cli.sub_commands.convert import ConvertFromFormatSubCommand
from tests.cases import CompareTestCase


class TestConvert(CompareTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.parser = MainParser()
        cls.input_folder = cls.input_folder.parent.parent / 'sample_invoice'
        cls.default_main_parser_args = {
            'encoding': 'utf-8', 'command': 'convert', 'verbose': False, 'version': False, 'quiet': False
        }

    def assert_parse_then_execute(
            self, parsing_args, expected_args, stub_stdin=None, stub_stdout= None, execute_exception=None
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

    def test_parse_args_to_df_handler_convert_args(self):
        import json
        with (self.input_folder / 'invoice_aito_schema.json').open() as f:
            schema = json.load(f)

        self.assertEqual(
            {
                'read_input': sys.stdin,
                'write_output': sys.stdout,
                'in_format': 'json',
                'out_format': 'ndjson',
                'read_options': {'encoding': 'utf-8'},
                'convert_options': {},
            },
            ConvertFromFormatSubCommand.parsed_args_to_data_frame_handler_convert_args(
                vars(self.parser.parse_args(['convert', 'json']))
            )
        )

        self.assertEqual(
            {
                'read_input': sys.stdin,
                'write_output': sys.stdout,
                'in_format': 'ndjson',
                'out_format': 'json',
                'read_options': {'encoding': 'utf-8'},
                'convert_options': {},
                'use_table_schema': schema
            },
            ConvertFromFormatSubCommand.parsed_args_to_data_frame_handler_convert_args(
                vars(self.parser.parse_args([
                    'convert', 'ndjson', '-j', '-s', str(self.input_folder / 'invoice_aito_schema.json')
                ]))
            )
        )

        self.assertEqual(
            {
                'read_input': sys.stdin,
                'write_output': sys.stdout,
                'in_format': 'csv',
                'out_format': 'ndjson',
                'read_options': {'encoding': 'utf-8', 'delimiter': ';', 'decimal': ','},
                'convert_options': {},
                'use_table_schema': schema
            },
            ConvertFromFormatSubCommand.parsed_args_to_data_frame_handler_convert_args(
                vars(self.parser.parse_args([
                    'convert', 'csv', '-s', str(self.input_folder / 'invoice_aito_schema.json'), '-d', ';', '-p', ','
                ]))
            )
        )

        self.assertEqual(
            {
                'read_input': self.input_folder / 'invoice.xlsx',
                'write_output': sys.stdout,
                'in_format': 'excel',
                'out_format': 'ndjson',
                'read_options': {'encoding': 'utf-8', 'sheet_name': 'sheet_name'},
                'convert_options': {},
            },
            ConvertFromFormatSubCommand.parsed_args_to_data_frame_handler_convert_args(
                vars(self.parser.parse_args([
                    'convert', 'excel', '-o', 'sheet_name', str(self.input_folder / 'invoice.xlsx')
                ]))
            )
        )

    def test_json_to_ndjson(self):
        expected_args = {
            'input-format': 'json',
            'input': sys.stdin,
            'json': False,
            'use_table_schema': None,
            'create_table_schema': None,
            **self.default_main_parser_args
        }
        with (self.input_folder / 'invoice.json').open() as in_f, self.out_file_path.open('w') as out_f:
            self.assert_parse_then_execute(['convert', 'json'], expected_args, in_f, out_f)
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice.ndjson', is_ndjson_file=True)

    def test_json_to_ndjson_file_path(self):
        expected_args = {
            'input-format': 'json',
            'input': self.input_folder / 'invoice.json',
            'json': False,
            'use_table_schema': None,
            'create_table_schema': None,
            **self.default_main_parser_args
        }
        with self.out_file_path.open('w') as out_f:
            self.assert_parse_then_execute(
                ['convert', 'json', f'{self.input_folder}/invoice.json'], expected_args, None, out_f)
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice.ndjson', is_ndjson_file=True)

    def test_csv_to_ndjson(self):
        expected_args = {
            'input-format': 'csv',
            'input': sys.stdin,
            'json': False,
            'delimiter': ',',
            'decimal': '.',
            'use_table_schema': None,
            'create_table_schema': None,
            **self.default_main_parser_args
        }

        with (self.input_folder / 'invoice.csv').open() as in_f, self.out_file_path.open('w') as out_f:
            self.assert_parse_then_execute(['convert', 'csv'], expected_args, in_f, out_f)
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice.ndjson', is_ndjson_file=True)

    def test_csv_to_ndjson_file_path(self):
        expected_args = {
            'input-format': 'csv',
            'input': self.input_folder / 'invoice.csv',
            'json': False,
            'delimiter': ',',
            'decimal': '.',
            'use_table_schema': None,
            'create_table_schema': None,
            **self.default_main_parser_args
        }
        with self.out_file_path.open('w') as out_f:
            self.assert_parse_then_execute(
                ['convert', 'csv', f'{self.input_folder}/invoice.csv'], expected_args, stub_stdout=out_f)
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice.ndjson', is_ndjson_file=True)

    def test_csv_to_json(self):
        expected_args = {
            'input-format': 'csv',
            'input': self.input_folder / 'invoice.csv',
            'json': True,
            'delimiter': ',',
            'decimal': '.',
            'use_table_schema': None,
            'create_table_schema': None,
            **self.default_main_parser_args
        }
        with self.out_file_path.open('w') as out_f:
            self.assert_parse_then_execute(
                ['convert', 'csv', '-j', f'{self.input_folder}/invoice.csv'], expected_args, stub_stdout=out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice.json')

    def test_csv_semicolon_to_ndjson(self):
        expected_args = {
            'input-format': 'csv',
            'input': sys.stdin,
            'json': False,
            'delimiter': ';',
            'decimal': '.',
            'use_table_schema': None,
            'create_table_schema': None,
            **self.default_main_parser_args
        }
        with (self.input_folder / 'invoice_semicolon_delimiter.csv').open() as in_f, \
                self.out_file_path.open('w') as out_f:
            self.assert_parse_then_execute(['convert', 'csv', '-d', ';'], expected_args, in_f, out_f)
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice.ndjson', is_ndjson_file=True)

    def test_csv_semicolon_to_json(self):
        expected_args = {
            'input-format': 'csv',
            'input': self.input_folder / 'invoice_semicolon_delimiter.csv',
            'json': True,
            'delimiter': ';',
            'decimal': '.',
            'use_table_schema': None,
            'create_table_schema': None,
            **self.default_main_parser_args
        }
        with self.out_file_path.open('w') as out_f:
            self.assert_parse_then_execute(
                ['convert', 'csv', '-d', ';', '--json', f'{self.input_folder}/invoice_semicolon_delimiter.csv'],
                expected_args,
                stub_stdout=out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice.json')

    def test_csv_semicolon_comma_decimal_to_ndjson(self):
        expected_args = {
            'input-format': 'csv',
            'input': sys.stdin,
            'json': False,
            'delimiter': ';',
            'decimal': ',',
            'use_table_schema': None,
            'create_table_schema': None,
            **self.default_main_parser_args
        }
        with (self.input_folder / 'invoice_semicolon_delimiter_comma_decimal.csv').open() as in_f, \
                self.out_file_path.open('w') as out_f:
            self.assert_parse_then_execute(['convert', 'csv', '-d', ';', '-p', ','], expected_args, in_f, out_f)
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice.ndjson', is_ndjson_file=True)

    def test_csv_semicolon_comma_decimal_to_json(self):
        expected_args = {
            'input-format': 'csv',
            'input': sys.stdin,
            'json': True,
            'delimiter': ';',
            'decimal': ',',
            'use_table_schema': None,
            'create_table_schema': None,
            **self.default_main_parser_args
        }
        with (self.input_folder / 'invoice_semicolon_delimiter_comma_decimal.csv').open() as in_f, \
                self.out_file_path.open('w') as out_f:
            self.assert_parse_then_execute(['convert', 'csv', '-d', ';', '-p', ',', '-j'], expected_args, in_f, out_f)
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice.json')

    def test_excel_to_ndjson_stdin(self):
        expected_args = {
            'input-format': 'excel',
            'input': sys.stdin,
            'json': False,
            'one_sheet': None,
            'use_table_schema': None,
            'create_table_schema': None,
            **self.default_main_parser_args
        }
        self.assert_parse_then_execute(['convert', 'excel'], expected_args, execute_exception=SystemExit)

    def test_excel_to_ndjson_file_path(self):
        expected_args = {
            'input-format': 'excel',
            'input': self.input_folder / 'invoice.xlsx',
            'json': False,
            'one_sheet': None,
            'use_table_schema': None,
            'create_table_schema': None,
            **self.default_main_parser_args
        }

        with self.out_file_path.open('w') as out_f:
            self.assert_parse_then_execute(
                ['convert', 'excel', f'{self.input_folder}/invoice.xlsx'], expected_args, stub_stdout=out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice.ndjson', is_ndjson_file=True)

    def test_excel_multiple_sheet_to_ndjson(self):
        expected_args = {
            'input-format': 'excel',
            'input': self.input_folder / 'invoice_multi_sheets.xlsx',
            'json': False,
            'one_sheet': None,
            'use_table_schema': None,
            'create_table_schema': None,
            **self.default_main_parser_args
        }

        with self.out_file_path.open('w') as out_f:
            self.assert_parse_then_execute(
                ['convert', 'excel', f'{self.input_folder}/invoice_multi_sheets.xlsx'], expected_args, stub_stdout=out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice.ndjson', is_ndjson_file=True)

    def test_excel_one_sheet_to_ndjson(self):
        self.maxDiff = None

        expected_args = {
            'input-format': 'excel',
            'input': self.input_folder / 'invoice_multi_sheets.xlsx',
            'json': False,
            'one_sheet': 'Sheet2',
            'use_table_schema': None,
            'create_table_schema': None,
            **self.default_main_parser_args
        }

        with self.out_file_path.open('w') as out_f:
            self.assert_parse_then_execute(
                ['convert', 'excel', '-o', 'Sheet2', f'{self.input_folder}/invoice_multi_sheets.xlsx'],
                expected_args,
                stub_stdout=out_f
            )
        self.compare_json_files(
            self.out_file_path, self.input_folder / 'invoice_id_reversed.ndjson', is_ndjson_file=True
        )

    def test_excel_to_json(self):
        self.maxDiff = None

        expected_args = {
            'input-format': 'excel',
            'input': self.input_folder / 'invoice_multi_sheets.xlsx',
            'json': True,
            'one_sheet': None,
            'use_table_schema': None,
            'create_table_schema': None,
            **self.default_main_parser_args
        }
        with self.out_file_path.open('w') as out_f:
            self.assert_parse_then_execute(
                ['convert', 'excel', '-j', f'{self.input_folder}/invoice_multi_sheets.xlsx'],
                expected_args,
                stub_stdout=out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice.json')

    def test_excel_one_sheet_to_json(self):
        expected_args = {
            'input-format': 'excel',
            'input': self.input_folder / 'invoice_multi_sheets.xlsx',
            'json': True,
            'one_sheet': 'Sheet2',
            'use_table_schema': None,
            'create_table_schema': None,
            **self.default_main_parser_args
        }
        with self.out_file_path.open('w') as out_f:
            self.assert_parse_then_execute(
                ['convert', 'excel', '-j', '-o', 'Sheet2', f'{self.input_folder}/invoice_multi_sheets.xlsx'],
                expected_args,
                stub_stdout=out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_id_reversed.json')

    def test_generate_schema_from_csv(self):
        generated_schema_path = self.output_folder / f'{self.method_name}_schema_out.txt'
        expected_args = {
            'input-format': 'csv',
            'input': sys.stdin,
            'json': False,
            'delimiter': ',',
            'decimal': '.',
            'use_table_schema': None,
            'create_table_schema': generated_schema_path,
            **self.default_main_parser_args
        }
        with (self.input_folder / 'invoice.csv').open() as in_f, self.out_file_path.open('w') as out_f:
            self.assert_parse_then_execute(
                ['convert', 'csv', '-c', str(generated_schema_path)], expected_args, in_f, out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice.ndjson', is_ndjson_file=True)
        self.compare_json_files(generated_schema_path, self.input_folder / 'invoice_aito_schema.json')

    def test_generate_schema_from_excel(self):
        generated_schema_path = self.output_folder / f'{self.method_name}_schema_out.txt'
        expected_args = {
            'input-format': 'excel',
            'input': self.input_folder / 'invoice.xlsx',
            'json': False,
            'one_sheet': None,
            'use_table_schema': None,
            'create_table_schema': generated_schema_path,
            **self.default_main_parser_args
        }
        with self.out_file_path.open('w') as out_f:
            self.assert_parse_then_execute(
                ['convert', 'excel', '-c', str(generated_schema_path), str(self.input_folder / 'invoice.xlsx')],
                expected_args,
                stub_stdout=out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice.ndjson', is_ndjson_file=True)
        self.compare_json_files(generated_schema_path, self.input_folder / 'invoice_aito_schema.json')

    def test_generate_schema_from_json(self):
        generated_schema_path = self.output_folder / f'{self.method_name}_schema_out.txt'
        expected_args = {
            'input-format': 'json',
            'input': sys.stdin,
            'json': False,
            'use_table_schema': None,
            'create_table_schema': generated_schema_path,
            **self.default_main_parser_args
        }
        with (self.input_folder / 'invoice.json').open() as in_f, self.out_file_path.open('w') as out_f:
            self.assert_parse_then_execute(
                ['convert', 'json', '-c', str(generated_schema_path)], expected_args, in_f, out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice.ndjson', is_ndjson_file=True)
        self.compare_json_files(generated_schema_path, self.input_folder / 'invoice_aito_schema.json')

    def test_generate_schema_from_ndjson(self):
        generated_schema_path = self.output_folder / f'{self.method_name}_schema_out.txt'
        in_fp = self.input_folder / 'invoice.ndjson'
        expected_args = {
            'input-format': 'ndjson',
            'input': in_fp,
            'json': True,
            'use_table_schema': None,
            'create_table_schema': generated_schema_path,
            **self.default_main_parser_args
        }
        with self.out_file_path.open('w') as out_f:
            self.assert_parse_then_execute(
                ['convert', 'ndjson', '-j', '-c', str(generated_schema_path), str(in_fp)],
                expected_args,
                stub_stdout=out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice.json')
        self.compare_json_files(generated_schema_path, self.input_folder / 'invoice_aito_schema.json')

    def test_generate_schema_erroneous_file_path(self):
        with self.assertRaises(SystemExit):
            self.parser.parse_args([
                'convert', 'csv', '-c',
                str(self.input_folder / 'another_folder' / 'schema.json'),
                str(self.input_folder / 'invoice.csv')
            ])

    def test_use_schema_csv_to_ndjson(self):
        expected_args = {
            'input-format': 'csv',
            'input': sys.stdin,
            'json': False,
            'delimiter': ',',
            'decimal': '.',
            'use_table_schema': self.input_folder / 'invoice_aito_schema_altered.json',
            'create_table_schema': None,
            **self.default_main_parser_args
        }
        with (self.input_folder / 'invoice.csv').open() as in_f, self.out_file_path.open('w') as out_f:
            self.assert_parse_then_execute(
                ['convert', 'csv', '-s', str(self.input_folder / 'invoice_aito_schema_altered.json')],
                expected_args, in_f, out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_altered.ndjson', is_ndjson_file=True)

    def test_use_schema_excel(self):
        expected_args = {
            'input-format': 'excel',
            'input': self.input_folder / 'invoice.xlsx',
            'json': False,
            'one_sheet': None,
            'use_table_schema': self.input_folder / 'invoice_aito_schema_altered.json',
            'create_table_schema': None,
            **self.default_main_parser_args
        }
        with self.out_file_path.open('w') as out_f:
            self.assert_parse_then_execute([
                'convert', 'excel', '-s', str(self.input_folder / 'invoice_aito_schema_altered.json'),
                str(self.input_folder / 'invoice.xlsx')
            ], expected_args, stub_stdout=out_f)
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_altered.ndjson', is_ndjson_file=True)

    def test_use_schema_json(self):
        expected_args = {
            'input-format': 'json',
            'input': sys.stdin,
            'json': False,
            'use_table_schema': self.input_folder / 'invoice_aito_schema_altered.json',
            'create_table_schema': None,
            **self.default_main_parser_args
        }
        with (self.input_folder / 'invoice.json').open() as in_f, self.out_file_path.open('w') as out_f:
            self.assert_parse_then_execute(
                ['convert', 'json', '-s', str(self.input_folder / 'invoice_aito_schema_altered.json')],
                expected_args, in_f, out_f)
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_altered.ndjson', is_ndjson_file=True)

    def test_use_schema_ndjson(self):
        expected_args = {
            'input-format': 'ndjson',
            'input': self.input_folder / 'invoice.ndjson',
            'json': True,
            'use_table_schema': self.input_folder / 'invoice_aito_schema_altered.json',
            'create_table_schema': None,
            **self.default_main_parser_args
        }
        with self.out_file_path.open('w') as out_f:
            self.assert_parse_then_execute([
                'convert', 'ndjson', '-j', '-s', str(self.input_folder / 'invoice_aito_schema_altered.json'),
                str(self.input_folder / 'invoice.ndjson')],
                expected_args, stub_stdout=out_f
            )
        self.compare_json_files(self.out_file_path, self.input_folder / 'invoice_altered.json')

    def test_use_schema_not_exist_path(self):
        with self.assertRaises(SystemExit):
            self.parser.parse_args(['convert', 'csv', '-s', str(self.input_folder / 'a_random_file_appears')])

    def test_both_create_and_use_schema(self):
        with self.assertRaises(SystemExit):
            self.parser.parse_args([
                'convert', 'csv',
                '-c', str(self.output_folder / f'{self.method_name}_schema_out.txt'),
                '-s', str(self.input_folder / 'invoice_aito_schema_altered.json')
            ])
