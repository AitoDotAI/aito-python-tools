import json
import os

import ndjson

from aito.cli.main_parser_wrapper import MainParserWrapper
from tests.test_case import TestCaseCompare


class TestConvertCli(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass(test_path='cli/convert')
        cls.input_folder = cls.input_folder.parent.parent / 'sample_invoice'

    def setUp(self):
        super().setUp()
        self.out_file_path = self.output_folder / f"{self.method_name}_out.ndjson"

    def test_json_to_ndjson(self):
        os.system(f"python -m aito.cli.main_parser convert json < {self.input_folder}/invoice.json "
                  f"> {self.out_file_path}")
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'invoice.ndjson').open()))

    def test_json_to_ndjson_file_path(self):
        os.system(f"python -m aito.cli.main_parser convert json "
                  f"{self.input_folder}/invoice.json > {self.out_file_path}")
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'invoice.ndjson').open()))

    def test_csv_to_ndjson(self):
        os.system(f"python -m aito.cli.main_parser convert csv < "
                  f"{self.input_folder}/invoice.csv > {self.out_file_path}")
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'invoice.ndjson').open()))

    def test_csv_to_ndjson_file_path(self):
        os.system(f"python -m aito.cli.main_parser convert csv {self.input_folder}/invoice.csv > {self.out_file_path}")
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'invoice.ndjson').open()))

    def test_csv_semicolon_to_ndjson(self):
        os.system(f"python -m aito.cli.main_parser convert csv -d ';' "
                  f"< {self.input_folder}/invoice_semicolon_delimiter.csv > {self.out_file_path}")
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'invoice.ndjson').open()))

    def test_csv_semicolon_comma_decimal_to_ndjson(self):
        os.system(f"python -m aito.cli.main_parser convert csv -d ';' -p ','"
                  f"< {self.input_folder}/invoice_semicolon_delimiter_comma_decimal.csv > {self.out_file_path}")
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'invoice.ndjson').open()))

    def test_csv_semicolon_comma_decimal_to_ndjson_file_path(self):
        os.system(f"python -m aito.cli.main_parser convert csv -d ';' -p ',' "
                  f"{self.input_folder}/invoice_semicolon_delimiter_comma_decimal.csv > {self.out_file_path}")
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'invoice.ndjson').open()))

    def test_excel_to_ndjson_file_path(self):
        os.system(f"python -m aito.cli.main_parser convert excel {self.input_folder}/invoice.xlsx "
                  f"> {self.out_file_path}")
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'invoice.ndjson').open()))

    def test_excel_one_sheet_to_ndjson(self):
        os.system(f"python -m aito.cli.main_parser convert excel -o Sheet2 "
                  f"{self.input_folder}/invoice_multi_sheets.xlsx > {self.out_file_path}")
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'invoice_id_reversed.ndjson').open()))

    def test_csv_to_json(self):
        os.system(f"python -m aito.cli.main_parser convert csv -j < {self.input_folder}/invoice.csv "
                  f"> {self.out_file_path}")
        self.assertCountEqual(json.load(self.out_file_path.open()),
                              json.load((self.input_folder / 'invoice.json').open()))

    def test_csv_semicolon_to_json(self):
        os.system(f"python -m aito.cli.main_parser convert csv -d ';' --json "
                  f"< {self.input_folder}/invoice_semicolon_delimiter.csv > {self.out_file_path}")
        self.assertCountEqual(json.load(self.out_file_path.open()),
                              json.load((self.input_folder / 'invoice.json').open()))

    def test_csv_semicolon_comma_decimal_to_json(self):
        os.system(f"python -m aito.cli.main_parser convert csv -d ';' -p ',' --json"
                  f"< {self.input_folder}/invoice_semicolon_delimiter_comma_decimal.csv > {self.out_file_path}")
        self.assertCountEqual(json.load(self.out_file_path.open()),
                              json.load((self.input_folder / 'invoice.json').open()))

    def test_excel_to_json(self):
        os.system(f"python -m aito.cli.main_parser convert excel --json {self.input_folder}/invoice.xlsx "
                  f"> {self.out_file_path}")
        self.assertCountEqual(json.load(self.out_file_path.open()),
                              json.load((self.input_folder / 'invoice.json').open()))

    def test_excel_one_sheet_to_json(self):
        os.system(f"python -m aito.cli.main_parser convert excel -o Sheet2 -j "
                  f"{self.input_folder}/invoice_multi_sheets.xlsx > {self.out_file_path}")
        self.assertCountEqual(json.load(self.out_file_path.open()),
                              json.load((self.input_folder / 'invoice_id_reversed.json').open()))

    def test_generate_schema(self):
        schema_out_path = self.output_folder / f"{self.method_name}_schema_out.json"
        os.system(f"python -m aito.cli.main_parser convert csv -c {schema_out_path} "
                  f"< {self.input_folder}/invoice.csv > {self.out_file_path}")
        self.assertDictEqual(json.load(schema_out_path.open()),
                             json.load((self.input_folder / 'invoice_aito_schema.json').open()))

    def test_generate_schema_file_path(self):
        schema_out_path = self.output_folder / f"{self.method_name}_schema_out.json"
        os.system(f"python -m aito.cli.main_parser convert csv -c {schema_out_path} "
                  f"{self.input_folder}/invoice.csv > {self.out_file_path}")
        self.assertDictEqual(json.load(schema_out_path.open()),
                             json.load((self.input_folder / 'invoice_aito_schema.json').open()))

    def test_use_schema(self):
        os.system(f"python -m aito.cli.main_parser convert csv -s "
                  f"{self.input_folder / 'invoice_aito_schema_altered.json'} "
                  f"< {self.input_folder}/invoice.csv > {self.out_file_path}")
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'invoice_altered.ndjson').open()))

    def test_use_schema_file_path(self):
        os.system(f"python -m aito.cli.main_parser convert csv -s "
                  f"{self.input_folder / 'invoice_aito_schema_altered.json'} "
                  f"{self.input_folder}/invoice.csv > {self.out_file_path}")
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'invoice_altered.ndjson').open()))

    def test_excel_to_ndjson_std_in(self):
        main_parser = MainParserWrapper()
        with self.assertRaises(SystemExit) as context:
            main_parser.parse_and_execute(['convert', 'excel', '-'])
        self.assertEqual(context.exception.code, 2)

    def test_both_create_and_use_schema(self):
        schema_out_path = self.output_folder / f"{self.method_name}_schema_out.json"
        main_parser = MainParserWrapper()
        with self.assertRaises(SystemExit) as context:
            main_parser.parse_and_execute(['convert', 'csv', f"-c={schema_out_path}",
                                           f"-s={self.input_folder / 'invoice_aito_schema_altered.json'}"])
        self.assertEqual(context.exception.code, 2)