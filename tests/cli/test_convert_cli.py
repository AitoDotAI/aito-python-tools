import json
import os

import ndjson

from aito.cli.main_parser import MainParserWrapper
from tests.test_case import TestCaseCompare


class TestConvertCli(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass(test_path='cli/convert')
        cls.input_folder = cls.input_folder.parent.parent / 'schema'
        cls.main_parser = MainParserWrapper()

    def setUp(self):
        super().setUp()
        self.out_file_path = self.output_folder / f"{self.method_name}_out.ndjson"

    def test_json_to_ndjson(self):
        os.system(f"python aito.py convert json < {self.input_folder}/sample.json > {self.out_file_path}")
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'sample.ndjson').open()))

    def test_csv_to_ndjson(self):
        os.system(f"python aito.py convert csv < {self.input_folder}/sample.csv > {self.out_file_path}")
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'sample.ndjson').open()))

    def test_csv_semicolon_to_ndjson(self):
        os.system(f"python aito.py convert csv -d ';' "
                  f"< {self.input_folder}/sample_semicolon.csv > {self.out_file_path}")
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'sample.ndjson').open()))

    def test_csv_semicolon_comma_decimal_to_ndjson(self):
        os.system(f"python aito.py convert csv -d ';' -p ','"
                  f"< {self.input_folder}/sample_semicolon_comma_decimal.csv > {self.out_file_path}")
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'sample.ndjson').open()))

    def test_excel_to_ndjson(self):
        os.system(f"python aito.py convert excel {self.input_folder}/sample.xlsx > {self.out_file_path}")
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'sample.ndjson').open()))

    def test_excel_one_sheet_to_ndjson(self):
        os.system(f"python aito.py convert excel {self.input_folder}/sample_multi_sheets.xlsx -o Sheet2 "
                  f"> {self.out_file_path}")
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'sample_id_reversed.ndjson').open()))

    def test_csv_to_json(self):
        os.system(f"python aito.py convert csv --json < {self.input_folder}/sample.csv > {self.out_file_path}")
        self.assertCountEqual(json.load(self.out_file_path.open()),
                              json.load((self.input_folder / 'sample.json').open()))

    def test_csv_semicolon_to_json(self):
        os.system(f"python aito.py convert csv -d ';' --json "
                  f"< {self.input_folder}/sample_semicolon.csv > {self.out_file_path}")
        self.assertCountEqual(json.load(self.out_file_path.open()),
                              json.load((self.input_folder / 'sample.json').open()))

    def test_csv_semicolon_comma_decimal_to_json(self):
        os.system(f"python aito.py convert csv -d ';' -p ',' --json"
                  f"< {self.input_folder}/sample_semicolon_comma_decimal.csv > {self.out_file_path}")
        self.assertCountEqual(json.load(self.out_file_path.open()),
                              json.load((self.input_folder / 'sample.json').open()))

    def test_excel_to_json(self):
        os.system(f"python aito.py convert excel {self.input_folder}/sample.xlsx --json > {self.out_file_path}")
        self.assertCountEqual(json.load(self.out_file_path.open()),
                              json.load((self.input_folder / 'sample.json').open()))

    def test_excel_one_sheet_to_json(self):
        os.system(f"python aito.py convert excel {self.input_folder}/sample_multi_sheets.xlsx -o Sheet2 -j"
                  f"> {self.out_file_path}")
        self.assertCountEqual(json.load(self.out_file_path.open()),
                              json.load((self.input_folder / 'sample_id_reversed.ndjson').open()))

    def test_generate_schema(self):
        schema_out_path = self.output_folder / f"{self.method_name}_schema_out.json"
        os.system(f"python aito.py convert csv -c {schema_out_path}"
                  f" < {self.input_folder}/sample.csv > {self.out_file_path}")
        self.assertDictEqual(json.load(schema_out_path.open()),
                             json.load((self.input_folder / 'sample_schema.json').open()))

    def test_use_schema(self):
        os.system(f"python aito.py convert csv -s {self.input_folder / 'sample_schema_altered.json'}"
                  f" < {self.input_folder}/sample.csv > {self.out_file_path}")
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'sample_altered.ndjson').open()))