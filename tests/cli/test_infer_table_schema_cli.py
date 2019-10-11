import json
from aito.cli.main_parser import MainParserWrapper
from tests.test_case import TestCaseCompare
import os


class TestInferTableSchemaCli(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass(test_path='cli/infer_table_schema')
        cls.input_folder = cls.input_folder.parent.parent / 'schema'
        cls.main_parser = MainParserWrapper()

    def setUp(self):
        super().setUp()
        self.out_file_path = self.output_folder / f"{self.method_name}_out.json"

    def tearDown(self):
        super().tearDown()
        self.assertCountEqual(json.load(self.out_file_path.open()),
                              json.load((self.input_folder / 'sample_schema.json').open()))

    def test_infer_schema_from_csv(self):
        os.system(f"python -m aito.cli.main_parser infer-table-schema csv < {self.input_folder}/sample.csv > {self.out_file_path}")

    def test_infer_schema_from__csv_semicolon(self):
        os.system(f"python -m aito.cli.main_parser infer-table-schema csv -d ';' "
                  f"< {self.input_folder}/sample.csv > {self.out_file_path}")

    def test_csv_semicolon_comma_decimal_to_ndjson(self):
        os.system(f"python -m aito.cli.main_parser infer-table-schema csv -d ';' -p ',' "
                  f"< {self.input_folder}/sample.csv > {self.out_file_path}")

    def test_excel_to_ndjson(self):
        os.system(f"python -m aito.cli.main_parser infer-table-schema excel {self.input_folder}/sample.xlsx > {self.out_file_path}")

    def test_excel_one_sheet_to_ndjson(self):
        os.system(f"python -m aito.cli.main_parser infer-table-schema excel {self.input_folder}/sample_multi_sheets.xlsx -o Sheet2 "
                  f"> {self.out_file_path}")

    def test_infer_schema_from_json(self):
        os.system(f"python -m aito.cli.main_parser infer-table-schema json < {self.input_folder}/sample.json > {self.out_file_path}")

    def test_infer_schema_from_ndjson(self):
        os.system(f"python -m aito.cli.main_parser infer-table-schema ndjson < {self.input_folder}/sample.ndjson "
                  f"> {self.out_file_path}")