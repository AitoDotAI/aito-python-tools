import json
import os

from tests.test_case import TestCaseCompare


class TestInferTableSchemaCli(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass(test_path='cli/infer_table_schema')
        cls.input_folder = cls.input_folder.parent.parent / 'schema'

    def setUp(self):
        super().setUp()
        self.out_file_path = self.output_folder / f"{self.method_name}_out.json"

    def tearDown(self):
        super().tearDown()
        self.assertCountEqual(json.load(self.out_file_path.open()),
                              json.load((self.input_folder / 'sample_schema.json').open()))

    def test_infer_schema_from_csv(self):
        os.system(f"python -m aito.cli.main_parser infer-table-schema csv < {self.input_folder}/sample.csv > "
                  f"{self.out_file_path}")

    def test_infer_schema_from_csv_file_path(self):
        os.system(f"python -m aito.cli.main_parser infer-table-schema csv {self.input_folder}/sample.csv > "
                  f"{self.out_file_path}")

    def test_infer_schema_from__csv_semicolon(self):
        os.system(f"python -m aito.cli.main_parser infer-table-schema csv -d ';' "
                  f"< {self.input_folder}/sample.csv > {self.out_file_path}")

    def test_infer_schema_from_csv_semicolon_comma_decimal(self):
        os.system(f"python -m aito.cli.main_parser infer-table-schema csv -d ';' -p ',' "
                  f"< {self.input_folder}/sample.csv > {self.out_file_path}")

    def test_infer_schema_from_excel_file_path(self):
        os.system(f"python -m aito.cli.main_parser infer-table-schema excel {self.input_folder}/sample.xlsx > "
                  f"{self.out_file_path}")

    def test_infer_schema_from_excel_one_sheet(self):
        os.system(f"python -m aito.cli.main_parser infer-table-schema excel  -o Sheet2 "
                  f"{self.input_folder}/sample_multi_sheets.xlsx > {self.out_file_path}")

    def test_infer_schema_from_json(self):
        os.system(f"python -m aito.cli.main_parser infer-table-schema json < "
                  f"{self.input_folder}/sample.json > {self.out_file_path}")

    def test_infer_schema_from_json_file_path(self):
        os.system(f"python -m aito.cli.main_parser infer-table-schema json {self.input_folder}/sample.json > "
                  f"{self.out_file_path}")

    def test_infer_schema_from_ndjson(self):
        os.system(f"python -m aito.cli.main_parser infer-table-schema ndjson < "
                  f"{self.input_folder}/sample.ndjson > {self.out_file_path}")

    def test_infer_schema_from_ndjson_file_path(self):
        os.system(f"python -m aito.cli.main_parser infer-table-schema ndjson {self.input_folder}/sample.ndjson "
                  f"> {self.out_file_path}")
