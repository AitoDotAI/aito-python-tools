import ndjson
import json

from tests.test_case import TestCaseCompare
from aito.cli.main_parser import MainParser


class TestAitoConvertParser(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass(test_path='cli/convert')
        cls.input_folder = cls.input_folder.parent.parent / 'schema'
        cls.main_parser = MainParser()

    def setUp(self):
        super().setUp()
        self.out_file_path = self.output_folder / f"{self.method_name}_out.ndjson"

    def test_json_to_ndjson(self):
        self.main_parser.parse_and_execute(['convert', 'json', f"{self.input_folder / 'sample.json'}",
                                            f"{self.out_file_path}"])
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'sample.ndjson').open()))

    def test_csv_to_ndjson(self):
        self.main_parser.parse_and_execute(['convert', 'csv', f"{self.input_folder / 'sample.csv'}",
                                            f"{self.out_file_path}"])
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'sample.ndjson').open()))

    def test_csv_semicolon_to_ndjson(self):
        self.main_parser.parse_and_execute(['convert', 'csv', f"{self.input_folder / 'sample_semicolon.csv'}",
                                            f"{self.out_file_path}", "-d=;"])
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'sample.ndjson').open()))

    def test_excel_to_ndjson(self):
        self.main_parser.parse_and_execute(['convert', 'xlsx', f"{self.input_folder / 'sample.xlsx'}",
                                            f"{self.out_file_path}"])
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'sample.ndjson').open()))

    def test_generate_schema(self):
        schema_out_path = self.output_folder / f"{self.method_name}_schema_out.json"
        self.main_parser.parse_and_execute(['convert', f"-c={schema_out_path}", 'csv',
                                            f"{self.input_folder / 'sample.csv'}", f"{self.out_file_path}"])
        self.assertDictEqual(json.load(schema_out_path.open()),
                             json.load((self.input_folder / 'sample_schema.json').open()))

    def test_use_schema(self):
        self.main_parser.parse_and_execute(['convert', f"-s={self.input_folder / 'sample_schema_altered.json'}", 'csv',
                                            f"{self.input_folder / 'sample.csv'}", f"{self.out_file_path}"])
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'sample_altered.ndjson').open()))
