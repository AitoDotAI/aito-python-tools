import ndjson

from tests.test_case import TestCaseCompare
from aito.cli.main_parser import MainParser


class TestAitoConvertParser(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass(test_path='cli')
        cls.input_folder = cls.input_folder.parent / 'schema'
        cls.main_parser = MainParser()

    def setUp(self):
        super().setUp()
        self.out_file_path = self.output_folder / f"{self.method_name}_out.ndjson"

    def test_json_to_ndjson(self):
        self.main_parser.parse_and_execute(['convert', f"-i={self.input_folder / 'sample.json'}",
                                            f"-o={self.out_file_path}", 'json'])
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'sample.ndjson').open()))

    def test_csv_to_ndjson(self):
        self.main_parser.parse_and_execute(['convert', f"-i={self.input_folder / 'sample.csv'}",
                                            f"-o={self.out_file_path}", 'csv'])
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'sample.ndjson').open()))

    def test_csv_semicolon_to_ndjson(self):
        self.main_parser.parse_and_execute(['convert', f"-i={self.input_folder / 'sample_semicolon.csv'}",
                                            f"-o={self.out_file_path}", 'csv', '-d=;'])
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'sample.ndjson').open()))

    def test_excel_to_ndjson(self):
        self.main_parser.parse_and_execute(['convert', f"-i={self.input_folder / 'sample.xlsx'}",
                                            f"-o={self.out_file_path}", 'xlsx'])
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'sample.ndjson').open()))
