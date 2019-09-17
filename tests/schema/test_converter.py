import json

import ndjson

from aito.schema.converter import Converter
from tests.test_case import TestCaseCompare


class TestPandasConverter(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass(test_path='schema/converter')
        cls.input_folder = cls.input_folder.parent
        cls.converter = Converter()

    def setUp(self):
        super().setUp()
        self.out_file_path = self.output_folder / f"{self.method_name}_out.ndjson"

    def test_json_to_ndjson(self):
        self.converter.convert_file(self.input_folder / 'sample.json', self.out_file_path, 'json', 'ndjson')
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'sample.ndjson').open()))

    def test_csv_to_ndjson(self):
        self.converter.convert_file(self.input_folder / 'sample.csv', self.out_file_path, 'csv', 'ndjson')
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'sample.ndjson').open()))

    def test_csv_compressed_to_ndjson(self):
        self.converter.convert_file(self.input_folder / 'sample.csv.gz', self.out_file_path, 'csv', 'ndjson')
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'sample.ndjson').open()))

    def test_csv_semicolon_to_ndjson(self):
        self.converter.convert_file(self.input_folder / 'sample_semicolon.csv', self.out_file_path, 'csv', 'ndjson',
                                    read_options={'sep': ';'})
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'sample.ndjson').open()))

    def test_excel_to_ndjson(self):
        self.converter.convert_file(self.input_folder / 'sample.xlsx', self.out_file_path, 'xlsx', 'ndjson')
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'sample.ndjson').open()))

    def test_generate_schema(self):
        schema_file_path = self.output_folder / f"{self.method_name}_schema_out.json"
        self.converter.convert_file(self.input_folder / 'sample.csv', self.out_file_path, 'csv', 'ndjson',
                                    generate_aito_schema=schema_file_path)
        self.assertDictEqual(json.load(schema_file_path.open()),
                             json.load((self.input_folder / 'sample_schema.json').open()))