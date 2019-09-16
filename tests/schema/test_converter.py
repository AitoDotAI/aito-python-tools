import json

import ndjson

from aito.schema.converter import Converter
from tests.test_case import TestCaseCompare


class TestPandasConverter(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass(test_path='schema')
        cls.converter = Converter()

    def setUp(self):
        super().setUp()
        self.out_file_path = self.output_folder / f"{self.method_name}_out.ndjson"

    def test_json_to_ndjson(self):
        with self.out_file_path.open(mode='w') as out_f:
            self.converter.convert_file(self.input_folder / 'sample.json', out_f, 'json', 'ndjson',
                                        generate_aito_schema=False)
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'sample.ndjson').open()))

    def test_csv_to_ndjson(self):
        with self.out_file_path.open(mode='w') as out_f:
            self.converter.convert_file(self.input_folder / 'sample.csv', out_f, 'csv', 'ndjson',
                                        generate_aito_schema=False)
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'sample.ndjson').open()))

    def test_csv_compressed_to_ndjson(self):
        with self.out_file_path.open(mode='w') as out_f:
            self.converter.convert_file(self.input_folder / 'sample.csv.gz', out_f, 'csv', 'ndjson',
                                        generate_aito_schema=False)
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'sample.ndjson').open()))

    def test_csv_semicolon_to_ndjson(self):
        with self.out_file_path.open(mode='w') as out_f:
            self.converter.convert_file(self.input_folder / 'sample_semicolon.csv', out_f, 'csv', 'ndjson',
                                        read_options={'sep': ';'}, generate_aito_schema=False)
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'sample.ndjson').open()))

    def test_excel_to_ndjson(self):
        with self.out_file_path.open(mode='w') as out_f:
            self.converter.convert_file(self.input_folder / 'sample.xlsx', out_f, 'xlsx', 'ndjson',
                                        generate_aito_schema=False)
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'sample.ndjson').open()))

