import json

import ndjson

from aito.schema.data_frame_converter import DataFrameConverter
from tests.test_case import TestCaseCompare


class TestDataFrameConverter(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass(test_path='schema/converter')
        cls.input_folder = cls.input_folder.parent
        cls.converter = DataFrameConverter()

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
        self.converter.convert_file(self.input_folder / 'sample.xlsx', self.out_file_path, 'excel', 'ndjson')
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'sample.ndjson').open()))

    def test_excel_one_sheet_to_ndjson(self):
        self.converter.convert_file(self.input_folder / 'sample_multi_sheets.xlsx', self.out_file_path, 'excel',
                                    'ndjson', read_options={'sheet_name': 'Sheet2'})
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'sample_id_reversed.ndjson').open()))

    # def test_excel_all_sheet_to_ndjson(self):
    #     self.converter.convert_file(self.input_folder / 'sample_multi_sheets.xlsx', self.out_file_path, 'excel',
    #                                 'ndjson', read_options={'sheet_name': None})
    #     self.assertCountEqual(ndjson.load(self.out_file_path.open()),
    #                           ndjson.load((self.input_folder / 'sample_id_reversed.ndjson').open()))

    def test_generate_schema(self):
        schema_file_path = self.output_folder / f"{self.method_name}_schema_out.json"
        self.converter.convert_file(self.input_folder / 'sample.csv', self.out_file_path, 'csv', 'ndjson',
                                    create_table_schema=schema_file_path)
        self.assertDictEqual(json.load(schema_file_path.open()),
                             json.load((self.input_folder / 'sample_schema.json').open()))

    def test_csv_to_ndjson_with_aito_schema(self):
        schema_altered = self.input_folder / 'sample_schema_altered.json'
        self.converter.convert_file(self.input_folder / 'sample.csv', self.out_file_path, 'csv', 'ndjson',
                                    use_table_schema=schema_altered)
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'sample_altered.ndjson').open()))

    def test_csv_to_ndjson_with_aito_schema_convert_nullable(self):
        input_schema = self.input_folder / 'sample_schema_error_nullable.json'
        with self.assertRaises(ValueError):
            self.converter.convert_file(self.input_folder / 'sample.csv', self.out_file_path, 'csv', 'ndjson',
                                        use_table_schema=input_schema)
