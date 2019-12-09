import json

import ndjson

from aito.utils.data_frame_handler import DataFrameHandler
from tests.test_case import TestCaseCompare


class TestDataFrameHandler(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass(test_path='utils/df_handler')
        cls.input_folder = cls.input_folder.parent.parent / 'sample_invoice'
        cls.df_handler = DataFrameHandler()

    def setUp(self):
        super().setUp()
        self.out_file_path = self.output_folder / f"{self.method_name}_out.ndjson"

    def test_json_to_ndjson(self):
        self.df_handler.convert_file(self.input_folder / 'invoice.json', self.out_file_path, 'json', 'ndjson')
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'invoice.ndjson').open()))

    def test_csv_to_ndjson(self):
        self.df_handler.convert_file(self.input_folder / 'invoice.csv', self.out_file_path, 'csv', 'ndjson')
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'invoice.ndjson').open()))

    def test_csv_compressed_to_ndjson(self):
        self.df_handler.convert_file(self.input_folder / 'invoice.csv.gz', self.out_file_path, 'csv', 'ndjson')
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'invoice.ndjson').open()))

    def test_csv_semicolon_to_ndjson(self):
        self.df_handler.convert_file(self.input_folder / 'invoice_semicolon_delimiter.csv', self.out_file_path, 'csv', 'ndjson',
                                     read_options={'sep': ';'})
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'invoice.ndjson').open()))

    def test_excel_to_ndjson(self):
        self.df_handler.convert_file(self.input_folder / 'invoice.xlsx', self.out_file_path, 'excel', 'ndjson')
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'invoice.ndjson').open()))

    def test_excel_one_sheet_to_ndjson(self):
        self.df_handler.convert_file(self.input_folder / 'invoice_multi_sheets.xlsx', self.out_file_path, 'excel',
                                    'ndjson', read_options={'sheet_name': 'Sheet2'})
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'invoice_id_reversed.ndjson').open()))

    def test_csv_to_ndjson_with_aito_schema(self):
        with (self.input_folder / 'invoice_aito_schema_altered.json').open() as f:
            schema_altered = json.load(f)
        self.df_handler.convert_file(self.input_folder / 'invoice.csv', self.out_file_path, 'csv', 'ndjson',
                                     use_table_schema=schema_altered)
        self.assertCountEqual(ndjson.load(self.out_file_path.open()),
                              ndjson.load((self.input_folder / 'invoice_altered.ndjson').open()))

    def test_csv_to_ndjson_with_aito_schema_convert_nullable(self):
        with (self.input_folder / 'invoice_aito_schema_error_nullable.json').open() as f:
            input_schema = json.load(f)
        with self.assertRaises(ValueError):
            self.df_handler.convert_file(self.input_folder / 'invoice.csv', self.out_file_path, 'csv', 'ndjson',
                                         use_table_schema=input_schema)