import json
from aito.utils.schema_handler import SchemaHandler
from aito.utils.data_frame_handler import DataFrameHandler
from tests.test_case import TestCaseCompare


class TestSchemaHandler(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass(test_path='utils/schema_handler')
        cls.input_folder = cls.input_folder.parent.parent / 'sample_invoice'
        cls.schema_handler = SchemaHandler()

    def setUp(self):
        super().setUp()
        self.out_file_path = self.output_folder / f"{self.method_name}_out.ndjson"

    def test_generate_table_schema_from_df(self):
        df_handler = DataFrameHandler()
        df = df_handler.read_file_to_df(self.input_folder / 'invoice.csv', 'csv')
        table_schema = self.schema_handler.generate_table_schema_from_pandas_dataframe(df)
        self.assertDictEqual(table_schema, json.load((self.input_folder / 'invoice_aito_schema.json').open()))

    def test_validate_table_schema(self):
        with (self.input_folder / 'invoice_aito_schema.json').open() as f:
            table_schema = json.load(f)
        self.schema_handler.validate_table_schema(table_schema)
