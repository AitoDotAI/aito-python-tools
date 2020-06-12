import json

from aito.sdk.aito_schema import AitoTableSchema
from aito.sdk.data_frame_handler import DataFrameHandler
from aito.sdk.schema_handler import SchemaHandler
from tests.cases import CompareTestCase


class TestHandlingTableSchema(CompareTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.input_folder = cls.input_folder.parent.parent / 'sample_invoice'
        cls.schema_handler = SchemaHandler()

    def setUp(self):
        super().setUp()
        self.out_file_path = self.output_folder / f"{self.method_name}_out.ndjson"

    def test_infer_table_schema_from_df(self):
        df_handler = DataFrameHandler()
        df = df_handler.read_file_to_df(self.input_folder / 'invoice.csv', 'csv')
        table_schema_data = self.schema_handler.infer_table_schema_from_pandas_data_frame(df)
        with (self.input_folder / 'invoice_aito_schema.json').open() as f:
            expected_schema_data = json.load(f)
        self.assertEqual(
            AitoTableSchema.from_deserialized_object(table_schema_data),
            AitoTableSchema.from_deserialized_object(expected_schema_data)
        )

    def test_validate_table_schema(self):
        with (self.input_folder / 'invoice_aito_schema.json').open() as f:
            table_schema = json.load(f)
        self.schema_handler.validate_table_schema(table_schema)
