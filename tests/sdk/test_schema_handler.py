import json
from aito.sdk.schema_handler import SchemaHandler
from aito.sdk.data_frame_handler import DataFrameHandler
from tests.cases import BaseTestCase, CompareTestCase
import datetime


class TestInferAitoType(BaseTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.schema_handler = SchemaHandler()

    def _assert_inferred(self, samples, expect_type):
        self.assertEqual(self.schema_handler.infer_column_type(samples), expect_type)

    def test_infer_from_datetime_type(self):
        self._assert_inferred([datetime.date(1981, 9, 21), datetime.date.today()], 'String')
        self._assert_inferred([datetime.time(), datetime.time(23, 59, 59)], 'String')
        self._assert_inferred([datetime.datetime.now(), datetime.datetime.utcnow()], 'String')
        self._assert_inferred(
            [datetime.date.today() - datetime.date(1981, 9, 21), datetime.timedelta(seconds=20)], 'String'
        )

    def test_infer_from_numeric_type(self):
        self._assert_inferred([1, 2, 3], 'Int')
        self._assert_inferred([1.0, 2.0, 3.0], 'Decimal')
        self._assert_inferred([1, 2, 3.0], 'Decimal')

    def test_infer_from_boolean_type(self):
        self._assert_inferred([True, False, False], 'Boolean')

    def test_infer_from_mixed_type(self):
        self._assert_inferred(['good', 1, 'bad'], 'Text')
        self._assert_inferred([1, 0, 0, True, False], 'Text')


class TestInferAnalyzer(BaseTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.schema_handler = SchemaHandler()

    def _assert_inferred(self, samples, expect_type):
        self.assertEqual(self.schema_handler.infer_text_analyzer(samples), expect_type)

    def test_infer_delimited_text(self):
        self._assert_inferred(
            ['random, seperated, text', 'another, random, separated'],
            {'type': 'delimiter', 'delimiter': ','}
        )
        self._assert_inferred(
            ['random| seperated|text', 'another|random |separated'],
            {'type': 'delimiter', 'delimiter': '|'}
        )
        self._assert_inferred(
            ['random  -seperated- text', 'normal text', 'just-   enough -f  or-   hyphen'],
            {'type': 'delimiter', 'delimiter': '-'}
        )
        self._assert_inferred(
            ['tab\tseperated\ttext', 'another\tone'],
            {'type': 'delimiter', 'delimiter': '\t'}
        )
        self._assert_inferred(
            ['abc abca abcab', 'abcd dcba'],
            'Whitespace'
        )

    def test_infer_language_analyzer(self):
        self._assert_inferred(
            ['is this in english?', 'it definitely is'],
            'en'
        )
        self._assert_inferred(
            ['onko suomeksi?', 'ei todellakaan'],
            'fi'
        )
        self._assert_inferred(
            ['Bonjour monsieur', 'aimez-vous la baguette'],
            'fr'
        )

    def test_not_supported_language(self):
        self._assert_inferred(
            ['đây là tiếng việt', 'đúng rồi'],
            'Whitespace'
        )


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
        table_schema = self.schema_handler.infer_table_schema_from_pandas_data_frame(df)
        self.assertDictEqual(table_schema, json.load((self.input_folder / 'invoice_aito_schema.json').open()))

    def test_validate_table_schema(self):
        with (self.input_folder / 'invoice_aito_schema.json').open() as f:
            table_schema = json.load(f)
        self.schema_handler.validate_table_schema(table_schema)
