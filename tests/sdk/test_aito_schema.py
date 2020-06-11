from parameterized import parameterized

from aito.sdk.aito_schema import *
from tests.cases import BaseTestCase


class TestAnalyzerSchema(BaseTestCase):
    @parameterized.expand([
        ('alias', 'whitespace', AliasAnalyzerSchema(alias='whitespace')),
        ('alias_lang', 'en', AliasAnalyzerSchema(alias="english")),
        ('language', {'type': 'language', 'language': 'english'}, LanguageAnalyzerSchema(language='english')),
        ('delimiter', {'type': 'delimiter', 'delimiter': ','}, DelimiterAnalyzerSchema(delimiter=',')),
        ('char-ngram', {'type': 'char-ngram', 'minGram': 1, 'maxGram': 2}, CharNGramAnalyzerSchema(1, 2)),
        (
            'token-ngram',
            {'type': 'token-ngram', 'source': {'type': 'language', 'language': 'french'}, 'minGram': 1, 'maxGram' : 2},
            TokenNgramAnalyzerSchema(source=AliasAnalyzerSchema('fr'), min_gram=1, max_gram=2, token_separator=' ')
        ),
        (
                'token_ngram_inception',
                {
                    'type': 'token-ngram',
                    'source': {'type': 'token-ngram', 'source': 'french', 'minGram': 1, 'maxGram': 2},
                    'minGram': 2,
                    'maxGram': 3
                },
                TokenNgramAnalyzerSchema(
                    source=TokenNgramAnalyzerSchema(
                        AliasAnalyzerSchema('fr'), min_gram=1, max_gram=2, token_separator=' '
                    ),
                    min_gram=2,
                    max_gram=3,
                    token_separator=' ')
        ),
    ])
    def test_from_deserialized_object(self, test_name, deserialized_obj, expected):
        self.assertEqual(AnalyzerSchema.from_deserialized_object(deserialized_obj), expected)

    @parameterized.expand([
        ('invalid_alias', 'spacewhite', ValueError),
        ('missing_type', {'minGram': 1, 'maxGram': 2}, ValueError),
        ('invalid_language', {'type': 'language', 'language': 'elvish'}, ValueError),
        ('no_delimiter', {'type': 'delimiter', 'trimWhiteSpace': True}, ValueError),
        ('invalid_source', {'type': 'token-ngram', 'source': 'aGram', 'minGram': 1, 'maxGram': 2}, ValueError),
    ])
    def test_erroneous_from_deserialized_object(self, test_name, deserialized_obj, error):
        with self.assertRaises(error):
            AnalyzerSchema.from_deserialized_object(deserialized_obj)

    @parameterized.expand([
        ('alias', AliasAnalyzerSchema(alias='whitespace'), 'whitespace'),
        ('alias_lang', AliasAnalyzerSchema(alias="english"), 'english'),
        (
                'language',
                LanguageAnalyzerSchema(language='english'),
                {
                    'type': 'language',
                    'language': 'english',
                    'useDefaultStopWords': False,
                    'customStopWords': [],
                    'customKeyWords': []
                }
        ),
        (
                'delimiter',
                DelimiterAnalyzerSchema(delimiter=','),
                {'type': 'delimiter', 'delimiter': ',', 'trimWhiteSpace': True}
        ),
        ('char-ngram', CharNGramAnalyzerSchema(1, 2), {'type': 'char-ngram', 'minGram': 1, 'maxGram': 2}),
        (
                'token-ngram',
                TokenNgramAnalyzerSchema(source=AliasAnalyzerSchema('fr'), min_gram=1, max_gram=2, token_separator=' '),
                {'type': 'token-ngram', 'source': 'french', 'minGram': 1, 'maxGram': 2, 'tokenSeparator': ' '},
        )
    ])
    def test_to_json_serializable(self, test_name, analyzer, expected):
        self.assertEqual(analyzer.to_json_serializable(), expected)

    @parameterized.expand([
        ('diff_analyzer_type', AliasAnalyzerSchema('fr'), DelimiterAnalyzerSchema(','), False),
        ('diff_prop', DelimiterAnalyzerSchema(','), DelimiterAnalyzerSchema(',', trim_white_space=False), False),
        (
                'same',
                LanguageAnalyzerSchema('fr', custom_key_words=['baguette']),
                LanguageAnalyzerSchema('french', use_default_stop_words=False, custom_key_words=['baguette']),
                True
        ),
        ('alias_lang', AliasAnalyzerSchema('fr'), LanguageAnalyzerSchema('french'), True),
        ('alias_lang', AliasAnalyzerSchema('fr'), LanguageAnalyzerSchema('french'), True),
    ])
    def test_comparison(self, test_name, first, second, is_equal):
        self.assertEqual(first == second, is_equal)


class TestColumnTypeSchema(BaseTestCase):
    @parameterized.expand([
        ('string', {'type': 'String'}, ColumnTypeSchema(data_type='String', nullable=False)),
        ('text', {'type': 'Text', 'analyzer': 'fi'}, ColumnTypeSchema('Text', analyzer=AliasAnalyzerSchema('finnish'))),
        ('nullable', {'type': 'Int', 'nullable': True}, ColumnTypeSchema('Int', nullable=True)),
        ('link', {'type': 'Int', 'link': 'infinity.beyond'}, ColumnTypeSchema('Int', link='infinity.beyond')),
    ])
    def test_from_deserialized_object(self, test_name, deserialized_obj, expected):
        self.assertEqual(ColumnTypeSchema.from_deserialized_object(deserialized_obj), expected)

    @parameterized.expand([
        ('missing_type', {'analyzer': 'fi'}, ValueError),
        ('invalid_type', {'type': 'array'}, ValueError),
        ('invalid_analyzer', {'type': 'Text', 'analyzer': 'elvish'}, ValueError),
        ('unsupported_analyzer', {'type': 'String', 'analyzer': 'english'}, ValueError),
        ('invalid_link', {'type': 'Int', 'link': 'infinity'}, ValueError),
    ])
    def test_erroneous_from_deserialized_object(self, test_name, deserialized_obj, error):
        with self.assertRaises(error):
            ColumnTypeSchema.from_deserialized_object(deserialized_obj)

    @parameterized.expand([
        ('string', ColumnTypeSchema(data_type='String'), {'type': 'String', 'nullable': False}),
        (
                'text',
                ColumnTypeSchema('Text', analyzer=AliasAnalyzerSchema('finnish')),
                {'type': 'Text', 'nullable': False, 'analyzer': 'finnish'}
        ),
        (
                'link',
                ColumnTypeSchema('Int', link='infinity.beyond'),
                {'type': 'Int', 'nullable': False, 'link': 'infinity.beyond'}
        )
    ])
    def test_to_json_serializable(self, test_name, column_type, expected):
        self.assertEqual(column_type.to_json_serializable(), expected)

    def test_comparison(self):
        self.assertEqual(
            ColumnTypeSchema('Text', analyzer=AliasAnalyzerSchema('fr'), link='table.column'),
            ColumnTypeSchema('Text', nullable=False, analyzer=LanguageAnalyzerSchema('french'), link='table.column')
        )
        self.assertNotEqual(
            ColumnTypeSchema('Text', analyzer=AliasAnalyzerSchema('fr'), link='table.column'),
            AnalyzerSchema.from_deserialized_object('fi')
        )


class TestTableSchema(BaseTestCase):
    def test_from_deserialized_object(self):
        self.assertEqual(
            TableSchema.from_deserialized_object({
                'type': 'table',
                'columns': {
                    'col1': {'type': 'Int'},
                    'col2': {
                        'type': 'Text',
                        'nullable': True,
                        'analyzer': 'finnish',
                        'link': 'another.one'
                    }
                }
            }),
            TableSchema({
                'col1': ColumnTypeSchema('Int'),
                'col2': ColumnTypeSchema(
                    data_type='Text', nullable=True, analyzer=LanguageAnalyzerSchema('finnish'), link='another.one'
                )
            })
        )

    @parameterized.expand([
        ('missing_type', {'columns': {'col1': {'type': 'Int'}}}, ValueError),
        ('no_column', {'type': 'table', 'columns': {}}, ValueError),
        ('invalid_column', {'type': 'table', 'columns': {'col2': {'type': 'array'}}}, ValueError)

    ])
    def test_erroneous_from_deserialized_object(self, test_name, deserialized_obj, error):
        with self.assertRaises(error):
            TableSchema.from_deserialized_object(deserialized_obj)

    def test_to_json_serializable(self):
        self.assertDictEqual(
            TableSchema({
                'col': ColumnTypeSchema(
                    data_type='Text', nullable=True, analyzer=AliasAnalyzerSchema('fi'), link='another.one'
                )
            }).to_json_serializable(),
            {
                'type': 'table',
                'columns': {
                    'col': {
                        'type': 'Text',
                        'nullable': True,
                        'analyzer': 'finnish',
                        'link': 'another.one'
                    }
                }
            }
        )

    def test_comparison(self):
        self.assertEqual(
            TableSchema({'col': ColumnTypeSchema('Int')}),
            TableSchema({'col': ColumnTypeSchema('Int')}),
        )
        self.assertNotEqual(
            TableSchema({'col': ColumnTypeSchema('Int')}),
            TableSchema({'col': ColumnTypeSchema('String')}),
        )
        self.assertNotEqual(
            TableSchema({'col': ColumnTypeSchema('Int')}),
            TableSchema({'col1': ColumnTypeSchema('Int')}),
        )


class TestDatabaseSchema(BaseTestCase):
    def test_from_deserialized_object(self):
        self.assertEqual(
            DatabaseSchema.from_deserialized_object({
                'schema': {
                    'tbl1': {
                        'type': 'table',
                        'columns': {'col1': {'type': 'Boolean'}}
                    },
                    'tbl2': {
                        'type': 'table',
                        'columns': {'col2': {'type': 'String'}}
                    }
                }
            }),
            DatabaseSchema(tables={
                'tbl1': TableSchema(columns={'col1': ColumnTypeSchema(data_type='Boolean')}),
                'tbl2': TableSchema(columns={'col2': ColumnTypeSchema(data_type='String')})
            })
        )

    @parameterized.expand([
        ('missing_schema', {'tbl1': {'type': 'table', 'columns': {'col1': {'type': 'Boolean'}}}}, ValueError),
        ('no_table', {'schema': {}}, ValueError),
        ('invalid_table', {'schema': {'tbl1': {'type': 'table', 'columns': {}}}}, ValueError)
    ])
    def test_erroneous_from_deserialized_object(self, test_name, deserialized_obj, error):
        with self.assertRaises(error):
            DatabaseSchema.from_deserialized_object(deserialized_obj)

    def test_to_json_serializable(self):
        self.assertEqual(
            DatabaseSchema(
                tables={
                    'tbl': TableSchema(
                        columns={
                            'col': ColumnTypeSchema(
                                data_type='Text', nullable=True, analyzer=AliasAnalyzerSchema('fi'), link='another.one'
                            )
                        }
                    ),
                }
            ).to_json_serializable(),
            {
                'schema': {
                    'tbl': {
                        'type': 'table',
                        'columns': {
                            'col': {
                                'type': 'Text',
                                'nullable': True,
                                'analyzer': 'finnish',
                                'link': 'another.one'
                            }
                        }
                    }
                }
            }
        )

    def test_comparison(self):
        self.assertEqual(
            DatabaseSchema(tables={'tbl1': TableSchema(columns={'col1': ColumnTypeSchema(data_type='Boolean')})}),
            DatabaseSchema(tables={'tbl1': TableSchema(columns={'col1': ColumnTypeSchema(data_type='Boolean')})})
        )
        self.assertNotEqual(
            DatabaseSchema(tables={'tbl1': TableSchema(columns={'col1': ColumnTypeSchema(data_type='Boolean')})}),
            DatabaseSchema(tables={'tbl2': TableSchema(columns={'col1': ColumnTypeSchema(data_type='Boolean')})})
        )
        self.assertNotEqual(
            DatabaseSchema(tables={'tbl1': TableSchema(columns={'col1': ColumnTypeSchema(data_type='Boolean')})}),
            DatabaseSchema(tables={'tbl1': TableSchema(columns={'col2': ColumnTypeSchema(data_type='Boolean')})})
        )