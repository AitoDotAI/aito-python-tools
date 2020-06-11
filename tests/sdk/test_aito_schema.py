from parameterized import parameterized

from aito.sdk.aito_schema import *
from tests.cases import BaseTestCase


class TestAnalyzerSchema(BaseTestCase):
    @parameterized.expand([
        ('alias', 'whitespace', AitoAliasAnalyzerSchema(alias='whitespace')),
        ('alias_lang', 'en', AitoAliasAnalyzerSchema(alias="english")),
        ('language', {'type': 'language', 'language': 'english'}, AitoLanguageAnalyzerSchema(language='english')),
        ('delimiter', {'type': 'delimiter', 'delimiter': ','}, AitoDelimiterAnalyzerSchema(delimiter=',')),
        ('char-ngram', {'type': 'char-ngram', 'minGram': 1, 'maxGram': 2}, AitoCharNGramAnalyzerSchema(1, 2)),
        (
                'token-ngram',
                {'type': 'token-ngram', 'source': {'type': 'language', 'language': 'french'}, 'minGram': 1, 'maxGram' : 2},
                AitoTokenNgramAnalyzerSchema(source=AitoAliasAnalyzerSchema('fr'), min_gram=1, max_gram=2, token_separator=' ')
        ),
        (
                'token_ngram_inception',
                {
                    'type': 'token-ngram',
                    'source': {'type': 'token-ngram', 'source': 'french', 'minGram': 1, 'maxGram': 2},
                    'minGram': 2,
                    'maxGram': 3
                },
                AitoTokenNgramAnalyzerSchema(
                    source=AitoTokenNgramAnalyzerSchema(
                        AitoAliasAnalyzerSchema('fr'), min_gram=1, max_gram=2, token_separator=' '
                    ),
                    min_gram=2,
                    max_gram=3,
                    token_separator=' ')
        ),
    ])
    def test_from_deserialized_object(self, test_name, deserialized_obj, expected):
        self.assertEqual(AitoAnalyzerSchema.from_deserialized_object(deserialized_obj), expected)

    @parameterized.expand([
        ('invalid_alias', 'spacewhite', ValueError),
        ('missing_type', {'minGram': 1, 'maxGram': 2}, ValueError),
        ('invalid_language', {'type': 'language', 'language': 'elvish'}, ValueError),
        ('no_delimiter', {'type': 'delimiter', 'trimWhiteSpace': True}, ValueError),
        ('invalid_source', {'type': 'token-ngram', 'source': 'aGram', 'minGram': 1, 'maxGram': 2}, ValueError),
    ])
    def test_erroneous_from_deserialized_object(self, test_name, deserialized_obj, error):
        with self.assertRaises(error):
            AitoAnalyzerSchema.from_deserialized_object(deserialized_obj)

    @parameterized.expand([
        ('alias', AitoAliasAnalyzerSchema(alias='whitespace'), 'whitespace'),
        ('alias_lang', AitoAliasAnalyzerSchema(alias="english"), 'english'),
        (
                'language',
                AitoLanguageAnalyzerSchema(language='english'),
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
                AitoDelimiterAnalyzerSchema(delimiter=','),
                {'type': 'delimiter', 'delimiter': ',', 'trimWhiteSpace': True}
        ),
        ('char-ngram', AitoCharNGramAnalyzerSchema(1, 2), {'type': 'char-ngram', 'minGram': 1, 'maxGram': 2}),
        (
                'token-ngram',
                AitoTokenNgramAnalyzerSchema(source=AitoAliasAnalyzerSchema('fr'), min_gram=1, max_gram=2, token_separator=' '),
                {'type': 'token-ngram', 'source': 'french', 'minGram': 1, 'maxGram': 2, 'tokenSeparator': ' '},
        )
    ])
    def test_to_json_serializable(self, test_name, analyzer, expected):
        self.assertEqual(analyzer.to_json_serializable(), expected)

    @parameterized.expand([
        ('diff_analyzer_type', AitoAliasAnalyzerSchema('fr'), AitoDelimiterAnalyzerSchema(','), False),
        ('diff_prop', AitoDelimiterAnalyzerSchema(','), AitoDelimiterAnalyzerSchema(',', trim_white_space=False), False),
        (
                'same',
                AitoLanguageAnalyzerSchema('fr', custom_key_words=['baguette']),
                AitoLanguageAnalyzerSchema('french', use_default_stop_words=False, custom_key_words=['baguette']),
                True
        ),
        ('alias_lang', AitoAliasAnalyzerSchema('fr'), AitoLanguageAnalyzerSchema('french'), True),
        ('alias_lang', AitoAliasAnalyzerSchema('fr'), AitoLanguageAnalyzerSchema('french'), True),
    ])
    def test_comparison(self, test_name, first, second, is_equal):
        self.assertEqual(first == second, is_equal)


class TestColumnTypeSchema(BaseTestCase):
    @parameterized.expand([
        ('string', {'type': 'String'}, AitoColumnTypeSchema(AitoStringType(), nullable=False)),
        (
                'text',
                {'type': 'Text', 'analyzer': 'fi'},
                AitoColumnTypeSchema(AitoTextType(), analyzer=AitoAliasAnalyzerSchema('finnish'))
        ),
        (
                'nullable',
                {'type': 'Int', 'nullable': True},
                AitoColumnTypeSchema(AitoIntType(), nullable=True)
        ),
        (
                'link',
                {'type': 'Int', 'link': 'infinity.beyond'},
                AitoColumnTypeSchema(AitoIntType(), link='infinity.beyond')
        )
    ])
    def test_from_deserialized_object(self, test_name, deserialized_obj, expected):
        self.assertEqual(AitoColumnTypeSchema.from_deserialized_object(deserialized_obj), expected)

    @parameterized.expand([
        ('missing_type', {'analyzer': 'fi'}, ValueError),
        ('invalid_type', {'type': 'array'}, ValueError),
        ('invalid_analyzer', {'type': 'Text', 'analyzer': 'elvish'}, ValueError),
        ('unsupported_analyzer', {'type': 'String', 'analyzer': 'english'}, ValueError),
        ('invalid_link', {'type': 'Int', 'link': 'infinity'}, ValueError),
    ])
    def test_erroneous_from_deserialized_object(self, test_name, deserialized_obj, error):
        with self.assertRaises(error):
            AitoColumnTypeSchema.from_deserialized_object(deserialized_obj)

    @parameterized.expand([
        ('string', AitoColumnTypeSchema(AitoStringType()), {'type': 'String', 'nullable': False}),
        (
                'text',
                AitoColumnTypeSchema(AitoTextType(), analyzer=AitoAliasAnalyzerSchema('finnish')),
                {'type': 'Text', 'nullable': False, 'analyzer': 'finnish'}
        ),
        (
                'link',
                AitoColumnTypeSchema(AitoIntType(), link='infinity.beyond'),
                {'type': 'Int', 'nullable': False, 'link': 'infinity.beyond'}
        )
    ])
    def test_to_json_serializable(self, test_name, column_type, expected):
        self.assertEqual(column_type.to_json_serializable(), expected)

    def test_xd(self):
        self.assertEqual(
            AitoColumnTypeSchema(AitoStringType()).to_json_serializable(),
            {'type': 'String', 'nullable': False}
        )

    def test_comparison(self):
        self.assertEqual(
            AitoColumnTypeSchema(AitoTextType(), analyzer=AitoAliasAnalyzerSchema('fr'), link='table.column'),
            AitoColumnTypeSchema(
                AitoTextType(), nullable=False, analyzer=AitoLanguageAnalyzerSchema('french'), link='table.column'
            )
        )
        self.assertNotEqual(
            AitoColumnTypeSchema(AitoTextType(), analyzer=AitoAliasAnalyzerSchema('fr'), link='table.column'),
            AitoAnalyzerSchema.from_deserialized_object('fi')
        )


class TestTableSchema(BaseTestCase):
    def test_from_deserialized_object(self):
        self.assertEqual(
            AitoTableSchema.from_deserialized_object({
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
            AitoTableSchema({
                'col1': AitoColumnTypeSchema(AitoIntType()),
                'col2': AitoColumnTypeSchema(
                    AitoTextType(), nullable=True, analyzer=AitoLanguageAnalyzerSchema('finnish'), link='another.one'
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
            AitoTableSchema.from_deserialized_object(deserialized_obj)

    def test_to_json_serializable(self):
        self.assertDictEqual(
            AitoTableSchema({
                'col': AitoColumnTypeSchema(
                    AitoTextType(), nullable=True, analyzer=AitoAliasAnalyzerSchema('fi'), link='another.one'
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
            AitoTableSchema({'col': AitoColumnTypeSchema(AitoIntType())}),
            AitoTableSchema({'col': AitoColumnTypeSchema(AitoIntType())}),
        )
        self.assertNotEqual(
            AitoTableSchema({'col': AitoColumnTypeSchema(AitoIntType())}),
            AitoTableSchema({'col': AitoColumnTypeSchema(AitoStringType())}),
        )
        self.assertNotEqual(
            AitoTableSchema({'col': AitoColumnTypeSchema(AitoIntType())}),
            AitoTableSchema({'col1': AitoColumnTypeSchema(AitoIntType())}),
        )


class TestDatabaseSchema(BaseTestCase):
    def test_from_deserialized_object(self):
        self.assertEqual(
            AitoDatabaseSchema.from_deserialized_object({
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
            AitoDatabaseSchema(tables={
                'tbl1': AitoTableSchema(columns={'col1': AitoColumnTypeSchema(AitoBooleanType())}),
                'tbl2': AitoTableSchema(columns={'col2': AitoColumnTypeSchema(AitoStringType())})
            })
        )

    @parameterized.expand([
        ('missing_schema', {'tbl1': {'type': 'table', 'columns': {'col1': {'type': 'Boolean'}}}}, ValueError),
        ('no_table', {'schema': {}}, ValueError),
        ('invalid_table', {'schema': {'tbl1': {'type': 'table', 'columns': {}}}}, ValueError)
    ])
    def test_erroneous_from_deserialized_object(self, test_name, deserialized_obj, error):
        with self.assertRaises(error):
            AitoDatabaseSchema.from_deserialized_object(deserialized_obj)

    def test_to_json_serializable(self):
        self.assertEqual(
            AitoDatabaseSchema(
                tables={
                    'tbl': AitoTableSchema(
                        columns={
                            'col': AitoColumnTypeSchema(
                                AitoTextType(),
                                nullable=True,
                                analyzer=AitoAliasAnalyzerSchema('fi'),
                                link='another.one'
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
            AitoDatabaseSchema(tables={'tbl1': AitoTableSchema(columns={'col1': AitoColumnTypeSchema(AitoBooleanType())})}),
            AitoDatabaseSchema(tables={'tbl1': AitoTableSchema(columns={'col1': AitoColumnTypeSchema(AitoBooleanType())})})
        )
        self.assertNotEqual(
            AitoDatabaseSchema(tables={'tbl1': AitoTableSchema(columns={'col1': AitoColumnTypeSchema(AitoBooleanType())})}),
            AitoDatabaseSchema(tables={'tbl2': AitoTableSchema(columns={'col1': AitoColumnTypeSchema(AitoBooleanType())})})
        )
        self.assertNotEqual(
            AitoDatabaseSchema(tables={'tbl1': AitoTableSchema(columns={'col1': AitoColumnTypeSchema(AitoBooleanType())})}),
            AitoDatabaseSchema(tables={'tbl1': AitoTableSchema(columns={'col2': AitoColumnTypeSchema(AitoBooleanType())})})
        )
