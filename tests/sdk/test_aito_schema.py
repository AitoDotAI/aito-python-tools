import datetime

from parameterized import parameterized

from aito.schema import *
from aito.utils._json_format import JsonValidationError
from tests.cases import BaseTestCase


class TestAitoAnalyzerSchema(BaseTestCase):
    @parameterized.expand([
        ('alias', 'whitespace', AitoAliasAnalyzerSchema(alias='whitespace')),
        ('alias_lang', 'en', AitoAliasAnalyzerSchema(alias="english")),
        ('language', {'type': 'language', 'language': 'english'}, AitoLanguageAnalyzerSchema(language='english')),
        ('delimiter', {'type': 'delimiter', 'delimiter': ','}, AitoDelimiterAnalyzerSchema(delimiter=',')),
        ('char-ngram', {'type': 'char-ngram', 'minGram': 1, 'maxGram': 2}, AitoCharNGramAnalyzerSchema(1, 2)),
        (
            'token-ngram',
            {'type': 'token-ngram', 'source': {'type': 'language', 'language': 'french'}, 'minGram': 1, 'maxGram': 2},
            AitoTokenNgramAnalyzerSchema(
                source=AitoAliasAnalyzerSchema('fr'), min_gram=1, max_gram=2, token_separator=' '
            )
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
    def test_from_deserialized_object(self, _, deserialized_obj, expected):
        self.assertEqual(AitoAnalyzerSchema.from_deserialized_object(deserialized_obj), expected)

    @parameterized.expand([
        ('invalid_alias', 'spacewhite', JsonValidationError),
        ('missing_type', {'minGram': 1, 'maxGram': 2}, JsonValidationError),
        ('invalid_language', {'type': 'language', 'language': 'elvish'}, JsonValidationError),
        ('no_delimiter', {'type': 'delimiter', 'trimWhitespace': True}, JsonValidationError),
        ('invalid_source', {'type': 'token-ngram', 'source': 'aGram', 'minGram': 1, 'maxGram': 2}, JsonValidationError),
    ])
    def test_erroneous_from_deserialized_object(self, _, deserialized_obj, error):
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
                {'type': 'delimiter', 'delimiter': ',', 'trimWhitespace': True}
        ),
        ('char-ngram', AitoCharNGramAnalyzerSchema(1, 2), {'type': 'char-ngram', 'minGram': 1, 'maxGram': 2}),
        (
                'token-ngram',
                AitoTokenNgramAnalyzerSchema(
                    source=AitoAliasAnalyzerSchema('fr'), min_gram=1, max_gram=2, token_separator=' '
                ),
                {'type': 'token-ngram', 'source': 'french', 'minGram': 1, 'maxGram': 2, 'tokenSeparator': ' '},
        )
    ])
    def test_to_json_serializable(self, _, analyzer, expected):
        self.assertEqual(analyzer.to_json_serializable(), expected)

    @parameterized.expand([
        ('diff_analyzer_type', AitoAliasAnalyzerSchema('fr'), AitoDelimiterAnalyzerSchema(','), False),
        (
            'diff_prop',
            AitoDelimiterAnalyzerSchema(','),
            AitoDelimiterAnalyzerSchema(',', trim_white_space=False),
            False
        ),
        (
            'same',
            AitoLanguageAnalyzerSchema('fr', custom_key_words=['baguette']),
            AitoLanguageAnalyzerSchema('french', use_default_stop_words=False, custom_key_words=['baguette']),
            True
        ),
        ('alias_lang', AitoAliasAnalyzerSchema('fr'), AitoLanguageAnalyzerSchema('french'), True),
        ('alias_delimiter', AitoAliasAnalyzerSchema('whitespace'), AitoDelimiterAnalyzerSchema(' '), True),
    ])
    def test_comparison(self, _, first, second, is_equal):
        self.assertEqual(first == second, is_equal)

    @parameterized.expand([
        ('comma', ['random, seperated, text', 'another, random, separated'], ','),
        ('pipe', ['random| seperated|text', 'another|random |separated'], '|'),
        ('hyphen', ['random  -seperated- text', 'normal text', 'just-   enough -f  or-   hyphen'], '-'),
        ('tab', ['tab\tseperated\ttext', 'another\tone'], '\t'),
        ('whitespace', ['abc abca abcab', 'abcd dcba'], ' ')
    ])
    def test_infer_delimited_text(self, _, samples, delimiter):
        self.assertEqual(AitoAnalyzerSchema.infer_from_samples(samples), AitoDelimiterAnalyzerSchema(delimiter))

    @parameterized.expand([
        ('english', ['is this in english?', 'it definitely is']),
        ('finnish', ['onko suomeksi?', 'ei todellakaan']),
        ('fr', ['Bonjour monsieur', 'aimez-vous la baguette'])
    ])
    def test_infer_language_analyzer(self, language, samples):
        self.assertEqual(AitoAnalyzerSchema.infer_from_samples(samples), AitoLanguageAnalyzerSchema(language))

    def test_unsupported_language(self):
        self.assertEqual(
            AitoAnalyzerSchema.infer_from_samples(['đây là tiếng việt', 'đúng rồi']),
            AitoAliasAnalyzerSchema('whitespace')
        )


class TestAitoDataType(BaseTestCase):
    @parameterized.expand([
        ('String', AitoStringType()),
        ('Text', AitoTextType()),
        ('Boolean', AitoBooleanType()),
        ('Int', AitoIntType()),
        ('Decimal', AitoDecimalType())
    ])
    def test_from_deserialized_object_and_to_serialized_object(self, deserialized_obj, aito_type):
        self.assertEqual(AitoDataTypeSchema.from_deserialized_object(deserialized_obj), aito_type)
        self.assertEqual(aito_type.to_json_serializable(), deserialized_obj)

    @parameterized.expand([
        ('unsupported type', 'Type'),
        ('not string', {'type': 'String'})
    ])
    def test_erroneous_from_deserialized_object(self, _, deserialized_obj):
        with self.assertRaises(JsonValidationError):
            AitoDataTypeSchema.from_deserialized_object(deserialized_obj)

    def test_comparison(self):
        self.assertNotEqual(AitoIntType(), AitoStringType())
        self.assertTrue(AitoIntType.is_int)
        self.assertTrue(AitoIntType.is_text)

    @parameterized.expand([
        ('date', [datetime.date(1981, 9, 21), datetime.date.today()]),
        ('time', [datetime.time(), datetime.time(23, 59, 59)]),
        ('datetime', [datetime.datetime.now(), datetime.datetime.utcnow()]),
        ('timedelta', [datetime.date.today() - datetime.date(1981, 9, 21), datetime.timedelta(seconds=20)])
    ])
    def test_infer_from_datetime_type(self, _, samples):
        self.assertEqual(AitoDataTypeSchema.infer_from_samples(samples), AitoStringType())

    @parameterized.expand([
        ('int', [1, 2, 3], AitoIntType()),
        ('float', [1.0, 2.0, 3.0], AitoDecimalType()),
        ('mixed', [1, 2, 3.0], AitoDecimalType()),
    ])
    def test_infer_from_numeric_type(self, _, samples, expected):
        self.assertEqual(AitoDataTypeSchema.infer_from_samples(samples), expected)

    @parameterized.expand([
        ('bool', [True, False, False], AitoBooleanType()),
    ])
    def test_infer_from_boolean_type(self, _, samples, expected):
        self.assertEqual(AitoDataTypeSchema.infer_from_samples(samples), expected)

    @parameterized.expand([
        ('str_num', ['good', 1, 'bad'], AitoTextType()),
        ('num_bool', [1, 0, 0, True, False], AitoTextType())
    ])
    def test_infer_from_mixed_type(self, _, samples, expected):
        self.assertEqual(AitoDataTypeSchema.infer_from_samples(samples), expected)


class TestAitoColumnLink(BaseTestCase):
    def test_from_deserialized_object(self):
        self.assertEqual(AitoColumnLinkSchema.from_deserialized_object('tbl.col'), AitoColumnLinkSchema('tbl', 'col'))

    def test_to_serialized_object(self):
        self.assertEqual(AitoColumnLinkSchema('tbl', 'col').to_json_serializable(), 'tbl.col')

    def test_comparison(self):
        self.assertEqual(AitoColumnLinkSchema('tbl', 'col'), AitoColumnLinkSchema('tbl', 'col'))
        self.assertNotEqual(
            AitoColumnLinkSchema('tbl', 'col'), AitoColumnLinkSchema.from_deserialized_object('tbl.col1')
        )
        self.assertNotEqual(
            AitoColumnLinkSchema.from_deserialized_object('tbl1.col'), AitoColumnLinkSchema('tbl', 'col')
        )

class TestDataSeriesProperties(BaseTestCase):
    @parameterized.expand([
        (
                'integer',
                [1, 2, 3],
                DataSeriesProperties('integer', 1, 3),
                'Int',
        ),
        (
                'within-bounds-integer',
                [DataSeriesProperties.MIN_INT_VALUE, 2, DataSeriesProperties.MAX_INT_VALUE],
                DataSeriesProperties('integer', DataSeriesProperties.MIN_INT_VALUE, DataSeriesProperties.MAX_INT_VALUE),
                'Int',
        ),
        (
                'big-integer',
                [1, 2, DataSeriesProperties.MAX_INT_VALUE + 1],
                DataSeriesProperties('integer', 1, DataSeriesProperties.MAX_INT_VALUE + 1),
                'String',
        ),
        (
                'big-negative-integer',
                [DataSeriesProperties.MIN_INT_VALUE - 1, 2, 3],
                DataSeriesProperties('integer', DataSeriesProperties.MIN_INT_VALUE - 1, 3),
                'String',
        ),
        (
                'empty',
                [],
                DataSeriesProperties('empty', None, None),
                'String',
        ),
        (
                'whitespace',
                [" ", "", "  "],
                DataSeriesProperties('string', None, None),
                'Text',
        )
    ])

    def test_from_series(self, _, serie, expected, aito_dtype):
        ds = DataSeriesProperties._infer_from_pandas_series(pd.Series(serie))
        self.assertEqual(
            ds.pandas_dtype,
            expected.pandas_dtype)
        self.assertEqual(
            ds.min_value,
            expected.min_value)
        self.assertEqual(
            ds.max_value,
            expected.max_value)
        self.assertEqual(
            ds.target_aito_dtype,
            aito_dtype)
    

class TestAitoColumnTypeSchema(BaseTestCase):
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
                AitoColumnTypeSchema(AitoIntType(), link=AitoColumnLinkSchema('infinity', 'beyond'))
        )
    ])
    def test_from_deserialized_object(self, _, deserialized_obj, expected):
        self.assertEqual(AitoColumnTypeSchema.from_deserialized_object(deserialized_obj), expected)

    @parameterized.expand([
        ('missing_type', {'analyzer': 'fi'}, JsonValidationError),
        ('invalid_type', {'type': 'array'}, JsonValidationError),
        ('invalid_analyzer', {'type': 'Text', 'analyzer': 'elvish'}, JsonValidationError),
        ('unsupported_analyzer', {'type': 'String', 'analyzer': 'english'}, ValueError),
        ('invalid_link', {'type': 'Int', 'link': 'infinity'}, JsonValidationError),
    ])
    def test_erroneous_from_deserialized_object(self, _, deserialized_obj, error):
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
                AitoColumnTypeSchema(AitoIntType(), link=AitoColumnLinkSchema('infinity', 'beyond')),
                {'type': 'Int', 'nullable': False, 'link': 'infinity.beyond'}
        )
    ])
    def test_to_json_serializable(self, _, column_type, expected):
        self.assertEqual(column_type.to_json_serializable(), expected)

    def test_comparison(self):
        self.assertEqual(
            AitoColumnTypeSchema(
                AitoTextType(), analyzer=AitoAliasAnalyzerSchema('fr'), link=AitoColumnLinkSchema('table', 'column')),
            AitoColumnTypeSchema(
                AitoTextType(),
                nullable=False,
                analyzer=AitoLanguageAnalyzerSchema('french'),
                link=AitoColumnLinkSchema('table', 'column')
            )
        )
        self.assertNotEqual(
            AitoColumnTypeSchema(
                AitoTextType(), analyzer=AitoAliasAnalyzerSchema('fr'), link=AitoColumnLinkSchema('table', 'column')
            ),
            AitoColumnTypeSchema(
                AitoTextType(), analyzer=AitoAliasAnalyzerSchema('french')
            ),
        )

    def test_link(self):
        col_schema_1 = AitoColumnTypeSchema(AitoIntType(), link=AitoColumnLinkSchema('infinity', 'beyond'))
        self.assertTrue(col_schema_1.has_link)
        self.assertEqual(col_schema_1.link, AitoColumnLinkSchema('infinity', 'beyond'))


class TestAitoTableSchema(BaseTestCase):
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
                    AitoTextType(),
                    nullable=True,
                    analyzer=AitoLanguageAnalyzerSchema('finnish'),
                    link=AitoColumnLinkSchema('another', 'one')
                )
            })
        )

    @parameterized.expand([
        ('missing_type', {'columns': {'col1': {'type': 'Int'}}}, JsonValidationError),
        ('wrong_type', {'type': 'something_else', 'columns': {}}, JsonValidationError),
        ('invalid_column', {'type': 'table', 'columns': {'col2': {'type': 'array'}}}, JsonValidationError)

    ])
    def test_erroneous_from_deserialized_object(self, _, deserialized_obj, error):
        with self.assertRaises(error):
            AitoTableSchema.from_deserialized_object(deserialized_obj)

    def test_to_json_serializable(self):
        self.assertDictEqual(
            AitoTableSchema({
                'col': AitoColumnTypeSchema(
                    AitoTextType(),
                    nullable=True,
                    analyzer=AitoAliasAnalyzerSchema('fi'),
                    link=AitoColumnLinkSchema('another', 'one')
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

    def test_link(self):
        table_schema = AitoTableSchema({
            'col1': AitoColumnTypeSchema(AitoIntType(), link=AitoColumnLinkSchema('first', 'link')),
            'col2': AitoColumnTypeSchema(AitoIntType(), link=AitoColumnLinkSchema('second', 'link'))
        })
        self.assertEqual(
            table_schema.links,
            {'col1': AitoColumnLinkSchema('first', 'link'), 'col2': AitoColumnLinkSchema('second', 'link')}
        )

    def test_disallow_column_names_with_whitespace_in_json(self):
        self.assertRaises(
            JsonValidationError,
            AitoTableSchema.from_deserialized_object,
            {
                'type': 'table',
                'columns': {'col with whitespace': {'type': 'Int'}}
            }
        )

    def test_disallow_column_names_with_newlines_in_json(self):
        self.assertRaises(
            JsonValidationError,
            AitoTableSchema.from_deserialized_object,
            {
                'type': 'table',
                'columns': { 'col\nwith\nnewlines': {'type': 'Int'}}
            }
        )

    def test_disallow_column_names_with_tabs_in_json(self):
        self.assertRaises(
            JsonValidationError,
            AitoTableSchema.from_deserialized_object,
            {
                    'type': 'table',
                    'columns': {'col\twith\ttabs': {'type': 'Int'}}
            }
        )

    def test_disallow_column_names_with_dots_in_json(self):
        self.assertRaises(
            JsonValidationError,
            AitoTableSchema.from_deserialized_object,
            {
                    'type': 'table',
                    'columns': {'col...with.tabs': {'type': 'Int'}}
            }
        )

    def test_disallow_column_names_with_whitespace_when_creating_object(self):
        self.assertRaises(
            ValueError,
            AitoTableSchema,
            {'col with whitespace': AitoColumnTypeSchema(AitoIntType())}
        )

    def test_disallow_column_names_with_newlines_when_creating_object(self):
        self.assertRaises(
            ValueError,
            AitoTableSchema,
            {'col\nwith\nnewlines': AitoColumnTypeSchema(AitoIntType())}
        )


    def test_disallow_column_names_with_tabs_when_creating_object(self):
        self.assertRaises(
            ValueError,
            AitoTableSchema,
            {'col\twith\ttabs': AitoColumnTypeSchema(AitoIntType())}
        )

    def test_disallow_column_names_with_dots_when_creating_object(self):
        self.assertRaises(
            ValueError,
            AitoTableSchema,
            {'col...with.tabs': AitoColumnTypeSchema(AitoIntType())}
        )

class TestAitoDatabaseSchema(BaseTestCase):
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
        ('missing_schema', {'tbl1': {'type': 'table', 'columns': {'col1': {'type': 'Boolean'}}}}, JsonValidationError),
        ('invalid_table', {'schema': {'tbl1': {'type': 'table', 'columns': {'col1': {}}}}}, JsonValidationError)
    ])
    def test_erroneous_from_deserialized_object(self, _, deserialized_obj, error):
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
                                link=AitoColumnLinkSchema('another', 'one')
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
            AitoDatabaseSchema({'tbl1': AitoTableSchema({'col1': AitoColumnTypeSchema(AitoBooleanType())})}),
            AitoDatabaseSchema({'tbl1': AitoTableSchema({'col1': AitoColumnTypeSchema(AitoBooleanType())})})
        )
        self.assertNotEqual(
            AitoDatabaseSchema({'tbl1': AitoTableSchema({'col1': AitoColumnTypeSchema(AitoBooleanType())})}),
            AitoDatabaseSchema({'tbl2': AitoTableSchema({'col1': AitoColumnTypeSchema(AitoBooleanType())})})
        )
        self.assertNotEqual(
            AitoDatabaseSchema({'tbl1': AitoTableSchema({'col1': AitoColumnTypeSchema(AitoBooleanType())})}),
            AitoDatabaseSchema({'tbl1': AitoTableSchema({'col2': AitoColumnTypeSchema(AitoBooleanType())})})
        )

    def test_link(self):
        db_schema = AitoDatabaseSchema(tables={
            'tbl1': AitoTableSchema(columns={
                'col1': AitoColumnTypeSchema(AitoIntType(), link=AitoColumnLinkSchema('tbl2', 'col1'))
            }),
            'tbl2': AitoTableSchema(columns={
                'col1': AitoColumnTypeSchema(AitoIntType()),
                'col2': AitoColumnTypeSchema(AitoStringType())
            })
        })
        self.assertEqual(
            db_schema.reachable_columns('tbl1'),
            ['col1', 'tbl2.col1', 'tbl2.col2']
        )
