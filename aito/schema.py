"""

Data structure for the Aito Database Schema
"""
import re
import itertools
import logging
from abc import abstractmethod, ABC
from collections import Counter
from csv import Sniffer, Error as csvError
from typing import List, Iterable, Dict, Optional
import re

import pandas as pd
from langdetect import detect_langs

from aito.utils._json_format import JsonFormat, JsonValidationError

LOG = logging.getLogger('AitoSchema')

class AitoSchema(JsonFormat, ABC):
    """The base class for Aito schema component

    """
    @property
    @abstractmethod
    def type(self):
        """the type of the schema component

        :rtype: str
        """
        pass

    @property
    @abstractmethod
    def comparison_properties(self) -> Iterable[str]:
        """properties of the schema object that will be used for comparison operation

        :rtype: Iterable[str]
        """
        pass

    def _compare_type(self, other) -> bool:
        try:
            return self.type == other.type
        except AttributeError:
            return False

    def _compare_properties(self, other) -> bool:
        return all([getattr(self, prop) == getattr(other, prop) for prop in self.comparison_properties])

    def __eq__(self, other):
        return self._compare_type(other) and self._compare_properties(other)

    table_name_pattern = r'[^\/\".$\r\n\s]+'
    column_name_pattern = r'[^\/\".$\r\n\s]+'
    column_link_pattern = f'{table_name_pattern}\.{column_name_pattern}'
    uuid_pattern = r'[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}'


class AitoAnalyzerSchema(AitoSchema, ABC):
    """the base class for `Aito Analyzer <https://aito.ai/docs/api/#schema-analyzer>`__
    """
    _supported_analyzer_type = ['char-ngram', 'delimiter', 'language', 'token-ngram']
    _lang_detect_code_to_aito_code = {
        'ko': 'cjk',
        'ja': 'cjk',
        'zh-cn': 'cjk',
        'zh-tw': 'cjk',
        'pt': 'pt-br'
    }
    _supported_language_analyzer_iso_code_to_name = {
        'ar': 'arabic',
        'bg': 'bulgarian',
        'ca': 'catalan',
        'cjk': 'cjk',
        'cs': 'czech',
        'da': 'danish',
        'de': 'german',
        'el': 'greek',
        'en': 'english',
        'es': 'spanish',
        'eu': 'basque',
        'fa': 'persian',
        'fi': 'finnish',
        'fr': 'french',
        'ga': 'irish',
        'gl': 'galician',
        'hi': 'hindi',
        'hu': 'hungarian',
        'hy': 'armenian',
        'id': 'indonesian',
        'it': 'italian',
        'lv': 'latvian',
        'nl': 'dutch',
        'no': 'norwegian',
        'pt': 'portuguese',
        'pt - br': 'brazilian',
        'ro': 'romanian',
        'ru': 'russian',
        'sv': 'swedish',
        'th': 'thai',
        'tr': 'turkish'
    }
    _supported_language_analyzer_aliases = [
        al for iso_code_and_lang in _supported_language_analyzer_iso_code_to_name.items() for al in iso_code_and_lang
    ]
    _supported_analyzer_aliases = ['standard', 'whitespace'] + _supported_language_analyzer_aliases

    @property
    def type(self):
        return 'analyzer'

    @property
    @abstractmethod
    def analyzer_type(self) -> str:
        """the type of the analyzer

        :rtype: str
        """
        pass

    @classmethod
    @abstractmethod
    def json_schema(cls):
        return {
            'oneOf': [
                {'type': 'string'},
                {
                    'type': 'object',
                    'properties': {'type': {'enum': cls._supported_analyzer_type}},
                    'required': ['type']
                }
            ]
        }

    @classmethod
    @abstractmethod
    def from_deserialized_object(cls, obj):
        cls.json_schema_validate(obj)

        if isinstance(obj, str):
            return AitoAliasAnalyzerSchema.from_deserialized_object(obj)
        else:
            analyzer_type = obj.get('type')
            if analyzer_type == 'language':
                return AitoLanguageAnalyzerSchema.from_deserialized_object(obj)
            if analyzer_type == 'delimiter':
                return AitoDelimiterAnalyzerSchema.from_deserialized_object(obj)
            if analyzer_type == 'char-ngram':
                return AitoCharNGramAnalyzerSchema.from_deserialized_object(obj)
            if analyzer_type == 'token-ngram':
                return AitoTokenNgramAnalyzerSchema.from_deserialized_object(obj)

    def __eq__(self, other):
        self._compare_type(other)
        if self.analyzer_type != other.analyzer_type:
            if {self.analyzer_type, other.analyzer_type} == {'alias', 'language'}:
                alias_analyzer, language_analyzer = (self, other) if self.analyzer_type == 'alias' else (other, self)
                language_analyzer_args = [
                    getattr(language_analyzer, key)
                    for key in ('use_default_stop_words', 'custom_stop_words', 'custom_key_words')
                ]
                # if language_analyzer has the same language and use the default parameters
                if alias_analyzer.alias == language_analyzer.language and language_analyzer_args == [False, [], []]:
                    return True
            if {self.analyzer_type, other.analyzer_type} == {'alias', 'delimiter'}:
                alias_analyzer, delimiter_analyzer = (self, other) if self.analyzer_type == 'alias' else (other, self)
                if alias_analyzer.alias == 'whitespace' and delimiter_analyzer.delimiter == ' ' and \
                        delimiter_analyzer.trim_white_space:
                    return True
            return False
        return self._compare_properties(other)

    @staticmethod
    def _try_sniff_delimiter(sample: str, candidates='-,;:|\t') -> Optional[str]:
        try:
            return str(Sniffer().sniff(sample, candidates).delimiter)
        except csvError:
            # deprio whitespace in case tokens has trailing or leading whitespace
            try:
                return str(Sniffer().sniff(sample, ' ').delimiter)
            except csvError:
                return None

    @classmethod
    def _infer_delimiter(cls, samples: Iterable[str]) -> Optional[str]:
        """returns the most common inferred delimiter from the samples"""
        inferred_delimiter_counter = Counter([cls._try_sniff_delimiter(smpl) for smpl in samples])
        LOG.debug(f'inferred delimiter and count: {inferred_delimiter_counter}')
        most_common_delimiter_and_count = inferred_delimiter_counter.most_common(1)[0]
        return most_common_delimiter_and_count[0]

    @classmethod
    def _infer_language(cls, samples: Iterable[str]) -> Optional[str]:
        """infer language from samples"""
        concatenated_sample_text = ' '.join(samples)
        try:
            detected_langs_and_probs = detect_langs(concatenated_sample_text)
        except:
            detected_langs_and_probs = None
        if detected_langs_and_probs:
            LOG.debug(f'inferred languages and probabilities: {detected_langs_and_probs}')
            most_probable_lang_and_prob = detected_langs_and_probs[0]
            if most_probable_lang_and_prob.prob > 0.9:
                most_probable_lang = most_probable_lang_and_prob.lang
                if most_probable_lang in cls._lang_detect_code_to_aito_code:
                    most_probable_lang = cls._lang_detect_code_to_aito_code[most_probable_lang]
                if most_probable_lang in cls._supported_language_analyzer_iso_code_to_name:
                    return most_probable_lang
        return None

    @classmethod
    def infer_from_samples(cls, samples: Iterable[str], max_sample_size: int = 10000):
        """Infer an analyzer from the given samples

        :param samples: iterable of sample
        :type samples: Iterable
        :param max_sample_size: at most first max_sample_size will be used for inference, defaults to 10000
        :type max_sample_size: int
        :return: inferred Analyzer or None if no analyzer is applicable
        :rtype: Optional[AitoAnalyzerSchema]
        """
        sliced_samples = list(itertools.islice(samples, max_sample_size))
        detected_delimiter = cls._infer_delimiter(sliced_samples)
        if detected_delimiter is None or detected_delimiter.isalnum():
            return None
        if detected_delimiter == ' ':
            detected_language = cls._infer_language(sliced_samples)
            return AitoLanguageAnalyzerSchema(language=detected_language) if detected_language \
                else AitoAliasAnalyzerSchema(alias='whitespace')
        return AitoDelimiterAnalyzerSchema(delimiter=detected_delimiter)


class AitoAliasAnalyzerSchema(AitoAnalyzerSchema):
    """Aito `AliasAnalyzer <https://aito.ai/docs/api/#schema-alias-analyzer>`__ schema
    """
    def __init__(self, alias: str):
        """

        :param alias: the alias of the analyzer, standardize to the language name if the alias is a language ISO code
        :type alias: str
        """
        if not isinstance(alias, str):
            raise TypeError("alias must be of type str")
        alias = alias.lower().strip()
        if alias not in self._supported_analyzer_aliases:
            raise ValueError(f"unsupported alias {alias}")
        if alias in self._supported_language_analyzer_iso_code_to_name:
            alias = self._supported_language_analyzer_iso_code_to_name[alias]
        self._alias = alias

    @property
    def analyzer_type(self) -> str:
        return 'alias'

    @property
    def alias(self) -> str:
        """the alias of the analyzer

        :rtype: str
        """
        return self._alias

    @property
    def comparison_properties(self) -> Iterable[str]:
        return ['alias']

    @classmethod
    def json_schema(cls):
        return {'type': 'string', 'enum': AitoAnalyzerSchema._supported_analyzer_aliases}

    @classmethod
    def from_deserialized_object(cls, obj: str):
        cls.json_schema_validate(obj)
        return cls(alias=obj)

    def to_json_serializable(self) -> str:
        return self._alias


class AitoLanguageAnalyzerSchema(AitoAnalyzerSchema):
    """Aito `LanguageAnalyzer <https://aito.ai/docs/api/#schema-language-analyzer>`__ schema

    """
    def __init__(
            self,
            language: str,
            use_default_stop_words: bool = None,
            custom_stop_words: List[str] = None,
            custom_key_words: List[str] = None
    ):
        """

        :param language: the name or the ISO code of the language
        :type language: str
        :param use_default_stop_words: filter the language default stop words
        :type use_default_stop_words: bool, defaults to False
        :param custom_stop_words: words that will be filtered
        :type custom_stop_words: List[str], defaults to []
        :param custom_key_words: words that will not be featurized
        :type custom_key_words: List[str], defaults to []
        """
        self.language = language
        self.use_default_stop_words = use_default_stop_words
        self.custom_stop_words = custom_stop_words
        self.custom_key_words = custom_key_words if custom_key_words is not None else []

    @property
    def analyzer_type(self) -> str:
        return 'language'

    @property
    def language(self) -> str:
        """the language of the analyzer

        :rtype: str
        """
        return self._language

    @language.setter
    def language(self, value):
        self.json_schema_validate_with_schema(value, self.json_schema()['properties']['language'])
        if value in self._supported_language_analyzer_iso_code_to_name:
            value = self._supported_language_analyzer_iso_code_to_name[value]
        self._language = value

    @property
    def use_default_stop_words(self) -> bool:
        """filter the language default stop words

        :rtype: bool
        """
        return self._use_default_stop_words

    @use_default_stop_words.setter
    def use_default_stop_words(self, value):
        schema = self.json_schema()['properties']['useDefaultStopWords']
        if value is None:
            value = schema['default']
        else:
            self.json_schema_validate_with_schema(value, schema)
        self._use_default_stop_words = value

    @property
    def custom_stop_words(self) -> List[str]:
        """list of words that will be filtered

        :rtype: List[str]
        """
        return self._custom_stop_words

    @custom_stop_words.setter
    def custom_stop_words(self, value):
        schema = self.json_schema()['properties']['customStopWords']
        if value is None:
            value = schema['default']
        else:
            self.json_schema_validate_with_schema(value, schema)
        self._custom_stop_words = value

    @property
    def custom_key_words(self) -> List[str]:
        """list of words that will not be featurized

        :rtype: List[str]
        """
        return self._custom_key_words

    @custom_key_words.setter
    def custom_key_words(self, value):
        schema = self.json_schema()['properties']['customKeyWords']
        if value is None:
            value = schema['default']
        else:
            self.json_schema_validate_with_schema(value, schema)
        self._custom_key_words = value

    @property
    def comparison_properties(self) -> Iterable[str]:
        return ['language', 'use_default_stop_words', 'custom_stop_words', 'custom_key_words']

    @classmethod
    def json_schema(cls):
        return {
            'type': 'object',
            'properties': {
                'type': {'enum': ['language']},
                'language': {'type': 'string', 'enum': AitoAnalyzerSchema._supported_language_analyzer_aliases},
                'useDefaultStopWords': {'type': 'boolean', 'default': False},
                'customStopWords': {'type': 'array', 'items': {'type': 'string'}, 'default': []},
                'customKeyWords': {'type': 'array', 'items': {'type': 'string'}, 'default': []},
            },
            'required': ['type', 'language'],
            'additionalProperties': False
        }

    @classmethod
    def from_deserialized_object(cls, obj: Dict):
        cls.json_schema_validate(obj)
        return cls(
            language=obj.get('language'),
            use_default_stop_words=obj.get('useDefaultStopWords'),
            custom_stop_words=obj.get('customStopWords'),
            custom_key_words=obj.get('customKeyWords')
        )

    def to_json_serializable(self) -> Dict:
        return {
            'type': 'language',
            'language': self.language,
            'useDefaultStopWords': self.use_default_stop_words,
            'customStopWords': self.custom_stop_words,
            'customKeyWords': self.custom_key_words
        }


class AitoDelimiterAnalyzerSchema(AitoAnalyzerSchema):
    """Aito `DelimiterAnalyzer <https://aito.ai/docs/api/#schema-delimiter-analyzer>`__ schema

    """
    def __init__(self, delimiter: str, trim_white_space: bool = None):
        """

        :param delimiter: the delimiter
        :type delimiter: str
        :param trim_white_space: trim leading and trailing whitespaces of the features
        :type trim_white_space: bool, defaults to True
        """
        self.delimiter = delimiter
        self.trim_white_space = trim_white_space

    @property
    def analyzer_type(self) -> str:
        return 'delimiter'

    @property
    def delimiter(self):
        """the delimiter of the analyzer

        :rtype: str
        """
        return self._delimiter

    @delimiter.setter
    def delimiter(self, value):
        self.json_schema_validate_with_schema(value, self.json_schema()['properties']['delimiter'])
        self._delimiter = value

    @property
    def trim_white_space(self) -> bool:
        """trim leading and trailing whitespaces of the features

        :rtype: bool
        """
        return self._trim_white_space

    @trim_white_space.setter
    def trim_white_space(self, value):
        schema = self.json_schema()['properties']['trimWhitespace']
        if value is None:
            value = schema['default']
        else:
            self.json_schema_validate_with_schema(value, schema)
        self._trim_white_space = value

    @property
    def comparison_properties(self) -> Iterable[str]:
        return ['delimiter', 'trim_white_space']

    @classmethod
    def json_schema(cls):
        return {
            'type': 'object',
            'properties': {
                'type': {'enum': ['delimiter']},
                'delimiter': {'type': 'string'},
                'trimWhitespace': {'type': 'boolean', 'default': True}
            },
            'required': ['type', 'delimiter'],
            'additionalProperties': False
        }

    def to_json_serializable(self):
        return {
            'type': 'delimiter',
            'delimiter': self.delimiter,
            'trimWhitespace': self.trim_white_space
        }

    @classmethod
    def from_deserialized_object(cls, obj):
        cls.json_schema_validate(obj)
        return cls(
            delimiter=obj.get('delimiter'),
            trim_white_space=obj.get('trimWhitespace')
        )


class AitoCharNGramAnalyzerSchema(AitoAnalyzerSchema):
    """Aito `CharNGramAnalyzer <https://aito.ai/docs/api/#schema-char-n-gram-analyzer>`__ schema

    """
    def __init__(self, min_gram: int, max_gram: int):
        """

        :param min_gram: the minimum length of characters in a feature
        :type min_gram: int
        :param max_gram: the maximum length of characters in a feature
        :type max_gram: int
        """
        self.min_gram = min_gram
        self.max_gram = max_gram

    @property
    def analyzer_type(self) -> str:
        return 'char-ngram'

    @property
    def comparison_properties(self) -> Iterable[str]:
        return ['min_gram', 'max_gram']

    def to_json_serializable(self):
        return {
            'type': 'char-ngram',
            'minGram': self.min_gram,
            'maxGram': self.max_gram
        }

    @property
    def min_gram(self):
        return self._min_gram

    @min_gram.setter
    def min_gram(self, value):
        self.json_schema_validate_with_schema(value, self.json_schema()['properties']['minGram'])
        self._min_gram = value

    @property
    def max_gram(self):
        return self._max_gram

    @max_gram.setter
    def max_gram(self, value):
        self.json_schema_validate_with_schema(value, self.json_schema()['properties']['maxGram'])
        self._max_gram = value

    @classmethod
    def json_schema(cls):
        return {
            'type': 'object',
            'properties': {
                'type': {'enum': ['char-ngram']},
                'minGram': {'type': 'integer', 'minimum': 1},
                'maxGram': {'type': 'integer', 'minimum': 1}
            },
            'required': ['type', 'minGram', 'maxGram'],
            'additionalProperties': False
        }

    @classmethod
    def from_deserialized_object(cls, obj):
        cls.json_schema_validate(obj)
        return cls(
            min_gram=obj.get('minGram'),
            max_gram=obj.get('maxGram')
        )


class AitoTokenNgramAnalyzerSchema(AitoAnalyzerSchema):
    """Aito `TokenNGramAnalyzer <https://aito.ai/docs/api/#schema-token-n-gram-analyzer>`__ schema

    """
    def __init__(self, source: AitoAnalyzerSchema, min_gram: int, max_gram: int, token_separator: str = None):
        """

        :param source: the source analyzer to generate features before being combined into n-grams
        :type source: AitoAnalyzerSchema
        :param min_gram: the minimum length of characters in a feature
        :type min_gram: int
        :param max_gram: the maximum length of characters in a feature
        :type max_gram: int
        :param token_separator: the string used to join the features of the source analyzer
        :type token_separator: str, defaults to ' '
        """
        self.source = source
        self.min_gram = min_gram
        self.max_gram = max_gram
        self.token_separator = token_separator

    @property
    def analyzer_type(self) -> str:
        return 'token-ngram'

    @property
    def source(self):
        """the source analyzer

        :rtype: AitoAnalyzerSchema
        """
        return self._source

    @source.setter
    def source(self, value):
        if not isinstance(value, AitoAnalyzerSchema):
            raise TypeError('source analyzer must be of type AitoAnalyzerSchema')
        self._source = value

    @property
    def min_gram(self):
        return self._min_gram

    @min_gram.setter
    def min_gram(self, value):
        self.json_schema_validate_with_schema(value, self.json_schema()['properties']['minGram'])
        self._min_gram = value

    @property
    def max_gram(self):
        return self._max_gram

    @max_gram.setter
    def max_gram(self, value):
        self.json_schema_validate_with_schema(value, self.json_schema()['properties']['maxGram'])
        self._max_gram = value

    @property
    def token_separator(self) -> str:
        """the string that will be used to join the features

        :rtype: str
        """
        return self._token_separator

    @token_separator.setter
    def token_separator(self, value):
        schema = self.json_schema()['properties']['tokenSeparator']
        if value is None:
            value = schema['default']
        else:
            self.json_schema_validate_with_schema(value, schema)
        self._token_separator = value

    @property
    def comparison_properties(self) -> Iterable[str]:
        return ['source', 'min_gram', 'max_gram', 'token_separator']

    @classmethod
    def json_schema(cls):
        return {
            'type': 'object',
            'properties': {
                'type': {'enum': ['token-ngram']},
                'source': {'oneOf': [{'type': 'string'}, {'type': 'object'}]},
                'minGram': {'type': 'integer', 'minimum': 1},
                'maxGram': {'type': 'integer', 'minimum': 1},
                'tokenSeparator': {'type': 'string', 'default': ' '}
            },
            'required': ['type', 'source', 'minGram', 'maxGram'],
            'additionalProperties': False
        }

    def to_json_serializable(self):
        return {
            'type': 'token-ngram',
            'source': self.source.to_json_serializable(),
            'minGram': self.min_gram,
            'maxGram': self.max_gram,
            'tokenSeparator': self.token_separator
        }

    @classmethod
    def from_deserialized_object(cls, obj):
        cls.json_schema_validate(obj)
        return cls(
            source=AitoAnalyzerSchema.from_deserialized_object(obj.get('source')),
            min_gram=obj.get('minGram'),
            max_gram=obj.get('maxGram'),
            token_separator=obj.get('tokenSeparator')
        )

class DataSeriesProperties :
    """DataSeriesProperties is an utility class that is used to inspect pandas data series
       properties and infer the Aito data type based on it. It checks the maximum and
       minimum value and it will convert integers into string, if Aito does support the
       numeric range.
    """

    _pandas_dtypes_name_to_aito_type = {
        'string': 'Text',
        'bytes': 'String',
        'floating': 'Decimal',
        'integer': 'Int',
        'mixed-integer': 'Text',
        'mixed-integer-float': 'Decimal',
        'decimal': 'Decimal',
        'complex': 'Decimal',
        'categorical': 'String',
        'boolean': 'Boolean',
        'datetime64': 'String',
        'datetime': 'String',
        'date': 'String',
        'timedelta64': 'String',
        'timedelta': 'String',
        'time': 'String',
        'period': 'String',
        'mixed': 'Text',
        'empty': 'String'
    }

    MAX_INT_VALUE = 2147483647
    MIN_INT_VALUE = -2147483648

    def __init__(self, pandas_dtype: str, min_value, max_value):
        self.pandas_dtype = pandas_dtype
        self.min_value = min_value
        self.max_value = max_value

        if pandas_dtype == 'integer' and (max_value > self.MAX_INT_VALUE or min_value < self.MIN_INT_VALUE):
            self.target_aito_dtype = 'String' # neither pandas (!) or Aito supports integers this large
        else:
            self.target_aito_dtype = DataSeriesProperties.pandas_dtype_to_aito_dtype(pandas_dtype)


    @classmethod
    def pandas_dtype_to_aito_dtype(cls, pandas_dtype) -> str:
        """ Converts a pandas data type into Aito data type

        :rtype: str
        """
        return cls._pandas_dtypes_name_to_aito_type[pandas_dtype]

    def to_data_type_schema(self) -> 'AitoDataTypeSchema':
        """ Provides the AitoDataTypeSchema that is inferred for this data series
        """
        return AitoDataTypeSchema.from_deserialized_object(self.target_aito_dtype)

    @classmethod
    def _infer_from_pandas_series(cls, series: pd.Series, max_sample_size: int = 100000) -> 'DataSeriesProperties':
        """Infer aito column type from a Pandas Series

        :param series: input Pandas Series
        :type series: pd.Series
        :param max_sample_size: maximum sample size that will be used for type inference, defaults to 100000
        :type max_sample_size: int, optional
        :raises Exception: fail to infer type
        :return: inferred Aito type
        :rtype: str
        """
        sampled_values = series.values if len(series) < max_sample_size else series.sample(max_sample_size).values

        if len(series) == series.isna().sum(): # pandas will infer empty series as floating point as default
            inferred_dtype = 'empty'
        else:
            inferred_dtype = pd.api.types.infer_dtype(sampled_values)

        lower_bound = None
        upper_bound = None
        if inferred_dtype == 'integer':
            lower_bound = sampled_values.min()
            upper_bound = sampled_values.max()
            # See integer MAX_VALUE and MIN_VALUE in https://docs.oracle.com/javase/8/docs/api/constant-values.html
            LOG.debug(f'inferred pandas dtype: {inferred_dtype}')
        if inferred_dtype not in cls._pandas_dtypes_name_to_aito_type:
            LOG.debug(f'failed to convert pandas dtype {inferred_dtype} to aito dtype')
            raise Exception(f'failed to infer aito data type')

        return DataSeriesProperties(inferred_dtype, lower_bound, upper_bound)


class AitoDataTypeSchema(AitoSchema, ABC):
    """The base class for Aito DataType"""

    _supported_data_types = ["Boolean", "Decimal", "Int", "String", "Text"]

    def __init__(self, aito_dtype: str):
        """

        :param aito_dtype: the Aito data type
        :type aito_dtype: str
        """
        if aito_dtype not in self._supported_data_types:
            raise TypeError(
                f"unrecognized data type `{aito_dtype}`. "
                f"Data type must be one of {'|'.join(self._supported_data_types)}"
            )
        self._aito_dtype = aito_dtype

    @property
    def type(self):
        return 'dtype'

    @property
    def aito_dtype(self) -> str:
        """the data type

        :rtype: str
        """
        return self._aito_dtype

    @property
    def is_string(self) -> bool:
        """returns True if the data type is String

        :rtype: bool
        """
        return self._aito_dtype == 'String'

    @property
    def is_text(self) -> bool:
        """returns True if the data type is Text

        :rtype: bool
        """
        return self._aito_dtype == 'Text'

    @property
    def is_bool(self) -> bool:
        """returns True if the data type is Boolean

        :rtype: bool
        """
        return self._aito_dtype == 'Boolean'

    @property
    def is_int(self) -> bool:
        """returns True if the data type is Int

        :rtype: bool
        """
        return self._aito_dtype == 'Int'

    @property
    def is_decimal(self) -> bool:
        """returns True if the data type is Decimal

        :rtype: bool
        """
        return self._aito_dtype == 'Decimal'

    def to_json_serializable(self) -> str:
        return self._aito_dtype

    @abstractmethod
    def to_python_type(self):
        """the equivalent python type
        """
        pass

    @classmethod
    def json_schema(cls):
        return {'type': 'string', 'enum': cls._supported_data_types}

    @classmethod
    def from_deserialized_object(cls, obj: str) -> 'AitoDataTypeSchema':
        cls.json_schema_validate(obj)
        if obj == 'Boolean':
            return AitoBooleanType()
        if obj == 'Int':
            return AitoIntType()
        if obj == 'Decimal':
            return AitoDecimalType()
        if obj == 'String':
            return AitoStringType()
        if obj == 'Text':
            return AitoTextType()

    @property
    def comparison_properties(self) -> Iterable[str]:
        return ['aito_dtype']

    @classmethod
    def _infer_from_pandas_series(cls, series: pd.Series, max_sample_size: int = 100000) -> 'AitoDataTypeSchema':
        """Infer aito column type from a Pandas Series

        :param series: input Pandas Series
        :type series: pd.Series
        :param max_sample_size: maximum sample size that will be used for type inference, defaults to 100000
        :type max_sample_size: int, optional
        :raises Exception: fail to infer type
        :return: inferred Aito type
        :rtype: str
        """
        return DataSeriesProperties._infer_from_pandas_series(series, max_sample_size).to_data_type_schema()

    @classmethod
    def infer_from_samples(cls, samples: Iterable, max_sample_size: int = 100000) -> 'AitoDataTypeSchema':
        """infer AitoDataType from the given samples

        :param samples: iterable of sample
        :type samples: Iterable
        :param max_sample_size: at most first max_sample_size will be used for inference, defaults to 100000
        :type max_sample_size: int
        :return: inferred Aito column type
        :rtype: str
        """
        try:
            casted_samples = pd.Series(itertools.islice(samples, max_sample_size))
        except Exception as e:
            LOG.debug(f'failed to cast samples ({list(itertools.islice(samples, 10))}, ...)  to pandas Series: {e}')
            raise Exception(f'failed to infer aito type')
        return cls._infer_from_pandas_series(casted_samples)


class AitoBooleanType(AitoDataTypeSchema):
    """Aito `Boolean Type <https://aito.ai/docs/api/#schema-boolean-type>`__"""
    def __init__(self):
        super().__init__('Boolean')

    def to_python_type(self):
        return bool


class AitoIntType(AitoDataTypeSchema):
    """Aito `Int Type <https://aito.ai/docs/api/#schema-int-type>`__"""
    def __init__(self):
        super().__init__('Int')

    def to_python_type(self):
        return int


class AitoDecimalType(AitoDataTypeSchema):
    """Aito `Decimal Type <https://aito.ai/docs/api/#schema-decimal-type>`__"""
    def __init__(self):
        super().__init__('Decimal')

    def to_python_type(self):
        return float


class AitoStringType(AitoDataTypeSchema):
    """Aito `String Type <https://aito.ai/docs/api/#schema-string-type>`__"""
    def __init__(self):
        super().__init__('String')

    def to_python_type(self):
        return str

class AitoTextType(AitoDataTypeSchema):
    """Aito `Text Type <https://aito.ai/docs/api/#schema-text-type>`__"""
    def __init__(self):
        super().__init__('Text')

    def to_python_type(self):
        return str


class AitoColumnLinkSchema(AitoSchema):
    """Link to a column of another table"""

    # Essentially a combination of the table- and column-regexes
    _column_link_regex = f'^{AitoSchema.column_link_pattern}$'

    def __init__(self, table_name: str, column_name: str):
        """

        :param table_name: the name of the linked table
        :type table_name: str
        :param column_name: the name of the linked column
        :type column_name: str
        """
        if not bool(re.match(AitoDatabaseSchema._full_table_name_regex, table_name)):
            raise ValueError(f'Linked table name in "{table_name}.{column_name}" is not valid')
        if not bool(re.match(AitoTableSchema._column_name_regex, column_name)):
            raise ValueError(f'Linked column name in "{table_name}.{column_name}" is not valid')

        self._table_name = table_name
        self._column_name = column_name

    @property
    def type(self):
        return 'columnLink'

    @property
    def table_name(self) -> str:
        """the name of the linked table

        :rtype: str
        """
        return self._table_name

    @property
    def column_name(self) -> str:
        """the name of the linked column

        :rtype: str
        """
        return self._column_name

    def to_json_serializable(self):
        return f"{self.table_name}.{self.column_name}"

    @classmethod
    def json_schema(cls):
        return {'type': 'string', 'pattern': AitoColumnLinkSchema._column_link_regex}

    @classmethod
    def from_deserialized_object(cls, obj: str) -> "AitoColumnLinkSchema":
        cls.json_schema_validate(obj)
        parts = obj.split('.')
        return cls(parts[0], parts[1])

    @property
    def comparison_properties(self) -> Iterable[str]:
        return ['table_name', 'column_name']


class AitoColumnTypeSchema(AitoSchema):
    """Aito `ColumnType <https://aito.ai/docs/api/#schema-column-type>`__ schema
    """
    def __init__(
            self,
            data_type: AitoDataTypeSchema,
            nullable: bool = None,
            link: AitoColumnLinkSchema = None,
            analyzer: AitoAnalyzerSchema = None
    ):
        """

        :param data_type: the type of the column
        :type data_type: AitoDataTypeSchema
        :param nullable: when true, `null` values are allowed
        :type nullable: bool, default to False
        :param link: path to a linked column
        :type link: AitoColumnLink, optional
        :param analyzer: the analyzer of the column if the column is of type Text
        :type analyzer: AnalyzerSchema, optional
        """
        self._analyzer = None  # FIXME: Hacky way to avoid data type dependency on analyzer
        self.data_type = data_type
        self.nullable = nullable
        self.link = link
        self.analyzer = analyzer

    @property
    def type(self):
        return 'column'

    @property
    def data_type(self):
        """The data type of the column

        :rtype: AitoDataTypeSchema
        """
        return self._data_type

    @data_type.setter
    def data_type(self, value):
        if not isinstance(value, AitoDataTypeSchema):
            raise TypeError('data_type must be of type AitoDataTypeSchema')
        if self.analyzer and not value.is_text:
            raise ValueError(f"{value} does not support analyzer")
        self._data_type = value

    @property
    def nullable(self) -> bool:
        """returns True if the column allow `null` value

        :rtype: bool
        """
        return self._nullable

    @nullable.setter
    def nullable(self, value):
        schema = self.json_schema()['properties']['nullable']
        if value is None:
            value = schema['default']
        else:
            self.json_schema_validate_with_schema(value, schema)
        self._nullable = value

    def to_conversion(self):
        if self._nullable:
            tpt = self._data_type.to_python_type()
            return lambda x: None if pd.isna(x) else tpt(x)
        else:
            return self._data_type.to_python_type()

    @property
    def link(self):
        """the link of the column

        :rtype: AitoColumnLinkSchema
        """
        return self._link

    @link.setter
    def link(self, value):
        if value is not None:
            if not isinstance(value, AitoColumnLinkSchema):
                raise TypeError('link must be of type AitoColumnLinkSchema')
        self._link = value

    @property
    def analyzer(self):
        """the analyzer of the column

        :rtype: AitoAnalyzerSchema
        """
        return self._analyzer

    @analyzer.setter
    def analyzer(self, value):
        if value is not None:
            if not self.data_type.is_text:
                raise ValueError(f"{self.data_type} does not support analyzer")
            if not isinstance(value, AitoAnalyzerSchema):
                raise TypeError('analyzer must be of type AitoAnalyzerSchema')
        self._analyzer = value

    @property
    def has_link(self) -> bool:
        """return true if the column is linked to another column"""
        return self.link is not None

    @property
    def comparison_properties(self) -> Iterable[str]:
        return ['data_type', 'nullable', 'link', 'analyzer']

    def to_json_serializable(self):
        data = {
            'type': self.data_type.to_json_serializable(),
            'nullable': self.nullable,
            'link': self.link.to_json_serializable() if self.link is not None else None,
            'analyzer': self.analyzer.to_json_serializable() if self.analyzer else self.analyzer
        }
        return {
            key: val for key, val in data.items() if val is not None
        }

    @classmethod
    def json_schema(cls):
        return {
            'type': 'object',
            'properties': {
                'type': {},
                'nullable': {'type': 'boolean', 'default': False},
                'link': {},
                'analyzer': {}
            },
            'required': ['type'],
            'additionalProperties': False
        }

    @classmethod
    def from_deserialized_object(cls, obj: Dict):
        cls.json_schema_validate(obj)
        data_type = AitoDataTypeSchema.from_deserialized_object(obj.get('type'))
        analyzer_data = obj.get('analyzer')
        analyzer = AitoAnalyzerSchema.from_deserialized_object(analyzer_data) if analyzer_data is not None else None
        link_data = obj.get('link')
        link = AitoColumnLinkSchema.from_deserialized_object(link_data) if link_data is not None else None
        return cls(data_type=data_type, nullable=obj.get('nullable'), link=link, analyzer=analyzer)

    @classmethod
    def _infer_from_pandas_series(cls, series: pd.Series, max_sample_size: int = 100000) -> 'AitoColumnTypeSchema':
        samples = series if len(series) < max_sample_size else series.sample(max_sample_size)

        col_nullable = True if series.isna().sum() > 0 else False
        col_analyzer = None
        col_aito_type = AitoDataTypeSchema._infer_from_pandas_series(samples, max_sample_size)

        if col_aito_type.is_text:
            col_analyzer = AitoAnalyzerSchema.infer_from_samples(samples.dropna().__iter__())
            if not col_analyzer:
                col_aito_type = AitoStringType()
        return cls(
            data_type=col_aito_type, nullable=col_nullable, analyzer=col_analyzer
        )

    @classmethod
    def infer_from_samples(cls, samples: Iterable, max_sample_size: int = 100000) -> 'AitoColumnTypeSchema':
        """infer AitoColumnType from the given samples

        :param samples: iterable of sample
        :type samples: Iterable
        :param max_sample_size: at most first max_sample_size will be used for inference, defaults to 100000
        :type max_sample_size: int
        :return: inferred Aito column type
        :rtype: str
        """
        try:
            casted_samples = pd.Series(itertools.islice(samples, max_sample_size))
        except Exception as e:
            LOG.debug(f'failed to cast samples ({list(itertools.islice(samples, 10))}, ...)  to pandas Series: {e}')
            raise Exception(f'failed to infer column type')
        return cls._infer_from_pandas_series(casted_samples)


class AitoTableSchema(AitoSchema):
    """Aito Table schema contains the columns and their schema

    Can be thought of as a dict-like container for :class:`.AitoColumnTypeSchema` objects

    Infer AitoTableSchema from a Pandas DataFrame

    >>> import pandas as pd
    >>> df = pd.DataFrame(data={'id': [1, 2], 'name': ['Neil', 'Buzz']})
    >>> table_schema = AitoTableSchema.infer_from_pandas_data_frame(df)
    >>> print(table_schema.to_json_string(indent=2, sort_keys=True))
    {
      "columns": {
        "id": {
          "nullable": false,
          "type": "Int"
        },
        "name": {
          "nullable": false,
          "type": "String"
        }
      },
      "type": "table"
    }

    >>> print(table_schema['name'])
    {"type": "String", "nullable": false}

    change the property of a column

    >>> table_schema['name'].nullable = True
    >>> print(table_schema['name'])
    {"type": "String", "nullable": true}

    add a column to the table schema

    >>> table_schema['description'] = AitoColumnTypeSchema(AitoTextType(), nullable=True)
    >>> print(table_schema.to_json_string(indent=2, sort_keys=True))
    {
      "columns": {
        "description": {
          "nullable": true,
          "type": "Text"
        },
        "id": {
          "nullable": false,
          "type": "Int"
        },
        "name": {
          "nullable": true,
          "type": "String"
        }
      },
      "type": "table"
    }

    delete a column in the table schema

    >>> del table_schema['description']
    >>> table_schema.columns
    ['id', 'name']

    check if a column exist in the table schema

    >>> 'id' in table_schema
    True

    iterate over the table schema
    >>> for col in table_schema:
    ...     table_schema[col].nullable = False
    """

    _column_name_regex = f'^{AitoSchema.column_name_pattern}$'

    def __init__(self, columns: Dict[str, AitoColumnTypeSchema]):
        """

        :param columns: a dictionary of the table's columns' name and schema
        :type columns: Dict[str, AitoColumnTypeSchema]
        """
        invalid_columns = []
        for key in columns.keys():
            if not bool(re.match(AitoTableSchema._column_name_regex, key)):
                invalid_columns.append(key)

        if len(invalid_columns) > 0:
            raise ValueError(f'Column names are not valid: {invalid_columns}')

        self._columns = columns

    @property
    def type(self):
        return 'table'

    @property
    def comparison_properties(self) -> Iterable[str]:
        return ['columns_schemas']

    @property
    def columns(self) -> List[str]:
        """list of the table's columns name

        :rtype: List[str]
        """
        return sorted(list(self._columns.keys()))

    @property
    def columns_schemas(self) -> Dict[str, AitoColumnTypeSchema]:
        """a dictionary contains the names of the table columns and its corresponding schemas

        :rtype: Dict[str, AitoColumnTypeSchema]
        """
        return self._columns

    @property
    def links(self) -> Optional[Dict[str, AitoColumnLinkSchema]]:
        """a dictionary contains the names of the table columns and its corresponding link

        :rtype: Dict[str, AitoColumnLinkSchema]
        """
        return {col_name: col_schema.link for col_name, col_schema in self._columns.items() if col_schema.has_link}

    def __getitem__(self, column_name: str):
        """get the column schema of the specified column name
        """
        if not isinstance(column_name, str):
            raise TypeError('the name of the column must be of type string')
        if column_name not in self._columns:
            raise KeyError(f'column `{column_name}` does not exist')
        return self._columns[column_name]

    def __setitem__(self, column_name: str, value):
        """update the column schema of the specified column name
        """
        if not isinstance(column_name, str):
            raise TypeError('the name of the column must be of type string')

        if not bool(re.match(AitoTableSchema._column_name_regex, column_name)):
            raise ValueError(f'The column name {column_name} is not valid')

        if not isinstance(value, AitoColumnTypeSchema):
            raise TypeError('column schema must be of type AitoColumnTypeSchema')
        self._columns[column_name] = value

    def __delitem__(self, key):
        """delete a column
        """
        if key not in self._columns:
            raise KeyError(f'column `{key}` does not exist')
        del self._columns[key]

    def __contains__(self, item):
        return item in self._columns

    def __iter__(self):
        return iter(self._columns)

    def has_column(self, column_name: str) -> bool:
        """check if the table has the specified column

        :param column_name: the name of the column
        :type column_name: str
        :return: true if the table has the specified column
        :rtype: bool
        """
        return column_name in self._columns

    @classmethod
    def json_schema(cls):
        pattern = AitoTableSchema._column_name_regex
        return {
            'type': 'object',
            'properties': {
                'type': {'const': 'table'},
                'columns': {
                    'type': 'object',
                    'minProperties': 1,
                    'propertyNames': {'pattern': pattern}
                }
            },
            'required': ['type', 'columns'],
            'additionalProperties': False
        }

    def to_json_serializable(self):
        return {
            "type": "table",
            "columns": {
                col_name: col_type.to_json_serializable() for col_name, col_type in self._columns.items()
            }
        }

    @classmethod
    def from_deserialized_object(cls, obj):
        cls.json_schema_validate(obj)

        columns_data = obj.get('columns')
        columns = {
            col_name: AitoColumnTypeSchema.from_deserialized_object(col_data)
            for col_name, col_data in columns_data.items()
        }
        return cls(columns=columns)

    @classmethod
    def infer_from_pandas_data_frame(cls, df: pd.DataFrame, max_sample_size: int = 100000) -> 'AitoTableSchema':
        """Infer a TableSchema from a Pandas DataFrame

        :param df: input Pandas DataFrame
        :type df: pd.DataFrame
        :param max_sample_size: maximum number of rows that will be used for inference, defaults to 100000
        :type max_sample_size: int, optional
        :raises Exception: an error occurred during column type inference
        :return: inferred table schema
        :rtype: Dict
        """
        LOG.debug('inferring table schema...')
        columns_schema = {}
        for col in df.columns.values:
            LOG.debug(f'inferring column {col}...')
            col_type_schema = AitoColumnTypeSchema._infer_from_pandas_series(df[col], max_sample_size)
            columns_schema[col] = col_type_schema
        table_schema = cls(columns=columns_schema)
        LOG.debug('inferred table schema')
        return table_schema


class AitoDatabaseSchema(AitoSchema):
    """Aito Database Schema

    Can be thought of as a dict-like container for :class:`.AitoTableSchema` objects
    """

    _full_table_name_regex = f"^{AitoSchema.table_name_pattern}$"

    def __init__(self, tables: Dict[str, AitoTableSchema]):
        invalid_tables = []
        for key in tables.keys():
            if not bool(re.match(AitoDatabaseSchema._full_table_name_regex, key)):
                invalid_tables.append(key)

        if len(invalid_tables) > 0:
            raise ValueError(f'Column names are not valid: {invalid_tables}')

        self._tables = tables

    def _validate_links(self):
        for tbl_name, tbl_schema in self._tables.items():
            for col_name, col_schema in tbl_schema.columns_schemas.items():
                if col_schema.has_link:
                    link = col_schema.link
                    linked_tbl_name = link.table_name
                    if not self.has_table(linked_tbl_name):
                        raise ValueError(f'column {tbl_name}.{col_name} link to non-exist table {linked_tbl_name}')

                    linked_table_schema = self._tables[linked_tbl_name]
                    linked_col_name = link.column_name
                    if linked_col_name not in linked_table_schema:
                        raise ValueError(f'column {tbl_name}.{col_name} link to non-exist column '
                                         f'{linked_tbl_name}.{linked_col_name}')

    @property
    def type(self):
        return 'database'

    @property
    def tables(self) -> List[str]:
        """a list contains the names of the database's tables

        :rtype:
        """
        return list(self._tables.keys())

    @property
    def tables_schemas(self) -> Dict[str, AitoTableSchema]:
        """a dictionary contains the name of the database's tables and its corresponding schemas

        :rtype: Dict[str, AitoTableSchema]
        """
        return self._tables

    @property
    def comparison_properties(self) -> Iterable[str]:
        return ['tables_schemas']

    def to_json_serializable(self):
        return {
            "schema": {
                tbl_name: tbl_schema.to_json_serializable() for tbl_name, tbl_schema in self._tables.items()
            }
        }

    def __getitem__(self, table_name: str) -> AitoTableSchema:
        """get the schema of the specified table
        """
        if not isinstance(table_name, str):
            raise TypeError('the name of the column must be of type string')
        if table_name not in self._tables:
            raise KeyError(f'table `{table_name}` does not exist')
        return self._tables[table_name]

    def __setitem__(self, table_name: str, value):
        """update the schema of the specified table
        """
        if not isinstance(table_name, str):
            raise TypeError('the name of the table must be of type string')

        if not bool(re.match(AitoDatabaseSchema._full_table_name_regex, table_name)):
            raise ValueError(f'Table name {table_name} is not valid')

        if not isinstance(value, AitoTableSchema):
            raise TypeError('the table schema must be of type AitoTableSchema')
        self._tables[table_name] = value

    def __delitem__(self, key):
        """delete a column
        """
        if key not in self._tables:
            raise KeyError(f'table `{key}` does not exist')
        del self._tables[key]

    def __contains__(self, item):
        return item in self._tables

    def __iter__(self):
        return iter(self._tables)

    def has_table(self, table_name: str) -> bool:
        """check if the database has the specified table

        :param table_name: the name of the column
        :type table_name: str
        :return: true if the table has the specified column
        :rtype: bool
        """
        return table_name in self._tables

    @classmethod
    def json_schema(cls):
        return {
            'type': 'object',
            'properties': {
                'schema': {
                    'type': 'object',
                    'propertyNames': {'pattern': AitoDatabaseSchema._full_table_name_regex}
                }
            },
            'required': ['schema'],
            'additionalProperties': False
        }

    @classmethod
    def from_deserialized_object(cls, obj):
        cls.json_schema_validate(obj)
        schema_data = obj.get('schema')
        tables = {
            tbl_name: AitoTableSchema.from_deserialized_object(tbl_data) for tbl_name, tbl_data in schema_data.items()
        }
        return cls(tables=tables)

    def reachable_columns(self, table_name) -> List[str]:
        """return the name of the columns that can be reached from the specified table if the table has link,
        including the columns of the table

        :param table_name: the name of the table
        :type table_name: str
        :return: list of linked columns
        :rtype: List[str]
        """
        table_schema = self.__getitem__(table_name)
        linked_columns = table_schema.columns
        for col_name, col_schema in table_schema.columns_schemas.items():
            if col_schema.has_link:
                linked_table_name = col_schema.link.table_name
                linked_table_schema = self.__getitem__(linked_table_name)
                linked_columns += [
                    f'{linked_table_name}.{linked_tbl_col}' for linked_tbl_col in linked_table_schema.columns
                ]
        return linked_columns
