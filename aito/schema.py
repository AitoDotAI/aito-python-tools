"""

Data structure for the Aito Database Schema
"""

import itertools
import json
import logging
from abc import abstractmethod, ABC
from collections import Counter
from csv import Sniffer, Error as csvError
from typing import List, Iterable, Dict, Optional

import pandas as pd
from jsonschema import Draft7Validator
from langdetect import detect_langs

from aito.exceptions import ValidationError

LOG = logging.getLogger('AitoSchema')


class AitoSchema(ABC):
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
    def json_schema(self):
        """the JSON schema of the schema component

        :rtype: Dict
        """
        pass

    @abstractmethod
    def to_json_serializable(self):
        """convert the AitoSchema object to a json serializable object (dict, in most case)
        """
        pass

    def to_json_string(self, **kwargs):
        """the AitoSchema object as a JSON string

        :param kwargs: the keyword arguments for json.dumps method
        :rtype: str
        """
        return json.dumps(self.to_json_serializable(), **kwargs)

    @classmethod
    @abstractmethod
    def from_deserialized_object(cls, obj):
        """create an AitoSchema object from a JSON deserialized object
        """
        pass

    @classmethod
    def from_json_string(cls, json_string: str, **kwargs):
        """create an AitoSchema object from a JSON string

        :param json_string: the JSON string
        :type json_string: str
        :param kwargs: the keyword arguments for json.loads method
        """
        return json.loads(json_string, object_hook=cls.from_deserialized_object, **kwargs)

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

    def __str__(self):
        return json.dumps(self.to_json_serializable())

    def __repr__(self):
        return json.dumps(self.to_json_serializable(), indent=2, sort_keys=True)

    @classmethod
    def _json_schema_validate(cls, obj, schema):
        validator = Draft7Validator(schema)
        for err in validator.iter_errors(obj):
            raise ValidationError(cls.__name__, err, LOG) from None

    def _raise_validation_error(self, e):
        raise ValidationError(self.__class__.__name__, e, LOG)


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

    alias_analyzer_json_schema = {'type': 'string', 'enum': _supported_analyzer_aliases}
    language_analyzer_json_schema = {
        'type': 'object',
        'properties': {
            'type': {'enum': ['language']},
            'language': {'type': 'string', 'enum': _supported_language_analyzer_aliases},
            'useDefaultStopWords': {'type': 'boolean', 'default': False},
            'customStopWords': {'type': 'array', 'items': {'type': 'string'}, 'default': []},
            'customKeyWords': {'type': 'array', 'items': {'type': 'string'}, 'default': []},
        },
        'required': ['type', 'language'],
        'additionalProperties': False
    }

    delimiter_analyzer_json_schema = {
        'type': 'object',
        'properties': {
            'type': {'enum': ['delimiter']},
            'delimiter': {'type': 'string'},
            'trimWhitespace': {'type': 'boolean', 'default': True}
        },
        'required': ['type', 'delimiter'],
        'additionalProperties': False
    }

    token_ngram_analyzer_json_schema = {
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

    char_ngram_analyzer_json_schema = {
        'type': 'object',
        'properties': {
            'type': {'enum': ['char-ngram']},
            'minGram': {'type': 'integer', 'minimum': 1},
            'maxGram': {'type': 'integer', 'minimum': 1}
        },
        'required': ['type', 'minGram', 'maxGram'],
        'additionalProperties': False
    }

    json_schema = {'oneOf': [
        alias_analyzer_json_schema,
        language_analyzer_json_schema,
        delimiter_analyzer_json_schema,
        token_ngram_analyzer_json_schema,
        char_ngram_analyzer_json_schema
    ]}

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
    def from_deserialized_object(cls, obj):
        cls._json_schema_validate(obj, cls.json_schema)

        if isinstance(obj, str):
            return AitoAliasAnalyzerSchema.from_deserialized_object(obj)
        else:
            analyzer_type = obj.get('type')
            if analyzer_type not in cls._supported_analyzer_type:
                raise ValidationError(cls.__name__, f'unsupported analyzer of type {analyzer_type}', LOG)
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
        detected_langs_and_probs = detect_langs(concatenated_sample_text)
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

    json_schema = AitoAnalyzerSchema.alias_analyzer_json_schema

    def __init__(self, alias: str):
        """

        :param alias: the alias of the analyzer, standardize to the language name if the alias is a language ISO code
        :type alias: str
        """
        if not isinstance(alias, str):
            self._raise_validation_error("alias must be of type str")
        alias = alias.lower().strip()
        if alias not in self._supported_analyzer_aliases:
            self._raise_validation_error(f"unsupported alias {alias}")
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
    def from_deserialized_object(cls, obj: str):
        cls._json_schema_validate(obj, cls.json_schema)
        return cls(alias=obj)

    def to_json_serializable(self) -> str:
        return self._alias


class AitoLanguageAnalyzerSchema(AitoAnalyzerSchema):
    """Aito `LanguageAnalyzer <https://aito.ai/docs/api/#schema-language-analyzer>`__ schema

    """
    json_schema = AitoAnalyzerSchema.language_analyzer_json_schema

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
        self._json_schema_validate(value, self.json_schema['properties']['language'])
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
        schema = self.json_schema['properties']['useDefaultStopWords']
        if value is None:
            value = schema['default']
        else:
            self._json_schema_validate(value, schema)
        self._use_default_stop_words = value

    @property
    def custom_stop_words(self) -> List[str]:
        """list of words that will be filtered

        :rtype: List[str]
        """
        return self._custom_stop_words

    @custom_stop_words.setter
    def custom_stop_words(self, value):
        schema = self.json_schema['properties']['customStopWords']
        if value is None:
            value = schema['default']
        else:
            self._json_schema_validate(value, schema)
        self._custom_stop_words = value

    @property
    def custom_key_words(self) -> List[str]:
        """list of words that will not be featurized

        :rtype: List[str]
        """
        return self._custom_key_words

    @custom_key_words.setter
    def custom_key_words(self, value):
        schema = self.json_schema['properties']['customKeyWords']
        if value is None:
            value = schema['default']
        else:
            self._json_schema_validate(value, schema)
        self._custom_key_words = value

    @property
    def comparison_properties(self) -> Iterable[str]:
        return ['language', 'use_default_stop_words', 'custom_stop_words', 'custom_key_words']

    @classmethod
    def from_deserialized_object(cls, obj: Dict):
        cls._json_schema_validate(obj, cls.json_schema)
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

    json_schema = AitoAnalyzerSchema.delimiter_analyzer_json_schema

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
        self._json_schema_validate(value, self.json_schema['properties']['delimiter'])
        self._delimiter = value

    @property
    def trim_white_space(self) -> bool:
        """trim leading and trailing whitespaces of the features

        :rtype: bool
        """
        return self._trim_white_space

    @trim_white_space.setter
    def trim_white_space(self, value):
        schema = self.json_schema['properties']['trimWhitespace']
        if value is None:
            value = schema['default']
        else:
            self._json_schema_validate(value, schema)
        self._trim_white_space = value

    @property
    def comparison_properties(self) -> Iterable[str]:
        return ['delimiter', 'trim_white_space']

    def to_json_serializable(self):
        return {
            'type': 'delimiter',
            'delimiter': self.delimiter,
            'trimWhitespace': self.trim_white_space
        }

    @classmethod
    def from_deserialized_object(cls, obj):
        cls._json_schema_validate(obj, cls.json_schema)
        return cls(
            delimiter=obj.get('delimiter'),
            trim_white_space=obj.get('trimWhitespace')
        )


class AitoCharNGramAnalyzerSchema(AitoAnalyzerSchema):
    """Aito `CharNGramAnalyzer <https://aito.ai/docs/api/#schema-char-n-gram-analyzer>`__ schema

    """

    json_schema = AitoAnalyzerSchema.char_ngram_analyzer_json_schema

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
        self._json_schema_validate(value, self.json_schema['properties']['minGram'])
        self._min_gram = value

    @property
    def max_gram(self):
        return self._max_gram

    @max_gram.setter
    def max_gram(self, value):
        self._json_schema_validate(value, self.json_schema['properties']['maxGram'])
        self._max_gram = value

    @classmethod
    def from_deserialized_object(cls, obj):
        cls._json_schema_validate(obj, cls.json_schema)
        return cls(
            min_gram=obj.get('minGram'),
            max_gram=obj.get('maxGram')
        )


class AitoTokenNgramAnalyzerSchema(AitoAnalyzerSchema):
    """Aito `TokenNGramAnalyzer <https://aito.ai/docs/api/#schema-token-n-gram-analyzer>`__ schema

    """

    json_schema = AitoAnalyzerSchema.token_ngram_analyzer_json_schema

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
            self._raise_validation_error('source analyzer must be of type AitoAnalyzerSchema')
        self._source = value

    @property
    def min_gram(self):
        return self._min_gram

    @min_gram.setter
    def min_gram(self, value):
        self._json_schema_validate(value, self.json_schema['properties']['minGram'])
        self._min_gram = value

    @property
    def max_gram(self):
        return self._max_gram

    @max_gram.setter
    def max_gram(self, value):
        self._json_schema_validate(value, self.json_schema['properties']['maxGram'])
        self._max_gram = value

    @property
    def token_separator(self) -> str:
        """the string that will be used to join the features

        :rtype: str
        """
        return self._token_separator

    @token_separator.setter
    def token_separator(self, value):
        schema = self.json_schema['properties']['tokenSeparator']
        if value is None:
            value = schema['default']
        else:
            self._json_schema_validate(value, schema)
        self._token_separator = value

    @property
    def comparison_properties(self) -> Iterable[str]:
        return ['source', 'min_gram', 'max_gram', 'token_separator']

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
        cls._json_schema_validate(obj, cls.json_schema)
        return cls(
            source=AitoAnalyzerSchema.from_deserialized_object(obj.get('source')),
            min_gram=obj.get('minGram'),
            max_gram=obj.get('maxGram'),
            token_separator=obj.get('tokenSeparator')
        )


class AitoDataTypeSchema(AitoSchema, ABC):
    """The base class for Aito DataType"""

    _supported_data_types = ["Boolean", "Decimal", "Int", "String", "Text"]
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
        'mixed': 'Text'
    }

    json_schema = {'type': 'string', 'enum': _supported_data_types}

    def __init__(self, aito_dtype: str):
        """

        :param aito_dtype: the Aito data type
        :type aito_dtype: str
        """
        if aito_dtype not in self._supported_data_types:
            self._raise_validation_error(
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
    def from_deserialized_object(cls, obj: str) -> 'AitoDataTypeSchema':
        cls._json_schema_validate(obj, cls.json_schema)
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
        sampled_values = series.values if len(series) < max_sample_size else series.sample(max_sample_size).values
        LOG.debug('inferring pandas dtype from sample values...')
        inferred_dtype = pd.api.types.infer_dtype(sampled_values)
        LOG.debug(f'inferred pandas dtype: {inferred_dtype}')
        if inferred_dtype not in cls._pandas_dtypes_name_to_aito_type:
            LOG.debug(f'failed to convert pandas dtype {inferred_dtype} to aito dtype')
            raise Exception(f'failed to infer aito data type')
        return cls.from_deserialized_object(cls._pandas_dtypes_name_to_aito_type[inferred_dtype])

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
    def __init__(self):
        super().__init__('Boolean')

    def to_python_type(self):
        return bool


class AitoIntType(AitoDataTypeSchema):
    def __init__(self):
        super().__init__('Int')

    def to_python_type(self):
        return int


class AitoDecimalType(AitoDataTypeSchema):
    def __init__(self):
        super().__init__('Decimal')

    def to_python_type(self):
        return float


class AitoStringType(AitoDataTypeSchema):
    def __init__(self):
        super().__init__('String')

    def to_python_type(self):
        return str


class AitoTextType(AitoDataTypeSchema):
    def __init__(self):
        super().__init__('Text')

    def to_python_type(self):
        return str


class AitoColumnLinkSchema(AitoSchema):
    """Link to a column of another table"""

    json_schema = {'type': 'string'}

    def __init__(self, linked_table_name: str, linked_field_name: str):
        self._linked_table_name = linked_table_name
        self._linked_field_name = linked_field_name

    @property
    def type(self):
        return 'columnLink'

    @property
    def linked_table_name(self) -> str:
        """the name of the linked table

        :rtype: str
        """
        return self._linked_table_name

    @property
    def linked_field_name(self) -> str:
        """the name of the linked field

        :rtype: str
        """
        return self._linked_field_name

    def to_json_serializable(self):
        return f"{self.linked_table_name}.{self.linked_field_name}"

    @classmethod
    def from_deserialized_object(cls, obj: str) -> "AitoColumnLinkSchema":
        cls._json_schema_validate(obj, cls.json_schema)
        splitted = obj.split('.')
        if not splitted or len(splitted) != 2:
            raise ValidationError(
                cls.__name__,
                'invalid link. The link must contain table and column in the format `<table_name>.<column_name>`',
                LOG
            )
        return cls(splitted[0], splitted[1])

    @property
    def comparison_properties(self) -> Iterable[str]:
        return ['linked_table_name', 'linked_field_name']


class AitoColumnTypeSchema(AitoSchema):
    """Aito `ColumnType <https://aito.ai/docs/api/#schema-column-type>`__ schema

    """

    json_schema = {
        'type': 'object',
        'properties': {
            'type': AitoDataTypeSchema.json_schema,
            'nullable': {'type': 'boolean', 'default': False},
            'link': AitoColumnLinkSchema.json_schema,
            'analyzer': AitoAnalyzerSchema.json_schema
        },
        'required': ['type'],
        'additionalProperties': False
    }

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
            self._raise_validation_error('data_type must be of type AitoDataTypeSchema')
        if self.analyzer and not value.is_text:
            self._raise_validation_error(f"{value} does not support analyzer")
        self._data_type = value

    @property
    def nullable(self) -> bool:
        """returns True if the column allow `null` value

        :rtype: bool
        """
        return self._nullable

    @nullable.setter
    def nullable(self, value):
        schema = self.json_schema['properties']['nullable']
        if value is None:
            value = schema['default']
        else:
            self._json_schema_validate(value, schema)
        self._nullable = value

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
                self._raise_validation_error('link must be of type AitoColumnLinkSchema')
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
                self._raise_validation_error(f"{self.data_type} does not support analyzer")
            if not isinstance(value, AitoAnalyzerSchema):
                self._raise_validation_error('analyzer must be of type AitoAnalyzerSchema')
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
    def from_deserialized_object(cls, obj: Dict):
        cls._json_schema_validate(obj, cls.json_schema)
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
    >>> table_schema
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

    >>> table_schema['name']
    {
      "nullable": false,
      "type": "String"
    }

    change the property of a column

    >>> table_schema['name'].nullable = True
    >>> table_schema['name']
    {
      "nullable": true,
      "type": "String"
    }
    """

    json_schema = {
        'type': 'object',
        'properties': {
            'type': {'enum': ['table']},
            'columns': {
                'type': 'object',
                'additionalProperties': AitoColumnTypeSchema.json_schema
            }
        },
        'required': ['type', 'columns'],
        'additionalProperties': False
    }

    def __init__(self, columns: Dict[str, AitoColumnTypeSchema]):
        """

        :param columns: a dictionary of the table's columns' name and schema
        :type columns: Dict[str, AitoColumnTypeSchema]
        """
        self._columns = columns

    @property
    def type(self):
        return 'table'

    @property
    def comparison_properties(self) -> Iterable[str]:
        return ['columns_schemas']

    def to_json_serializable(self):
        return {
            "type": "table",
            "columns": {
                col_name: col_type.to_json_serializable() for col_name, col_type in self._columns.items()
            }
        }

    @property
    def columns(self) -> List[str]:
        """list of the table's columns name

        :rtype: List[str]
        """
        return list(self._columns.keys())

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
            self._raise_validation_error('the name of the column must be of type string')
        if column_name not in self._columns:
            self._raise_validation_error(f'column `{column_name}` does not exist')
        return self._columns[column_name]

    def __setitem__(self, column_name: str, value):
        """update the column schema of the specified column name
        """
        if not isinstance(column_name, str):
            self._raise_validation_error('the name of the column must be of type string')
        if column_name not in self._columns:
            self._raise_validation_error(f'column `{column_name}` does not exist')
        if not isinstance(value, AitoColumnTypeSchema):
            self._raise_validation_error('column schema must be of type AitoColumnTypeSchema')
        self._columns[column_name] = value

    def has_column(self, column_name: str) -> bool:
        """check if the table has the specified column

        :param column_name: the name of the column
        :type column_name: str
        :return: true if the table has the specified column
        :rtype: bool
        """
        return column_name in self._columns

    def add_column(self, column_name: str, column_schema: AitoColumnTypeSchema):
        """add a column to the table schema

        :param column_name: the name of the added column
        :type column_name: str
        :param column_schema: the schema of the added column
        :type column_schema: AitoColumnTypeSchema
        """
        if self.has_column(column_name):
            self._raise_validation_error(f'column `{column_name}` already exists')
        self._columns[column_name] = column_schema

    @classmethod
    def from_deserialized_object(cls, obj):
        cls._json_schema_validate(obj, cls.json_schema)

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
            col_type_schema = AitoColumnTypeSchema._infer_from_pandas_series(df[col], max_sample_size)
            columns_schema[col] = col_type_schema
        table_schema = cls(columns=columns_schema)
        LOG.debug('inferred table schema')
        return table_schema


class AitoDatabaseSchema(AitoSchema):
    """Aito Database Schema

    Can be thought of as a dict-like container for :class:`.AitoTableSchema` objects
    """

    json_schema = {
        'type': 'object',
        'properties': {
            'schema': {
                'type': 'object',
                'additionalProperties': AitoTableSchema.json_schema
            }
        },
        'required': ['schema'],
        'additionalProperties': False
    }

    def __init__(self, tables: Dict[str, AitoTableSchema]):
        self._tables = tables

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
            self._raise_validation_error('the name of the column must be of type string')
        if table_name not in self._tables:
            self._raise_validation_error(f'table `{table_name}` does not exist')
        return self._tables[table_name]

    def __setitem__(self, table_name: str, value):
        """update the schema of the specified table
        """
        if not isinstance(table_name, str):
            self._raise_validation_error('the name of the column must be of type string')
        if table_name not in self._tables:
            self._raise_validation_error('table `{table_name}` does not exist')
        if not isinstance(value, AitoTableSchema):
            self._raise_validation_error('the table schema must be of type AitoTableSchema')
        self._tables[table_name] = value

    def has_table(self, table_name: str) -> bool:
        """check if the database has the specified table

        :param table_name: the name of the column
        :type table_name: str
        :return: true if the table has the specified column
        :rtype: bool
        """
        return table_name in self._tables

    def add_column(self, table_name: str, table_schema: AitoTableSchema):
        """add a column to the table schema

        :param table_name: the name of the added table
        :type table_name: str
        :param table_schema: the schema of the added table
        :type table_schema: AitoTableSchema
        """
        if self.has_table(table_name):
            self._raise_validation_error(f'table `{table_name}` already exists')
        self._tables[table_name] = table_schema

    @classmethod
    def from_deserialized_object(cls, obj):
        cls._json_schema_validate(obj, cls.json_schema)
        schema_data = obj.get('schema')
        tables = {
            tbl_name: AitoTableSchema.from_deserialized_object(tbl_data) for tbl_name, tbl_data in schema_data.items()
        }
        return cls(tables=tables)

    def get_linked_columns(self, table_name) -> List[str]:
        """return the columns name of the linked table of a table

        :param table_name: the name of the table
        :type table_name: str
        :return: list of linked columns
        :rtype: List[str]
        """
        table_schema = self.__getitem__(table_name)
        linked_columns = []
        for col_name, col_schema in table_schema.columns_schemas.items():
            if col_schema.has_link:
                col_link = col_schema.link
                col_linked_table_name = col_link.linked_table_name
                if col_linked_table_name not in self.tables:
                    self._raise_validation_error(f'column {col_name} link to non-exist table{col_linked_table_name}')
                linked_columns += [
                    f'{col_linked_table_name}.{linked_tbl_col}'
                    for linked_tbl_col in self.__getitem__(col_linked_table_name).columns
                ]
        return linked_columns
