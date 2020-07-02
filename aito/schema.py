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
from langdetect import detect_langs

LOG = logging.getLogger('AitoSchema')


def _check_object_type(obj_name, data, typ):
    if not isinstance(data, typ):
        raise TypeError(f"{obj_name} must be of type {typ}")


def _get_required_kwarg_of(obj_name, data, kwarg_name, assert_equal=False, assert_equal_val=None):
    kwarg_val = data.get(kwarg_name)
    if kwarg_val is None:
        raise ValueError(f"`{kwarg_name}` is required for {obj_name}")
    if assert_equal and kwarg_val != assert_equal_val:
        raise ValueError(f"`{kwarg_name}` must be {assert_equal_val} for {obj_name}")
    return kwarg_val


def _compare_optional_arg(first, second, default_value):
    if first is None and second is None:
        return True
    if first is not None and second is not None:
        return True
    if default_value == first or default_value == second:
        return True
    return False


class AitoSchema(ABC):
    """The base class for Aito schema component

    """
    _supported_analyzer_type = ['char-ngram', 'delimiter', 'language', 'token-ngram']
    _supported_data_types = ("Boolean", "Decimal", "Int", "String", "Text")
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
        al for iso_code_and_lang in _supported_language_analyzer_iso_code_to_name.items() for al in iso_code_and_lang]
    _supported_analyzer_aliases = ['standard', 'whitespace'] + _supported_language_analyzer_aliases

    def __init__(self, typ):
        """

        :param typ: the type of the schema component
        :type typ: str
        """
        self._typ = typ

    @property
    def type(self):
        """the type of the schema component
        """
        return self._typ

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
        deserialized_obj = json.loads(json_string, **kwargs)
        return cls.from_deserialized_object(deserialized_obj)

    @property
    @abstractmethod
    def comparison_properties(self) -> Iterable[str]:
        """iterable of the properties that will be used for comparison with another schema object of the same type

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
        return json.dumps(self.to_json_serializable(), indent=4, sort_keys=True)


class AitoAnalyzerSchema(AitoSchema, ABC):
    """the base class for `Aito Analyzer <https://aito.ai/docs/api/#schema-analyzer>`__
    """
    _lang_detect_code_to_aito_code = {
        'ko': 'cjk',
        'ja': 'cjk',
        'zh-cn': 'cjk',
        'zh-tw': 'cjk',
        'pt': 'pt-br'
    }

    def __init__(self, analyzer_type: str):
        """

        :param analyzer_type: the type of the analyzer
        :type analyzer_type: str
        """
        super().__init__('analyzer')
        self._analyzer_type = analyzer_type

    @property
    def analyzer_type(self) -> str:
        return self._analyzer_type

    @classmethod
    @abstractmethod
    def from_deserialized_object(cls, obj):
        if isinstance(obj, str):
            return AitoAliasAnalyzerSchema.from_deserialized_object(obj)
        else:
            analyzer_type = _get_required_kwarg_of('AnalyzerSchema', obj, 'type')
            if analyzer_type not in cls._supported_analyzer_type:
                raise ValueError(f'unsupported analyzer of type {analyzer_type}')
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
                LOG.debug(f'failed to sniff delimiter of {sample}: e')
        return None

    @classmethod
    def _infer_delimiter(cls, samples: Iterable[str]) -> Optional[str]:
        """returns the most common inferred delimiter from the samples"""
        inferred_delimiter_counter = Counter([cls._try_sniff_delimiter(smpl) for smpl in samples])
        most_common_delimiter_and_count = inferred_delimiter_counter.most_common(1)[0]
        LOG.debug(f'most common inferred delimiter and count: {most_common_delimiter_and_count}')
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

    def __init__(self, alias: str):
        """

        :param alias: the alias of the analyzer, standardize to the language name if the alias is a language ISO code
        :type alias: str
        """
        super().__init__(analyzer_type='alias')
        if not isinstance(alias, str):
            raise TypeError("alias must be of type str")
        alias = alias.lower().strip()
        if alias not in self._supported_analyzer_aliases:
            raise ValueError(f"unsupported alias {alias}")
        if alias in self._supported_language_analyzer_iso_code_to_name:
            alias = self._supported_language_analyzer_iso_code_to_name[alias]
        self._alias = alias

    @property
    def alias(self) -> str:
        """

        :return: the alias of the analyzer
        :rtype: str
        """
        return self._alias

    @property
    def comparison_properties(self) -> Iterable[str]:
        return ['alias']

    @classmethod
    def from_deserialized_object(cls, obj: str):
        _check_object_type('AliasAnalyzerSchema object', obj, str)
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
        :param custom_key_words: words that will not be featurizerd
        :type custom_key_words: List[str], defaults to []
        """
        super().__init__('language')
        self.language = language
        self._use_default_stop_words = use_default_stop_words if use_default_stop_words is not None else False
        self._custom_stop_words = custom_stop_words if custom_stop_words is not None else []
        self._custom_key_words = custom_key_words if custom_key_words is not None else []

    @property
    def language(self) -> str:
        """the language of the analyzer"""
        return self._language

    @language.setter
    def language(self, value):
        if not isinstance(value, str):
            raise TypeError("language must be of type str")
        language = value.lower().strip()
        if language not in self._supported_language_analyzer_aliases:
            raise ValueError(f'unsupported language {language}')
        if language in self._supported_language_analyzer_iso_code_to_name:
            language = self._supported_language_analyzer_iso_code_to_name[language]
        self._language = language

    @property
    def use_default_stop_words(self) -> bool:
        """filter the language default stop words"""
        return self._use_default_stop_words

    @use_default_stop_words.setter
    def use_default_stop_words(self, value):
        if value is None:
            value = False
        elif not isinstance(value, bool):
            raise TypeError('use_default_stop_words must be of type bool or None')
        self._use_default_stop_words = value

    @property
    def custom_stop_words(self) -> List:
        """list of words that will be filtered"""
        return self._custom_stop_words

    @custom_stop_words.setter
    def custom_stop_words(self, value):
        if value is None:
            value = []
        elif not (isinstance(value, list) and all(isinstance(item, str) for item in value)):
            raise TypeError('custom_stop_words must be of type list of string or None')
        self._use_default_stop_words = value

    @property
    def custom_key_words(self) -> List:
        """list of words that will not be featurizerd"""
        return self._custom_key_words

    @custom_key_words.setter
    def custom_key_words(self, value):
        if value is None:
            value = []
        elif not (isinstance(value, list) and all(isinstance(item, str) for item in value)):
            raise TypeError('custom_key_words must be of type list of string or None')
        self._custom_key_words = value

    @property
    def comparison_properties(self) -> Iterable[str]:
        return ['language', 'use_default_stop_words', 'custom_stop_words', 'custom_key_words']

    @classmethod
    def from_deserialized_object(cls, obj: Dict):
        _check_object_type('LanguageAnalyzerSchema object', obj, dict)
        _get_required_kwarg_of('LanguageAnalyzerSchema', obj, 'type', True, 'language')
        return cls(
            language=_get_required_kwarg_of('LanguageAnalyzerSchema', obj, 'language'),
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
        super().__init__('delimiter')
        self.delimiter = delimiter
        self.trim_white_space = trim_white_space

    @property
    def delimiter(self):
        return self._delimiter

    @delimiter.setter
    def delimiter(self, value):
        if not isinstance(value, str):
            raise TypeError('delimiter must be of type str')
        self._delimiter = value

    @property
    def trim_white_space(self) -> bool:
        """trim leading and trailing whitespaces of the features"""
        return self._trim_white_space

    @trim_white_space.setter
    def trim_white_space(self, value):
        if value is None:
            value = True
        elif not isinstance(value, bool):
            raise TypeError('trim_white_space must be of type bool or None')
        self._trim_white_space = value

    @property
    def comparison_properties(self) -> Iterable[str]:
        return ['delimiter', 'trim_white_space']

    def to_json_serializable(self):
        return {
            'type': 'delimiter',
            'delimiter': self.delimiter,
            'trimWhiteSpace': self.trim_white_space
        }

    @classmethod
    def from_deserialized_object(cls, obj):
        _check_object_type('DelimiterAnalyzerSchema object', obj, dict)
        _get_required_kwarg_of('DelimiterAnalyzerSchema', obj, 'type', True, 'delimiter')
        return cls(
            delimiter=_get_required_kwarg_of('DelimiterAnalyzerSchema', obj, 'delimiter'),
            trim_white_space=obj.get('trimWhiteSpace')
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
        super().__init__('char-ngram')
        self.min_gram = min_gram
        self.max_gram = max_gram

    @property
    def comparison_properties(self) -> Iterable[str]:
        return ['min_gram', 'max_gram']

    def to_json_serializable(self):
        return {
            'type': 'char-ngram',
            'minGram': self.min_gram,
            'maxGram': self.max_gram
        }

    @classmethod
    def from_deserialized_object(cls, obj):
        _check_object_type('CharNGramAnalyzerSchema object', obj, dict)
        _get_required_kwarg_of('CharNGramAnalyzerSchema', obj, 'type', True, 'char-ngram')
        return cls(
            min_gram=_get_required_kwarg_of('CharNGramAnalyzerSchema', obj, 'minGram'),
            max_gram=_get_required_kwarg_of('CharNGramAnalyzerSchema', obj, 'maxGram')
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
        super().__init__('token-ngram')
        self.source = source
        self.min_gram = min_gram
        self.max_gram = max_gram
        self.token_separator = token_separator

    @property
    def token_separator(self) -> str:
        """the string to join the features"""
        return self._token_separator

    @token_separator.setter
    def token_separator(self, value):
        if value is None:
            value = ' '
        elif not isinstance(value, str):
            raise TypeError('token_separator must be of type str')
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
        _check_object_type('TokenNGramAnalyzerSchema object', obj, dict)
        _get_required_kwarg_of('TokenNGramAnalyzerSchema', obj, 'type', True, 'token-ngram')
        return cls(
            source=AitoAnalyzerSchema.from_deserialized_object(
                _get_required_kwarg_of('TokenNGramAnalyzerSchema', obj, 'source')
            ),
            min_gram=_get_required_kwarg_of('TokenNGramAnalyzerSchema', obj, 'minGram'),
            max_gram=_get_required_kwarg_of('TokenNGramAnalyzerSchema', obj, 'maxGram'),
            token_separator=obj.get('tokenSeparator')
        )


class AitoDataTypeSchema(AitoSchema, ABC):
    """The base class for Aito DataType"""

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

    def __init__(self, aito_dtype: str):
        """

        :param aito_dtype: the Aito data type
        :type aito_dtype: str
        """
        super().__init__('dtype')
        if aito_dtype not in self._supported_data_types:
            raise ValueError(
                f"unrecognized data type `{aito_dtype}`. Data type must be one of {'|'.join(self._supported_data_types)}"
            )
        self._aito_dtype = aito_dtype

    @property
    def aito_dtype(self) -> str:
        """the data type

        :rtype: str
        """
        return self._aito_dtype

    @property
    def is_string(self) -> bool:
        """true if the data type is String
        """
        return self._aito_dtype == 'String'

    @property
    def is_text(self) -> bool:
        """true if the data type is Text
        """
        return self._aito_dtype == 'Text'

    @property
    def is_bool(self) -> bool:
        """true if the data type is Boolean
        """
        return self._aito_dtype == 'Boolean'

    @property
    def is_int(self) -> bool:
        """true if the data type is Int
        """
        return self._aito_dtype == 'Int'

    @property
    def is_decimal(self) -> bool:
        """true if the data type is Decimal
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
        _check_object_type('DataTypeSchema object', obj, str)
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
        raise ValueError(
            f"unrecognized data type `{obj}`. Data type must be one of {'|'.join(cls._supported_data_types)}"
        )

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


class AitoColumnLink(AitoSchema):
    def __init__(self, linked_table_name: str, linked_field_name: str):
        super().__init__('columnLink')
        self._linked_table_name = linked_table_name
        self._linked_field_name = linked_field_name

    @property
    def linked_table_name(self) -> str:
        """the name of the linked table"""
        return self._linked_table_name

    @property
    def linked_field_name(self) -> str:
        """the name of the linked field"""
        return self._linked_field_name

    def to_json_serializable(self):
        return f"{self.linked_table_name}.{self.linked_field_name}"

    @classmethod
    def from_deserialized_object(cls, obj: str) -> "AitoColumnLink":
        splitted = obj.split('.')
        if not splitted or len(splitted) != 2:
            raise ValueError(
                f'invalid link. The link must contain table and column in the format `<table_name>.<column_name>`'
            )
        return cls(splitted[0], splitted[1])

    @property
    def comparison_properties(self) -> Iterable[str]:
        return ['linked_table_name', 'linked_field_name']


class AitoColumnTypeSchema(AitoSchema):
    """Aito `ColumnType <https://aito.ai/docs/api/#schema-column-type>`__ schema

    """
    def __init__(
            self,
            data_type: AitoDataTypeSchema,
            nullable: bool = None,
            link: AitoColumnLink = None,
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
        super().__init__('column')
        self._data_type = data_type
        self._analyzer = analyzer
        self.data_type = data_type
        self.nullable = nullable
        self.link = link
        self.analyzer = analyzer

    @property
    def data_type(self):
        return self._data_type

    @data_type.setter
    def data_type(self, value):
        if not isinstance(value, AitoDataTypeSchema):
            raise TypeError('data_type must be of type AitoDataTypeSchema')
        if self.analyzer and not value.is_text:
            raise ValueError(f"{value} does not support analyzer")
        self._data_type = value

    @property
    def analyzer(self):
        return self._analyzer

    @analyzer.setter
    def analyzer(self, value):
        if value:
            if not isinstance(value, AitoAnalyzerSchema):
                raise TypeError('analyzer must be of type AitoAnalyzerSchema')
            if  not self.data_type.is_text:
                raise ValueError(f"{self.data_type} does not support analyzer")
        self._analyzer = value

    @property
    def nullable(self) -> bool:
        """True if the column allow `null` value"""
        return self._nullable

    @nullable.setter
    def nullable(self, value):
        if value is None:
            value = False
        elif not isinstance(value, bool):
            raise TypeError('nullable must be of type bool or None')
        self._nullable = value

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
        _check_object_type('ColumnTypeSchema object', obj, dict)
        data_type = AitoDataTypeSchema.from_deserialized_object(_get_required_kwarg_of('ColumnSchema', obj, 'type'))
        analyzer_data = obj.get('analyzer')
        analyzer = AitoAnalyzerSchema.from_deserialized_object(analyzer_data) if analyzer_data is not None else None
        link_data = obj.get('link')
        link = AitoColumnLink.from_deserialized_object(link_data) if link_data is not None else None
        return cls(data_type=data_type, nullable=obj.get('nullable'), link=link, analyzer=analyzer)


class AitoTableSchema(AitoSchema):
    """Aito Table schema contains the columns and their schema

    Can be thought of as a dict-like container for AitoColumnTypeSchema objects

    Examples
    --------

    Infer AitoTableSchema from a Pandas DataFrame

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
    >>> table_schema['name'].nullable = True
    >>> table_schema['name']
    {
        "nullable": true,
        "type": "String"
    }

    change the property of a column

    """

    def __init__(self, columns: Dict[str, AitoColumnTypeSchema]):
        """

        :param columns: a dictionary of the table's columns' name and scheam
        :type columns: Dict[str, AitoColumnTypeSchema]
        """
        super().__init__('table')
        self._columns = columns

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
        """

        :return: list of the table's columns name
        :rtype: List[str]
        """
        return list(self._columns.keys())

    @property
    def columns_schemas(self) -> Dict[str, AitoColumnTypeSchema]:
        """

        :return: the table columns and its schemas
        :rtype: List[str]
        """
        return self._columns

    @property
    def links(self) -> Optional[Dict[str, AitoColumnLink]]:
        """

        :return: dictionary of the table column and its link
        :rtype: Dict[str, AitoColumnLink]
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
        if column_name not in self._columns:
            raise KeyError(f'column `{column_name}` does not exist')
        if not isinstance(value, AitoColumnTypeSchema):
            raise TypeError('column shchema must be of type AitoColumnTypeSchema')
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
            raise ValueError(f'column `{column_name}` already exists')
        self._columns[column_name] = column_schema

    @classmethod
    def from_deserialized_object(cls, obj):
        _check_object_type('TableSchema object', obj, dict)
        _get_required_kwarg_of(
            '`Table`', obj, 'type', assert_equal=True, assert_equal_val='table'
        )
        columns_data = _get_required_kwarg_of('`TableSchema`', obj, 'columns')
        _check_object_type('TableSchema columns object', columns_data, dict)
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
            col_df_samples = df[col] if len(df[col]) < max_sample_size else df[col].sample(max_sample_size)
            col_nullable = True if col_df_samples.isna().sum() > 0 else False
            col_analyzer = None
            try:
                col_aito_type = AitoDataTypeSchema.infer_from_samples(col_df_samples, max_sample_size)
            except Exception as e:
                LOG.debug(f'failed to infer aito col type from col {col} samples: {col_df_samples[:10].values}: {e}')
                raise Exception(f'failed to infer aito column type of column `{col}`')
            # avoid numpy type
            if col_aito_type.is_text:
                col_df_samples.dropna(inplace=True)
                col_analyzer = AitoAnalyzerSchema.infer_from_samples(col_df_samples.__iter__())
                if not col_analyzer:
                    col_aito_type = AitoStringType()
            col_type_schema = AitoColumnTypeSchema(
                data_type=col_aito_type, nullable=col_nullable, analyzer=col_analyzer
            )
            columns_schema[col] = col_type_schema
        table_schema = cls(columns=columns_schema)
        LOG.debug('inferred table schema')
        return table_schema


class AitoDatabaseSchema(AitoSchema):
    """Aito Database Schema

    Can be thought of as a dict-like container for AitoTableSchema objects
    """
    def __init__(self, tables: Dict[str, AitoTableSchema]):
        super().__init__('database')
        self._tables = tables

    @property
    def tables(self) -> List[str]:
        """

        :return: list of the database's table name
        :rtype:
        """
        return list(self._tables.keys())

    @property
    def tables_schemas(self) -> Dict[str, AitoTableSchema]:
        """

        :return: the database tables and its schemas
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
            raise TypeError('the name of the column must be of type string')
        if table_name not in self._tables:
            raise KeyError(f'table `{table_name}` does not exist')
        if not isinstance(value, AitoTableSchema):
            raise TypeError('the table schema must be of type AitoTableSchema')
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
            raise ValueError(f'table `{table_name}` already exists')
        self._tables[table_name] = table_schema

    @classmethod
    def from_deserialized_object(cls, obj):
        _check_object_type('DatabaseSchema object', obj, dict)
        schema_data = _get_required_kwarg_of('`DatabaseSchema`', obj, 'schema')
        _check_object_type('DatabaseSchema schema objects', schema_data, dict)
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
                    raise ValueError(f'column {col_name} link to non-exist table{col_linked_table_name}')
                linked_columns += [
                    f'{col_linked_table_name}.{linked_tbl_col}'
                    for linked_tbl_col in self.__getitem__(col_linked_table_name).columns
                ]
        return linked_columns
