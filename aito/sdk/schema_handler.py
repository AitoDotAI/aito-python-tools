import logging
from typing import List, Dict

import pandas as pd
from langdetect import detect


LOG = logging.getLogger("SchemaHandler")


class SchemaHandler:
    def __init__(self):
        self.pandas_dtypes_name_to_aito_type = {
            'string': 'Text',
            'bytes': 'Text',
            'floating': 'Decimal',
            'integer': 'Int',
            'mixed-integer': 'Decimal',
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
        self.aito_types_to_python_types = {
            'Boolean': bool,
            'Decimal': float,
            'Int': int,
            'String': str,
            'Text': str
        }

        self.lang_detect_code_to_aito_code = {
            'ko': 'cjk',
            'ja': 'cjk',
            'zh-cn': 'cjk',
            'zh-tw': 'cjk',
            'pt': 'pt-br'
        }

        self.supported_alias_analyzer = ['standard', 'whitespace', 'ar', 'hy', 'eu', 'pt-br', 'bg', 'ca', 'cjk', 'cs',
                                         'da', 'nl', 'en', 'fi', 'fr', 'gl', 'de', 'el', 'hi', 'hu', 'id', 'ga', 'it',
                                         'lv', 'no', 'fa', 'pt', 'ro', 'ru', 'es', 'sv', 'th', 'tr']
        self.sample_size = 100000

    def infer_aito_types_from_pandas_series(self, series: pd.Series, sample_size = 100000) -> str:
        """Infer aito column type from a Pandas Series

        :param series: input Pandas Series
        :type series: pd.Series
        :param sample_size: sample size for type inference, defaults to 100000
        :type sample_size: int, optional
        :raises Exception: fail to infer type
        :return: inferred Aito type
        :rtype: str
        """
        sampled_values = series.values if len(series) < sample_size else series.sample(sample_size).values
        LOG.debug('inferring dtype from sample values...')
        inferred_dtype = pd.api.types.infer_dtype(sampled_values)
        LOG.debug(f'inferred dtype: {inferred_dtype}')
        if inferred_dtype not in self.pandas_dtypes_name_to_aito_type:
            raise Exception(f'failed to infer aito type from dtype {inferred_dtype}')
        return self.pandas_dtypes_name_to_aito_type[inferred_dtype]

    def infer_table_schema_from_pandas_data_frame(self, df: pd.DataFrame) -> Dict:
        """Infer a table schema from a Pandas DataFrame

        :param df: input Pandas DataFrame
        :type df: pd.DataFrame
        :raises Exception: an error occurred during column type inference
        :return: inferred table schema
        :rtype: Dict
        """
        LOG.debug('inferring table schema...')
        rows_count = df.shape[0]

        columns_schema = {}
        for col in df.columns.values:
            col_df = df[col]
            try:
                col_aito_type = self.infer_aito_types_from_pandas_series(df[col], self.sample_size)
            except Exception as e:
                raise Exception(f'failed to infer aito type of column `{col}`: {e}')
            col_na_count = col_df.isna().sum()
            col_schema = {
                'nullable': True if col_na_count > 0 else False,
                'type': col_aito_type
            }
            if col_schema['type'] == 'Text':
                col_unique_val_count = col_df.nunique()
                col_non_na_count = rows_count - col_na_count
                if (col_non_na_count / col_unique_val_count) > 2:
                    col_schema['type'] = 'String'
                else:
                    col_text = col_df.str.cat(sep=' ')
                    try:
                        detected_lang = detect(col_text)
                        if detected_lang in self.lang_detect_code_to_aito_code:
                            detected_lang = self.lang_detect_code_to_aito_code[detected_lang]
                    except Exception as e:
                        LOG.debug(f'failed to detect language: {e}')
                        detected_lang = None
                    if detected_lang and detected_lang in self.supported_alias_analyzer:
                        col_schema['analyzer'] = detected_lang

            columns_schema[col] = col_schema

        table_schema = {'type': 'table', 'columns': columns_schema}
        LOG.info('inferred table schema')
        return table_schema

    def validate_table_schema(self, table_schema: Dict) -> Dict:
        """Validate Aito Schema

        :param table_schema: input table schema
        :type table_schema: Dict
        :raises ValueError: table schema is invalid
        :return: table schema if valid
        :rtype: Dict
        """
        def validate_required_arg(object_name: str, object_dict: Dict, arg_name: str):
            if arg_name not in object_dict:
                raise ValueError(f"{object_name} missing '{arg_name}'")

        def validate_arg_type(object_name: str, object_dict: Dict, arg_name: str, arg_type: type,
                              accepted_values: List = None):
            arg_value = object_dict[arg_name]
            if not isinstance(arg_value, arg_type):
                raise ValueError(f"Invalid {object_name} {arg_name} type")
            if accepted_values:
                if arg_value not in accepted_values:
                    accepted_values_str = accepted_values[0] if len(accepted_values) == 1 \
                        else f"one of {accepted_values}"
                    raise ValueError(f"{object_name} '{arg_name}' must be {accepted_values_str}")

        def validate_arg(object_name: str, object_dict: Dict, arg_name: str, required: bool = False,
                         arg_type: type = None, accepted_values: List = None):
            if required:
                validate_required_arg(object_name, object_dict, arg_name)
            if arg_name in object_dict and arg_type:
                validate_arg_type(object_name, object_dict, arg_name, arg_type, accepted_values)

        LOG.debug('validating table schema..."')
        validate_arg('table', table_schema, 'type', True, str, ['table'])
        validate_arg('table', table_schema, 'columns', True, dict)
        for col in table_schema['columns']:
            col_schema = table_schema['columns'][col]
            validate_arg(col, col_schema, 'type', True, str, list(self.aito_types_to_python_types.keys()))
            validate_arg(col, col_schema, 'nullable', False, bool)
        LOG.info('validated table schem')
        return table_schema
